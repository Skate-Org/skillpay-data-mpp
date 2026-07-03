/**
 * SidecarSource — reads the Databento sidecar's NDJSON TCP stream and turns it
 * into AlphaSnapshots. This is the only data source in this repo.
 *
 * MULTI-PRODUCT: the sidecar now streams several CME products (CL/GOLD/SILVER)
 * in one Databento Live session. Every event carries a `prod` tag; we keep an
 * independent ProductState per product and compute the same Black-76 signal set
 * for each, then emit one AlphaSnapshot per catalog symbol.
 *
 * Sidecar events consumed (all carry `prod`, e.g. "CL"|"GOLD"|"SILVER"):
 *   {"t":"bbo","prod":"GOLD","sym":"GCZ6","bid","ask","bid_sz","ask_sz"}      ← front future top-of-book
 *   {"t":"lo_bbo","prod":"GOLD","sym":"OGZ6 C300000","bid","ask","strike","is_call","expiry_iso","tte_years"}
 *   {"t":"trade","prod":"GOLD","sym":"GCZ6","px","sz","side":"B"|"A"|"N"}      ← futures trades
 *   {"t":"hb",...}
 *
 * Derived signals published per product (Black-76, r≈0):
 *   cme_fwd_mid, atm_iv, rr25, skew_signal, rr25_d4h, iv_term_slope,
 *   signed_flow_10, imb_5, funding (HL funding for the mapped coin).
 * Still null (need open-interest pipeline): gex_put_wall, gex_call_wall, spot_vs_walls.
 */
import net from 'net';
import { STREAM_SKILLS } from './config.js';
import type { AlphaSnapshot, AlphaSource, FieldValue, Health } from './types.js';
import { impliedVol, delta as bsDelta, interp, interpStrict } from './bs.js';

const SIDECAR_HOST = process.env.SIDECAR_HOST ?? '127.0.0.1';
const SIDECAR_PORT = Number(process.env.SIDECAR_PORT ?? 5051);
const EMIT_MS = Number(process.env.SIDECAR_EMIT_MS ?? 1000);
const STALE_MS = 5000;
const SKEW_SCALE = Number(process.env.SKEW_SCALE ?? 0.20); // vol pts of RR → full-scale skew
const MAX_REL_SPREAD = Number(process.env.MAX_REL_SPREAD ?? 0.5); // skip options wider than this (illiquid → garbage IV)
const FLOW_WINDOW_MS = Number(process.env.FLOW_WINDOW_MS ?? 10_000);
const RR_HIST_MS = Number(process.env.RR_HIST_MS ?? 1_800_000); // 30m trailing for rr velocity
const FUNDING_POLL_MS = Number(process.env.FUNDING_POLL_MS ?? 30_000);
const HL_INFO = process.env.HL_INFO_URL ?? 'https://api.hyperliquid.xyz/info';

// Catalog symbol → sidecar product tag. Only symbols listed here get live data;
// any other catalog symbol (e.g. BRENT, not yet fed) emits null/stale.
const SYM_TO_PROD: Record<string, string> = { CL: 'CL', GOLD: 'GOLD', SILVER: 'SILVER' };

// HL xyz-dex universe name per catalog symbol (for the funding poll).
const HL_UNIVERSE_NAME: Record<string, string> = {
  CL: 'xyz:CL',
  BRENT: 'xyz:BRENTOIL',
  GOLD: 'xyz:GOLD',
  SILVER: 'xyz:SILVER',
};

// Per-product forward sanity band (reject settle/parity forwards outside it).
const FWD_BAND: Record<string, [number, number]> = {
  CL: [40, 200],
  GOLD: [800, 6000],
  SILVER: [5, 100],
};

function fv(v: number | null | undefined, ts: number): FieldValue {
  return { v: v == null || Number.isNaN(v) ? null : v, ts };
}

interface OptQuote {
  bid: number;
  ask: number;
  strike: number;
  is_call: boolean;
  tte: number;
  expiry: string;
  ts: number;
}

/** Independent live state for one CME product (futures + option chain + tape). */
class ProductState {
  futBid = 0;
  futAsk = 0;
  futBidSz = 0;
  futAskSz = 0;
  futAt = 0;
  opts = new Map<string, OptQuote>(); // raw_symbol -> latest quote
  trades: Array<{ t: number; signed: number }> = [];
  smoothSkew = 0;
  skewInit = false;
  rrHist: Array<{ t: number; rr: number }> = [];
}

export class SidecarSource implements AlphaSource {
  private readonly handlers = new Set<(s: AlphaSnapshot) => void>();
  private readonly latestBySym = new Map<string, AlphaSnapshot>();
  private readonly states = new Map<string, ProductState>(); // prod -> state
  private seq = 0;
  private sock: net.Socket | null = null;
  private buf = '';
  private timer: NodeJS.Timeout | null = null;
  private fundingTimer: NodeJS.Timeout | null = null;
  private fundingByCoin = new Map<string, number>();

  private state(prod: string): ProductState {
    let s = this.states.get(prod);
    if (!s) {
      s = new ProductState();
      this.states.set(prod, s);
    }
    return s;
  }

  symbols(): string[] {
    return STREAM_SKILLS.map((s) => s.symbol);
  }
  latest(sym: string): AlphaSnapshot | null {
    return this.latestBySym.get(sym) ?? null;
  }
  subscribe(handler: (s: AlphaSnapshot) => void): () => void {
    this.handlers.add(handler);
    return () => this.handlers.delete(handler);
  }

  start(): void {
    this.connect();
    if (!this.timer) this.timer = setInterval(() => this.emit(), EMIT_MS);
    void this.pollFunding();
    if (!this.fundingTimer) this.fundingTimer = setInterval(() => void this.pollFunding(), FUNDING_POLL_MS);
    console.log(`[sidecar-source] reading ${SIDECAR_HOST}:${SIDECAR_PORT}, emitting every ${EMIT_MS}ms`);
  }
  stop(): void {
    if (this.timer) clearInterval(this.timer);
    if (this.fundingTimer) clearInterval(this.fundingTimer);
    this.timer = this.fundingTimer = null;
    this.sock?.destroy();
    this.sock = null;
  }

  private connect(): void {
    const s = net.connect(SIDECAR_PORT, SIDECAR_HOST);
    this.sock = s;
    s.on('connect', () => console.log('[sidecar-source] connected'));
    s.on('data', (d) => this.onData(d.toString()));
    s.on('error', (e) => console.log('[sidecar-source] err:', (e as Error).message));
    s.on('close', () => {
      console.log('[sidecar-source] closed; reconnecting in 1.5s');
      this.sock = null;
      setTimeout(() => this.connect(), 1500);
    });
  }

  private onData(chunk: string): void {
    this.buf += chunk;
    let i: number;
    while ((i = this.buf.indexOf('\n')) >= 0) {
      const line = this.buf.slice(0, i);
      this.buf = this.buf.slice(i + 1);
      if (!line) continue;
      let e: any;
      try {
        e = JSON.parse(line);
      } catch {
        continue;
      }
      // Route by product tag; default to CL for pre-multi-product sidecars.
      const prod: string = e.prod ?? 'CL';
      if (e.t === 'hb') continue;
      const st = this.state(prod);
      switch (e.t) {
        case 'bbo':
          if (e.bid > 0 && e.ask > 0) {
            st.futBid = e.bid;
            st.futAsk = e.ask;
            st.futBidSz = Number(e.bid_sz ?? 0);
            st.futAskSz = Number(e.ask_sz ?? 0);
            st.futAt = Date.now();
          }
          break;
        case 'lo_bbo':
          if (e.bid > 0 && e.ask > 0)
            st.opts.set(e.sym, {
              bid: e.bid,
              ask: e.ask,
              strike: e.strike,
              is_call: e.is_call,
              tte: e.tte_years ?? 0,
              expiry: e.expiry_iso ?? '',
              ts: Date.now(),
            });
          break;
        case 'trade': {
          const signed = e.side === 'B' ? e.sz : e.side === 'A' ? -e.sz : 0;
          if (signed) st.trades.push({ t: Date.now(), signed });
          break;
        }
      }
    }
  }

  /** Poll HL funding for the streamed coins (best-effort; never throws into emit). */
  private async pollFunding(): Promise<void> {
    try {
      const r = await fetch(HL_INFO, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ type: 'metaAndAssetCtxs', dex: 'xyz' }),
      });
      const j: any = await r.json();
      const universe: any[] = j?.[0]?.universe ?? [];
      const ctxs: any[] = j?.[1] ?? [];
      for (let i = 0; i < universe.length; i++) {
        const name = universe[i]?.name;
        const f = Number(ctxs[i]?.funding);
        if (name && Number.isFinite(f)) this.fundingByCoin.set(name, f);
      }
    } catch {
      /* keep last known */
    }
  }

  /** Fresh, reasonably-liquid option quotes (two-sided, tight enough to invert). */
  private freshOpts(st: ProductState, now: number): OptQuote[] {
    const out: OptQuote[] = [];
    for (const q of st.opts.values()) {
      if (!(now - q.ts < STALE_MS && q.bid > 0 && q.ask > 0 && q.tte > 0)) continue;
      const rel = (q.ask - q.bid) / ((q.ask + q.bid) / 2);
      if (rel > MAX_REL_SPREAD) continue; // illiquid wing → unreliable IV, drop it
      out.push(q);
    }
    return out;
  }

  /** Front/next expiry IV analytics from the option chain. */
  private optionAnalytics(
    st: ProductState,
    prod: string,
    F: number,
    now: number
  ): { atm_iv: number | null; rr25: number | null; iv_term_slope: number | null } {
    const fresh = this.freshOpts(st, now);
    if (fresh.length < 6 || F <= 0) return { atm_iv: null, rr25: null, iv_term_slope: null };
    const [lo, hi] = FWD_BAND[prod] ?? [1, 1e6];

    const byExp = new Map<string, OptQuote[]>();
    for (const q of fresh) {
      const k = q.expiry || q.tte.toFixed(5);
      const arr = byExp.get(k) ?? [];
      arr.push(q);
      byExp.set(k, arr);
    }
    const exps = [...byExp.values()].sort((a, b) => a[0]!.tte - b[0]!.tte);
    if (!exps.length) return { atm_iv: null, rr25: null, iv_term_slope: null };

    // Forward from put-call parity (front expiry): F_impl = K + (callMid − putMid),
    // median over strikes near the futures mid where both sides quote. Using the
    // *implied* forward (not the raw futures mid) removes the systematic call/put
    // IV tilt that otherwise fabricates a huge risk reversal. The near-K window
    // scales with the product's strike spacing (≈ a few strikes wide).
    const near = Math.max(6, F * 0.01);
    const parityFwd = (qs: OptQuote[]): number => {
      const byK = new Map<number, { c?: number; p?: number }>();
      for (const q of qs) {
        const e = byK.get(q.strike) ?? {};
        if (q.is_call) e.c = (q.bid + q.ask) / 2;
        else e.p = (q.bid + q.ask) / 2;
        byK.set(q.strike, e);
      }
      const ests: number[] = [];
      for (const [K, e] of byK)
        if (e.c != null && e.p != null && Math.abs(K - F) < near) ests.push(K + e.c - e.p);
      if (!ests.length) return F;
      ests.sort((a, b) => a - b);
      const med = ests[Math.floor(ests.length / 2)]!;
      return med > lo && med < hi ? med : F;
    };
    F = parityFwd(exps[0]!);

    // ATM IV: OTM smile (puts K<F, calls K≥F), interpolate IV at K=F.
    const atmOf = (qs: OptQuote[]): number | null => {
      const pts: Array<{ x: number; y: number }> = [];
      for (const q of qs) {
        const otm = q.is_call ? q.strike >= F : q.strike < F;
        if (!otm) continue;
        const iv = impliedVol((q.bid + q.ask) / 2, F, q.strike, q.tte, q.is_call);
        if (iv != null && iv > 0.02 && iv < 3) pts.push({ x: q.strike, y: iv });
      }
      return interp(pts, F);
    };

    const frontQs = exps[0]!;
    const atm_iv = atmOf(frontQs);
    const atm_next = exps.length > 1 ? atmOf(exps[1]!) : null;
    const iv_term_slope = atm_iv != null && atm_next != null ? atm_next - atm_iv : null;

    // 25Δ risk reversal on the front expiry (delta computed from each option's IV).
    const callPts: Array<{ x: number; y: number }> = [];
    const putPts: Array<{ x: number; y: number }> = [];
    for (const q of frontQs) {
      const iv = impliedVol((q.bid + q.ask) / 2, F, q.strike, q.tte, q.is_call);
      if (iv == null || iv < 0.02 || iv > 3) continue;
      const d = bsDelta(F, q.strike, q.tte, iv, q.is_call);
      if (q.is_call && q.strike >= F) callPts.push({ x: d, y: iv }); // call delta ∈ (0,0.5)
      if (!q.is_call && q.strike < F) putPts.push({ x: -d, y: iv }); // |put delta| ∈ (0,0.5)
    }
    // Strict bracketing: only accept a 25Δ IV if real quotes straddle delta 0.25
    // (no extrapolating an illiquid wing). Otherwise rr25 is null (honest).
    const ivCall25 = interpStrict(callPts, 0.25);
    const ivPut25 = interpStrict(putPts, 0.25);
    const rr25 = ivCall25 != null && ivPut25 != null ? ivPut25 - ivCall25 : null;

    return { atm_iv, rr25, iv_term_slope };
  }

  private signedFlow(st: ProductState, now: number): number {
    const cut = now - FLOW_WINDOW_MS;
    while (st.trades.length && st.trades[0]!.t < cut) st.trades.shift();
    let s = 0;
    for (const t of st.trades) s += t.signed;
    return s;
  }

  /** Compute the full field set for one product from its state (or all-null). */
  private computeFields(prod: string | undefined, ts: number) {
    if (!prod || !this.states.has(prod)) {
      return { health: 'stale_chain' as Health, mid: null as number | null,
        atm_iv: null, rr25: null, iv_term_slope: null, skew: null as number | null,
        rr_vel: null as number | null, flow: null as number | null, imb5: null as number | null };
    }
    const st = this.state(prod);
    const futFresh = st.futBid > 0 && st.futAsk > 0 && ts - st.futAt < STALE_MS;
    const mid = futFresh ? (st.futBid + st.futAsk) / 2 : null;
    const health: Health = futFresh ? 'ok' : 'stale_chain';

    const { atm_iv, rr25, iv_term_slope } =
      mid != null ? this.optionAnalytics(st, prod, mid, ts) : { atm_iv: null, rr25: null, iv_term_slope: null };

    let skew: number | null = null;
    if (rr25 != null) {
      const raw = Math.max(-1, Math.min(1, -rr25 / SKEW_SCALE)); // puts rich → bearish (<0)
      const a = 0.15;
      st.smoothSkew = st.skewInit ? st.smoothSkew * (1 - a) + raw * a : raw;
      st.skewInit = true;
      skew = st.smoothSkew;
    }

    let rr_vel: number | null = null;
    if (rr25 != null) {
      st.rrHist.push({ t: ts, rr: rr25 });
      const cut = ts - RR_HIST_MS;
      while (st.rrHist.length && st.rrHist[0]!.t < cut) st.rrHist.shift();
      if (st.rrHist.length >= 2) rr_vel = rr25 - st.rrHist[0]!.rr;
    }

    const flow = futFresh ? this.signedFlow(st, ts) : null;
    // Top-of-book imbalance from the futures bbo sizes (mbp-1 entitled data):
    // (bidSz − askSz)/(bidSz + askSz) ∈ [-1,1]. >0 bid-heavy.
    const szTot = st.futBidSz + st.futAskSz;
    const imb5 = futFresh && szTot > 0 ? (st.futBidSz - st.futAskSz) / szTot : null;

    return { health, mid, atm_iv, rr25, iv_term_slope, skew, rr_vel, flow, imb5 };
  }

  private emit(): void {
    const ts = Date.now();
    for (const skill of STREAM_SKILLS) {
      const sym = skill.symbol;
      const prod = SYM_TO_PROD[sym];
      const f = this.computeFields(prod, ts);
      const funding = this.fundingByCoin.get(HL_UNIVERSE_NAME[sym] ?? '') ?? null;
      const snap: AlphaSnapshot = {
        sym,
        seq: ++this.seq,
        ts,
        health: f.health,
        fields: {
          rr25: fv(f.rr25, ts),
          rr25_d4h: fv(f.rr_vel, ts),
          skew_signal: fv(f.skew, ts),
          atm_iv: fv(f.atm_iv, ts),
          gex_put_wall: fv(null, ts),
          gex_call_wall: fv(null, ts),
          spot_vs_walls: fv(null, ts),
          iv_term_slope: fv(f.iv_term_slope, ts),
          cme_fwd_mid: fv(f.mid, ts),
          cme_hl_basis_bps: fv(0, ts),
          signed_flow_10: fv(f.flow, ts),
          imb_5: fv(f.imb5, ts),
          funding: fv(funding, ts),
        },
      };
      this.latestBySym.set(sym, snap);
      for (const h of this.handlers) {
        try {
          h(snap);
        } catch (err) {
          console.error('[sidecar-source] handler err:', (err as Error).message);
        }
      }
    }
  }
}
