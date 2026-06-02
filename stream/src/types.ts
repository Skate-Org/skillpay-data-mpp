/**
 * Core types for the oil alpha WebSocket stream.
 *
 * The AlphaSnapshot is the canonical object we publish. Per-field timestamps
 * solve the mixed-cadence problem: `cme_fwd_mid` ticks sub-second while
 * `gex_call_wall` updates ~daily — both live in one frame, each carrying its
 * own freshness.
 */

export type Health = 'ok' | 'stale_chain' | 'one_sided';

export interface FieldValue {
  v: number | null;
  ts: number; // epoch ms when this field was last computed
}

/** The 15-field alpha feed (see reviews/alpha_ws_stream_design.md). */
export interface AlphaSnapshot {
  sym: string; // 'CL' | 'BRENT' | ...
  seq: number; // monotonic per symbol
  ts: number; // epoch ms — snapshot assembly time
  health: Health;
  fields: {
    // Tier 1 — directional alpha (options skew, ~1h horizon)
    rr25: FieldValue; // 25Δ risk-reversal level
    rr25_d4h: FieldValue; // 4h change in RR
    skew_signal: FieldValue; // normalized -1..+1 directional tilt
    // Tier 2 — regime / vol structure
    atm_iv: FieldValue;
    gex_put_wall: FieldValue;
    gex_call_wall: FieldValue;
    spot_vs_walls: FieldValue;
    iv_term_slope: FieldValue;
    // Tier 3 — lead price & basis
    cme_fwd_mid: FieldValue;
    cme_hl_basis_bps: FieldValue;
    // Tier 4 — HL-native microstructure
    signed_flow_10: FieldValue;
    imb_5: FieldValue;
    funding: FieldValue;
  };
}

/** Source that produces snapshots for one or more symbols. */
export interface AlphaSource {
  /** Returns the latest snapshot for a symbol, or null if not yet available. */
  latest(sym: string): AlphaSnapshot | null;
  /** Subscribe to every new snapshot. Returns an unsubscribe fn. */
  subscribe(handler: (snap: AlphaSnapshot) => void): () => void;
  /** Symbols this source produces. */
  symbols(): string[];
  start(): void;
  stop(): void;
}

// ─── WS protocol (server → client) ──────────────────────────────────────────

export type ServerMessage =
  | { t: 'snapshot'; data: AlphaSnapshot } // full snapshot on connect + on each tick
  | { t: 'payment-receipt'; reference: string; expiresAt: number }
  | { t: 'payment-need-voucher'; reason: string } // session window expiring — top up
  | { t: 'subscription'; active: string[]; entitled: string[] } // ack after subscribe/unsubscribe
  | { t: 'error'; code: string; message: string };

// client → server
export type ClientMessage =
  | { t: 'subscribe'; symbols: string[] }
  | { t: 'unsubscribe'; symbols: string[] }
  | { t: 'authorization'; credential: string }; // base64url MPP wire credential (in-band top-up)

// ─── MPP payment-rail abstraction ────────────────────────────────────────────

/**
 * Result of verifying a presented MPP credential.
 * `reference` is the settlement key (sigHash on Monad / channelId+voucher on Tempo).
 * `paidMinutes` is how much streaming entitlement the payment grants.
 */
export interface VerifyResult {
  ok: boolean;
  reference: string;
  paidMinutes: number;
  reason?: string;
}

// Payment is handled solely by the Tempo MPP rail (mpp.ts) + stream tokens
// (stream-token.ts). The old swappable PaymentRail/ERC-3009 (Monad) interface
// was removed in the MPP-only restructure.

/** Per-symbol pricing (price per minute of streaming, USD). */
export interface StreamSkillConfig {
  symbol: string;
  name: string;
  pricePerMinuteUsd: number;
  channels: string[]; // e.g. ['oil.options', 'oil.structure', 'oil.micro', 'regime']
}
