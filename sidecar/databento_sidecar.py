# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "databento>=0.50.0",
#     "python-dotenv>=1.0.0",
# ]
# ///
"""
oil1 Databento sidecar (VOLGUARD v2 — multi-product).

Subscribes to GLBX.MDP3 live for SEVERAL CME products in ONE Live session and
re-streams every event as NDJSON over a plain TCP server on $SIDECAR_PORT
(default 5051). One JSON line per event. Every market event carries a `prod`
tag so the downstream stream can keep an independent signal state per product.

Products (see PRODUCTS below): WTI (CL/LO), Gold (GC/OG), Silver (SI/SO).
Each product resolves its own front+next outright future and an ATM ± window of
options at the product's strike spacing.

Events:
  {"t":"bbo","prod":"CL","sym":"CLN6","ts":<ns>,"bid":..,"ask":..,"bid_sz":..,"ask_sz":..}
  {"t":"trade","prod":"CL","sym":"CLN6","ts":<ns>,"px":..,"sz":..,"side":"B"|"A"|"N"}
  {"t":"lo_bbo","prod":"CL","sym":"LON6 C10500","ts":<ns>,"bid":..,"ask":..,"strike":..,"is_call":true,"expiry_iso":"2026-06-17","tte_years":..}
  {"t":"hb","ts":<ns>,"n_fut":<int>,"n_lo":<int>,"fronts":{"CL":"CLN6",...}}

Connection model:
  - We listen, the stream client connects. Single-client server (latest wins).
  - Buffered queue; on slow consumer we drop oldest (>8192 events).
  - SIGINT shuts the Databento Live stream and TCP server cleanly.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys
import time
from datetime import date, timedelta, datetime, timezone
from typing import Any

import databento as db
from dotenv import load_dotenv


def log(msg: str) -> None:
    sys.stderr.write(f"[sidecar {time.strftime('%H:%M:%S')}] {msg}\n")
    sys.stderr.flush()


# -------------------- Product definitions --------------------
# Each product maps a CME futures root + options root onto the shared signal
# pipeline. `strike_step` is the option strike spacing; `strike_mult` encodes the
# strike into the Databento raw_symbol (CL: "LON6 C8700" = 87.00 → strike*100).
# `px_lo/px_hi` sanity-bound the forward proxy. `opt_exp_day` approximates the
# option expiry as that day of the month PRECEDING the contract month (exact day
# is not needed for the RR *sign* — IVs at the same expiry, tte errors cancel).
PRODUCTS: list[dict] = [
    {
        "prod": "CL", "fut_root": "CL.FUT", "opt_root": "LO",
        "strike_step": 0.5, "strike_mult": 100, "px_lo": 40, "px_hi": 200,
        "opt_exp_day": 17,
    },
    {
        # OG raw_symbol encodes the strike as a plain integer dollar value:
        # "OGZ6 C4130" = $4130 (verified via GLBX definition). strike_mult=1.
        "prod": "GOLD", "fut_root": "GC.FUT", "opt_root": "OG",
        "strike_step": 10.0, "strike_mult": 1, "px_lo": 800, "px_hi": 6000,
        "opt_exp_day": 24,
    },
    {
        "prod": "SILVER", "fut_root": "SI.FUT", "opt_root": "SO",
        "strike_step": 0.5, "strike_mult": 100, "px_lo": 5, "px_hi": 100,
        "opt_exp_day": 25,
    },
]

# How many front outright futures to subscribe per product, and how many front
# option expiries to construct. Taking several (not just front+next) means the
# actively-traded contract is always included even when the nearest month is a
# thin serial in delivery — e.g. COMEX silver on 2026-07-02 rolls Jul(N)→Sep(U),
# skipping the Aug(Q) serial, so front-by-expiration (N,Q) would miss the live U.
# The live loop locks onto whichever subscribed contract actually quotes.
FUT_DEPTH = int(os.environ.get("FUT_DEPTH", "4"))
OPT_EXP_DEPTH = int(os.environ.get("OPT_EXP_DEPTH", "3"))


# -------------------- Universe resolution --------------------

def _prev_trading_day() -> date:
    d = date.today() - timedelta(days=1)
    while d.weekday() >= 5:  # Sat/Sun
        d -= timedelta(days=1)
    return d


_MONTH_CODE = {"F": 1, "G": 2, "H": 3, "J": 4, "K": 5, "M": 6,
               "N": 7, "Q": 8, "U": 9, "V": 10, "X": 11, "Z": 12}


def _opt_expiry_code(fut_sym: str, fut_root: str) -> str:
    """Front future raw symbol -> option expiry code, e.g. 'CLN6'->'N6', 'GCZ6'->'Z6'.

    The root is the 2-letter product code (CL/GC/SI); strip it to get <MonthYear>.
    """
    root2 = fut_root[:2].upper()
    return fut_sym[len(root2):] if fut_sym.upper().startswith(root2) else fut_sym


def _approx_opt_expiry(exp_code: str, ref: datetime, exp_day: int) -> datetime:
    """Approximate an option expiration for a <monthletter><yeardigits> code as
    `exp_day` of the month PRECEDING the contract month (CME energy/metals options
    terminate a few business days before the underlying, which is in the prior
    month). Only used for tte; RR is a same-expiry IV difference so small errors
    largely cancel."""
    m = _MONTH_CODE.get(exp_code[:1].upper())
    ydigits = exp_code[1:]
    if m is None or not ydigits.isdigit():
        return ref + timedelta(days=30)
    if len(ydigits) == 1:
        base = (ref.year // 10) * 10 + int(ydigits)
        year = base if base >= ref.year else base + 10
    else:
        year = 2000 + int(ydigits)
    em = m - 1 if m > 1 else 12
    ey = year if m > 1 else year - 1
    try:
        return datetime(ey, em, exp_day, 19, 30, tzinfo=timezone.utc)
    except ValueError:
        return datetime(ey, em, exp_day, tzinfo=timezone.utc)


def _forward_proxy(hist: "db.Historical", probe_syms: list[str], day: date,
                   px_lo: float, px_hi: float) -> float | None:
    """Resolve a forward proxy for a product from (1) settle stat_type=3, then
    (2) most-recent futures bbo-1m mid. Returns None if both fail."""
    start = day.isoformat()
    end = (day + timedelta(days=1)).isoformat()
    # 1) statistics settlement
    for psym in probe_syms:
        try:
            st = hist.timeseries.get_range(
                dataset="GLBX.MDP3", schema="statistics", symbols=[psym],
                stype_in="raw_symbol", start=start, end=end,
            ).to_df()
            st = st[st["stat_type"] == 3]
            if len(st):
                v = float(st.iloc[-1]["price"])
                if v > 1e6:
                    v /= 1e9
                if px_lo < v < px_hi:
                    log(f"forward proxy F = {v:.2f} (from {psym} settle stat_type=3)")
                    return v
        except Exception as e:
            log(f"settle stat fetch failed for {psym}: {e}")
    # 2) bbo-1m mid (live-ish forward), widen lookback to ride out quiet sessions
    bbo_start = (day - timedelta(days=5)).isoformat()
    bbo_end = (day + timedelta(days=1)).isoformat()
    for psym in probe_syms:
        try:
            cl_bbo = hist.timeseries.get_range(
                dataset="GLBX.MDP3", schema="bbo-1m", symbols=[psym],
                stype_in="raw_symbol", start=bbo_start, end=bbo_end,
            ).to_df()
            if not len(cl_bbo):
                continue
            cols = set(cl_bbo.columns)
            bid_col = next((c for c in ("bid_px_00", "bid_px", "bid_px_01") if c in cols), None)
            ask_col = next((c for c in ("ask_px_00", "ask_px", "ask_px_01") if c in cols), None)
            if not (bid_col and ask_col):
                continue
            for i in range(len(cl_bbo) - 1, -1, -1):
                row = cl_bbo.iloc[i]
                bid = float(row.get(bid_col, 0) or 0)
                ask = float(row.get(ask_col, 0) or 0)
                if bid > 1e6:
                    bid /= 1e9
                if ask > 1e6:
                    ask /= 1e9
                if bid > 0 and ask > 0 and px_lo < (bid + ask) / 2 < px_hi:
                    F = (bid + ask) / 2
                    log(f"forward proxy F = {F:.2f} (from {psym} bbo-1m mid, row -{len(cl_bbo)-i})")
                    return F
        except Exception as e:
            log(f"bbo-1m fallback failed for {psym}: {e}")
    return None


def resolve_product(hist: "db.Historical", p: dict, day: date, strikes_window: int):
    """Resolve one product's front FUT_DEPTH futures and ATM ± window option
    symbols across the front OPT_EXP_DEPTH expiries.

    Returns (fut_list, lo_symbols, lo_meta) where fut_list is the front futures'
    raw symbols (nearest first) and lo_meta[raw_symbol] =
    {prod, strike, is_call, expiry_iso, tte_years}.
    Raises on hard failure (no futures resolved)."""
    prod = p["prod"]
    start = day.isoformat()
    end = (day + timedelta(days=1)).isoformat()
    log(f"[{prod}] resolving {p['fut_root']} + {p['opt_root']}.OPT for {start}")

    # 1) futures front/next outright from the definition parent (this endpoint
    #    resolves reliably for futures; the OPTIONS parent 504s → we construct).
    cl_def = hist.timeseries.get_range(
        dataset="GLBX.MDP3", schema="definition", symbols=[p["fut_root"]],
        stype_in="parent", start=start, end=end,
    ).to_df()
    cl_fut = cl_def[cl_def["instrument_class"] == "F"].sort_values("expiration")
    if len(cl_fut) == 0:
        raise RuntimeError(f"[{prod}] no futures resolved for {p['fut_root']}")
    n_fut = min(max(2, FUT_DEPTH), len(cl_fut))
    fut_list = [str(cl_fut.iloc[i]["raw_symbol"]) for i in range(n_fut)]
    log(f"[{prod}] front futures (by expiry): {fut_list}")

    # 2) forward proxy — probe across the front futures; the first in-band value
    #    wins (a thin serial may have a stale/absent settle). Options recenter via
    #    put-call parity downstream, so this only needs to seed strike placement.
    F = _forward_proxy(hist, fut_list, day, p["px_lo"], p["px_hi"])
    if F is None or not (p["px_lo"] < F < p["px_hi"]):
        F = (p["px_lo"] + p["px_hi"]) / 2 if F is None else F
        # last-resort seed; live futures BBO refines ATM in practice
        log(f"[{prod}] forward proxy F = {F:.2f} (LAST RESORT default; futures probes failed)")

    # 3) construct the ATM ± window option chain at the product's strike spacing.
    #    Raw symbol format (validated for CL): "<optroot><MonthYear> <C|P><strike*mult>".
    now = datetime.now(timezone.utc)
    opt_exps = list(dict.fromkeys(
        _opt_expiry_code(s, p["fut_root"]) for s in fut_list
    ))[:OPT_EXP_DEPTH]
    step = float(p["strike_step"])
    mult = int(p["strike_mult"])
    base = round(F / step) * step
    lo_symbols: list[str] = []
    lo_meta: dict[str, dict] = {}
    for exp_code in opt_exps:
        exp_dt = _approx_opt_expiry(exp_code, now, int(p["opt_exp_day"]))
        tte = max(1e-6, (exp_dt - now).total_seconds() / (365.25 * 86400))
        for k in range(-strikes_window, strikes_window + 1):
            strike = base + k * step
            if strike <= 0:
                continue
            code = f"{int(round(strike * mult))}"
            for cp, is_call in (("C", True), ("P", False)):
                sym = f"{p['opt_root']}{exp_code} {cp}{code}"
                lo_symbols.append(sym)
                lo_meta[sym] = {
                    "prod": prod,
                    "strike": float(strike),
                    "is_call": is_call,
                    "expiry_iso": exp_dt.date().isoformat(),
                    "tte_years": tte,
                }
    log(f"[{prod}] constructed {len(lo_symbols)} option symbols across {opt_exps} "
        f"(ATM={base:.2f}, +/-{strikes_window} x {step}, C+P)")
    return fut_list, lo_symbols, lo_meta


# -------------------- Wire helpers --------------------

def _scale_px(raw: Any) -> float:
    """databento.Live prices for fixed-point fields are int * 1e-9."""
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return 0.0
    if v > 1e6:
        return v / 1e9
    return v


# -------------------- Pump --------------------

class Hub:
    def __init__(self) -> None:
        self.queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=8192)
        self.client_writer: asyncio.StreamWriter | None = None
        self.n_fut_evts = 0
        self.n_lo_evts = 0
        self.fronts: dict[str, str] = {}  # prod -> active future raw symbol (emitted)
        self.front_ts: dict[str, int] = {}  # prod -> wall-clock ns of active contract's last BBO
        self.fut_counts: dict[str, dict[str, int]] = {}  # prod -> {sym: BBO count} (liquidity)

    def emit(self, ev: dict) -> None:
        line = (json.dumps(ev, separators=(",", ":")) + "\n").encode("utf-8")
        try:
            self.queue.put_nowait(line)
        except asyncio.QueueFull:
            try:
                _ = self.queue.get_nowait()
                self.queue.task_done()
            except Exception:
                pass
            try:
                self.queue.put_nowait(line)
            except Exception:
                pass


async def tcp_pump(hub: Hub) -> None:
    while True:
        line = await hub.queue.get()
        w = hub.client_writer
        if w is None:
            hub.queue.task_done()
            continue
        try:
            w.write(line)
            await w.drain()
        except Exception as e:
            log(f"client write error: {e}; dropping client")
            hub.client_writer = None
        finally:
            hub.queue.task_done()


async def heartbeat(hub: Hub, every_ms: int = 1000) -> None:
    while True:
        await asyncio.sleep(every_ms / 1000.0)
        hub.emit({
            "t": "hb",
            "ts": time.time_ns(),
            "n_fut": hub.n_fut_evts,
            "n_lo": hub.n_lo_evts,
            "fronts": dict(hub.fronts),
        })


async def serve(hub: Hub, host: str, port: int) -> None:
    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = writer.get_extra_info("peername")
        log(f"client connected: {peer}")
        if hub.client_writer is not None:
            try:
                hub.client_writer.close()
            except Exception:
                pass
        hub.client_writer = writer
        try:
            while True:
                data = await reader.read(4096)
                if not data:
                    break
        except Exception as e:
            log(f"client read error: {e}")
        finally:
            log(f"client disconnected: {peer}")
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            if hub.client_writer is writer:
                hub.client_writer = None

    server = await asyncio.start_server(handle, host, port)
    log(f"TCP server listening on {host}:{port}")
    async with server:
        await server.serve_forever()


# -------------------- Databento Live ingest --------------------

def run_live(hub: Hub, api_key: str, fut_syms: dict[str, str], lo_meta: dict[str, dict],
             all_fut: list[str], all_lo: list[str], prod_by_sym: dict[str, str],
             loop: asyncio.AbstractEventLoop) -> None:
    """Auto-reconnecting wrapper. Rebuilds the full multi-product subscription on
    every (re)connect so a transient drop can't leave us wedged."""
    backoff = 2
    while True:
        try:
            _run_live_once(hub, api_key, fut_syms, lo_meta, all_fut, all_lo, prod_by_sym, loop)
        except Exception as e:
            log(f"run_live wrapper caught: {e}")
        log(f"live stream down; reconnecting in {backoff}s")
        time.sleep(backoff)
        backoff = min(backoff * 2, 30)


def _run_live_once(hub: Hub, api_key: str, fut_syms: dict[str, str], lo_meta: dict[str, dict],
                   all_fut: list[str], all_lo: list[str], prod_by_sym: dict[str, str],
                   loop: asyncio.AbstractEventLoop) -> None:
    """Blocking: pulls from databento.Live and emits tagged events until the
    stream errors or ends (then the wrapper reconnects)."""
    live = db.Live(key=api_key)

    # Futures: mbp-1 + trades for every product's FRONT + NEXT contract.
    fut_set = list(dict.fromkeys(all_fut))
    live.subscribe(dataset="GLBX.MDP3", schema="mbp-1", stype_in="raw_symbol", symbols=fut_set)
    live.subscribe(dataset="GLBX.MDP3", schema="trades", stype_in="raw_symbol", symbols=fut_set)
    # NOTE: mbp-10 (depth) is NOT entitled on this key ("Not authorized for mbp-10").
    # Book imbalance is derived downstream from mbp-1 top-of-book bid_sz/ask_sz.
    # Options: mbp-1 only, subscribe in chunks of 100 to stay under any per-call cap.
    chunk = 100
    for i in range(0, len(all_lo), chunk):
        live.subscribe(dataset="GLBX.MDP3", schema="mbp-1", stype_in="raw_symbol",
                       symbols=all_lo[i:i + chunk])

    log(f"connected to GLBX.MDP3; streaming {len(fut_set)} futures + {len(all_lo)} options")

    # fast raw_symbol set for the futures of each product (for the front-lock check)
    fut_prod = {s: prod_by_sym.get(s, "") for s in fut_set}
    sym_by_id: dict[int, str] = {}

    def push(ev: dict) -> None:
        asyncio.run_coroutine_threadsafe(asyncio.sleep(0), loop)  # ensure loop alive
        hub.emit(ev)

    try:
        for rec in live:
            cls = type(rec).__name__
            if cls == "SymbolMappingMsg":
                try:
                    sym_by_id[int(rec.instrument_id)] = str(rec.stype_out_symbol)
                except Exception:
                    pass
                continue
            if cls in ("SystemMsg", "ErrorMsg"):
                continue
            if cls in ("MBP1Msg", "Mbp1Msg"):
                iid = int(getattr(rec, "instrument_id", 0))
                sym = sym_by_id.get(iid, "")
                if not sym:
                    continue
                try:
                    lv = rec.levels[0]
                    bid = _scale_px(lv.bid_px)
                    ask = _scale_px(lv.ask_px)
                    bid_sz = int(lv.bid_sz)
                    ask_sz = int(lv.ask_sz)
                except Exception:
                    continue
                if bid <= 0 or ask <= 0:
                    continue
                ts_ns = int(getattr(rec, "ts_event", 0)) or time.time_ns()
                if sym in fut_prod:
                    prod = fut_prod[sym]
                    # Emit ONE contract per product — the most actively-quoting one.
                    # We subscribe the front few outrights (some may be thin serials
                    # or a front-month in delivery); pick the active contract by BBO
                    # count so we track the liquid month (e.g. silver Sep not the Aug
                    # serial), with hysteresis to avoid flapping and a staleness
                    # failover so a dying contract hands off immediately.
                    wall = time.time_ns()
                    counts = hub.fut_counts.setdefault(prod, {})
                    counts[sym] = counts.get(sym, 0) + 1
                    active = hub.fronts.get(prod)
                    if active is None:
                        hub.fronts[prod] = active = sym
                        hub.front_ts[prod] = wall
                        log(f"[{prod}] active future: {sym}")
                    elif sym != active:
                        stale = wall - hub.front_ts.get(prod, 0) > 6_000_000_000
                        if stale or counts[sym] > counts.get(active, 0) + 8:
                            hub.fronts[prod] = active = sym
                            hub.front_ts[prod] = wall
                            log(f"[{prod}] switched active future -> {sym}"
                                f"{' (previous went quiet)' if stale else ' (more liquid)'}")
                        else:
                            continue
                    else:
                        hub.front_ts[prod] = wall
                    hub.n_fut_evts += 1
                    push({"t": "bbo", "prod": prod, "sym": sym, "ts": ts_ns,
                          "bid": bid, "ask": ask, "bid_sz": bid_sz, "ask_sz": ask_sz})
                else:
                    meta = lo_meta.get(sym)
                    if meta is None:
                        continue
                    hub.n_lo_evts += 1
                    push({"t": "lo_bbo", "prod": meta["prod"], "sym": sym, "ts": ts_ns,
                          "bid": bid, "ask": ask, "strike": meta["strike"],
                          "is_call": meta["is_call"], "expiry_iso": meta["expiry_iso"],
                          "tte_years": meta["tte_years"]})
                continue
            if cls in ("TradeMsg",):
                iid = int(getattr(rec, "instrument_id", 0))
                sym = sym_by_id.get(iid, "")
                prod = fut_prod.get(sym, "")
                # Only emit trades for a product's locked active future.
                if not prod or hub.fronts.get(prod) != sym:
                    continue
                try:
                    px = _scale_px(rec.price)
                    sz = int(rec.size)
                    side_raw = str(getattr(rec, "side", "N"))
                except Exception:
                    continue
                if px <= 0 or sz <= 0:
                    continue
                side = "B" if side_raw == "B" else ("A" if side_raw == "A" else "N")
                ts_ns = int(getattr(rec, "ts_event", 0)) or time.time_ns()
                hub.n_fut_evts += 1
                push({"t": "trade", "prod": prod, "sym": sym, "ts": ts_ns,
                      "px": px, "sz": sz, "side": side})
                continue
    except Exception as e:
        log(f"live loop error: {e}")
    finally:
        try:
            live.stop()
        except Exception:
            pass
        log("live stream stopped")


# -------------------- Main --------------------

async def amain() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default=os.environ.get("SIDECAR_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("SIDECAR_PORT", "5051")))
    # ATM ± N strikes per expiry, at each product's strike spacing. 40 gives wide
    # coverage that survives a couple weeks of forward drift without re-resolving.
    parser.add_argument("--strikes-window", type=int,
                        default=int(os.environ.get("STRIKES_WINDOW", "40")))
    args = parser.parse_args()

    load_dotenv()
    api_key = os.environ.get("DATABENTO_API_KEY")
    if not api_key:
        log("ERROR: DATABENTO_API_KEY not set; aborting")
        return 1

    # Which products to stream (default all). PRODS env = comma-sep prod codes.
    want = {s.strip().upper() for s in os.environ.get("PRODS", "").split(",") if s.strip()}
    products = [p for p in PRODUCTS if not want or p["prod"] in want]

    hist = db.Historical(key=api_key)
    day = _prev_trading_day()

    fut_syms: dict[str, str] = {}   # prod -> front (informational)
    lo_meta: dict[str, dict] = {}   # option raw_symbol -> meta (incl prod)
    all_fut: list[str] = []
    all_lo: list[str] = []
    prod_by_sym: dict[str, str] = {}
    for p in products:
        try:
            futs, lo_syms, meta = resolve_product(hist, p, day, args.strikes_window)
        except Exception as e:
            log(f"[{p['prod']}] resolve FAILED, skipping: {e}")
            continue
        fut_syms[p["prod"]] = futs[0]
        for s in futs:
            if s not in prod_by_sym:
                prod_by_sym[s] = p["prod"]
                all_fut.append(s)
        all_lo.extend(lo_syms)
        lo_meta.update(meta)

    if not all_fut:
        log("ERROR: no products resolved; aborting")
        return 1
    log(f"resolved {len(fut_syms)} products: {list(fut_syms.keys())}; "
        f"{len(all_fut)} futures + {len(all_lo)} options total")

    hub = Hub()
    loop = asyncio.get_running_loop()

    import threading
    th = threading.Thread(
        target=run_live,
        args=(hub, api_key, fut_syms, lo_meta, all_fut, all_lo, prod_by_sym, loop),
        name="databento-live", daemon=True,
    )
    th.start()

    stop_evt = asyncio.Event()

    def _sig(*_a):
        log("shutdown signal received")
        stop_evt.set()

    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(s, _sig)
        except (NotImplementedError, RuntimeError):
            pass

    server_task = asyncio.create_task(serve(hub, args.host, args.port))
    pump_task = asyncio.create_task(tcp_pump(hub))
    hb_task = asyncio.create_task(heartbeat(hub))

    try:
        await stop_evt.wait()
    except KeyboardInterrupt:
        log("KeyboardInterrupt")
    finally:
        for t in (server_task, pump_task, hb_task):
            t.cancel()
        for t in (server_task, pump_task, hb_task):
            try:
                await t
            except Exception:
                pass
    return 0


def main() -> int:
    try:
        return asyncio.run(amain())
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())
