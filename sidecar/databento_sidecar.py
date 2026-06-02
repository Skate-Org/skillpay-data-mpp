# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "databento>=0.50.0",
#     "python-dotenv>=1.0.0",
# ]
# ///
"""
oil1 Databento sidecar (VOLGUARD v1).

Subscribes to GLBX.MDP3 live for:
  * CL.FUT  (mbp-1 + trades)        — front-month outright resolved from definitions.
  * LO.OPT  (mbp-1)                  — front 2 expiries, ATM +/- 20 strikes only.

Re-streams every event as NDJSON over a plain TCP server on
$SIDECAR_PORT (default 5051). One line per event. Events:

  {"t":"bbo","sym":"CLN6","ts":<ns>,"bid":..,"ask":..,"bid_sz":..,"ask_sz":..}
  {"t":"trade","sym":"CLN6","ts":<ns>,"px":..,"sz":..,"side":"B"|"A"|"N"}
  {"t":"lo_bbo","sym":"LON6 C10500","ts":<ns>,"bid":..,"ask":..,"strike":...,"is_call":true,"expiry_iso":"2026-06-17","tte_years":..}
  {"t":"hb","ts":<ns>,"n_fut":<int>,"n_lo":<int>,"front_fut":"CLN6"}

Connection model:
  - We listen, the Rust client connects. Single-client server (latest wins).
  - Buffered queue per client; on slow consumer we drop oldest (>4096 events).
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


# -------------------- Universe resolution --------------------

def _prev_trading_day() -> date:
    d = date.today() - timedelta(days=1)
    while d.weekday() >= 5:  # Sat/Sun
        d -= timedelta(days=1)
    return d


_MONTH_CODE = {"F": 1, "G": 2, "H": 3, "J": 4, "K": 5, "M": 6,
               "N": 7, "Q": 8, "U": 9, "V": 10, "X": 11, "Z": 12}


def _opt_expiry_code(fut_sym: str) -> str:
    """CL future raw symbol -> LO option expiry code. 'CLN6' -> 'N6'."""
    return fut_sym[2:] if fut_sym.upper().startswith("CL") else fut_sym


def _approx_lo_expiry(exp_code: str, ref: datetime) -> datetime:
    """Approximate the LO option expiration for a <monthletter><yeardigits> code.

    Exact expiry isn't needed for the skew SIGN (RR25 is a difference of IVs at
    the same expiry, so small tte errors largely cancel). CME WTI options
    terminate ~3 business days before the underlying future, which terminates
    ~3bd before the 25th of the month PRECEDING the contract month — i.e. roughly
    mid-month of the prior month. We approximate as the 17th of that prior month.
    """
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
        return datetime(ey, em, 17, 19, 30, tzinfo=timezone.utc)
    except ValueError:
        return datetime(ey, em, 17, tzinfo=timezone.utc)


def resolve_universe(api_key: str, strikes_window: int = 20) -> tuple[str, str, list[str], dict[str, dict]]:
    """Resolve the front CL future and LO option subset to subscribe to.

    Returns (front_fut_sym, list_of_lo_symbols, lo_meta_dict)
    where lo_meta_dict[raw_symbol] = {strike, is_call, expiry_iso, tte_years}
    """
    hist = db.Historical(key=api_key)
    day = _prev_trading_day()
    start = day.isoformat()
    end = (day + timedelta(days=1)).isoformat()

    log(f"resolving CL.FUT + LO.OPT for {start}")

    # 1) CL futures - pick front outright
    cl_def = hist.timeseries.get_range(
        dataset="GLBX.MDP3",
        schema="definition",
        symbols=["CL.FUT"],
        stype_in="parent",
        start=start,
        end=end,
    ).to_df()
    cl_fut = cl_def[cl_def["instrument_class"] == "F"].sort_values("expiration")
    if len(cl_fut) == 0:
        raise RuntimeError("no CL futures resolved")
    # Subscribe to FRONT + NEXT month to survive the roll period (front-month
    # expiry week). Sidecar's apply() locks `front_fut_sym` to whichever
    # contract delivers BBOs first — so the active contract wins regardless of
    # which is technically "front" by expiration date.
    front_fut_sym = str(cl_fut.iloc[0]["raw_symbol"])
    next_fut_sym = str(cl_fut.iloc[1]["raw_symbol"]) if len(cl_fut) > 1 else front_fut_sym
    front_settle = float(cl_fut.iloc[0].get("min_price_increment", 0) or 0)
    log(f"front CL future: {front_fut_sym} (also subscribing to {next_fut_sym} as roll fallback)")

    # 2) LO option chain. The historical `LO.OPT` PARENT definition pull
    # consistently 504s at Databento's gateway — the crude options parent is too
    # large to resolve within their ~60s server timeout (futures resolve fine).
    # Instead we CONSTRUCT a narrow ATM strike window directly and subscribe to
    # those raw_symbols on the live feed (validated: ~160 symbols all return
    # clean two-sided BBOs, no parent resolution needed). The forward proxy F
    # below is derived purely from CL futures, so we never hit the options
    # definition endpoint at all. Symbol construction happens after F is known.

    # Forward proxy: prefer settle stat_type=3; if missing, use the most recent
    # CL futures BBO mid from bbo-1m (this is the *live* forward, not a stale
    # historical median). The median-of-strikes fallback is last-resort only
    # because it returns the historical center of the chain, which can be
    # tens of dollars away from the live forward and breaks Δ=-0.25 IV interp.
    F_proxy: float | None = None

    # Probe order: try the next-month contract FIRST, then front. Reason: in
    # the roll window the front (e.g. CLM6) can have already expired and have
    # no recent quotes, while CLN6 is the actively-trading contract. The live
    # loop also auto-locks onto whichever delivers first, so we want our F
    # proxy to match that contract.
    probe_syms = list(dict.fromkeys([next_fut_sym, front_fut_sym]))

    # 1) Try statistics schema (settlement) for each candidate
    for psym in probe_syms:
        if F_proxy is not None:
            break
        try:
            st = hist.timeseries.get_range(
                dataset="GLBX.MDP3",
                schema="statistics",
                symbols=[psym],
                stype_in="raw_symbol",
                start=start,
                end=end,
            ).to_df()
            st = st[st["stat_type"] == 3]
            if len(st):
                v = float(st.iloc[-1]["price"])
                if v > 1e6:
                    v /= 1e9
                if 50 < v < 200:
                    F_proxy = v
                    log(f"forward proxy F = {F_proxy:.2f} (from {psym} settle stat_type=3)")
        except Exception as e:
            log(f"settle stat fetch failed for {psym}: {e}")

    # 2) Better fallback: most recent CL futures BBO mid (live forward).
    # Widen the lookback window to 5 days to ride out long weekends / quiet
    # sessions in the previous trading day.
    bbo_start = (day - timedelta(days=5)).isoformat()
    bbo_end = (day + timedelta(days=1)).isoformat()
    if F_proxy is None:
        for psym in probe_syms:
            if F_proxy is not None:
                break
            try:
                cl_bbo = hist.timeseries.get_range(
                    dataset="GLBX.MDP3",
                    schema="bbo-1m",
                    symbols=[psym],
                    stype_in="raw_symbol",
                    start=bbo_start,
                    end=bbo_end,
                ).to_df()
                if not len(cl_bbo):
                    log(f"bbo-1m: no data for {psym} in {bbo_start}..{bbo_end}")
                    continue
                # Discover column names — databento bbo-1m typically uses
                # bid_px_00 / ask_px_00, but historical schemas have shipped
                # bid_px / ask_px in older versions. Probe and adapt.
                cols = set(cl_bbo.columns)
                bid_col = next((c for c in ("bid_px_00", "bid_px", "bid_px_01") if c in cols), None)
                ask_col = next((c for c in ("ask_px_00", "ask_px", "ask_px_01") if c in cols), None)
                log(f"bbo-1m[{psym}] columns: bid={bid_col}, ask={ask_col} (rows={len(cl_bbo)})")
                if not (bid_col and ask_col):
                    continue
                # Walk from most recent backwards to find a valid two-sided quote
                for i in range(len(cl_bbo) - 1, -1, -1):
                    row = cl_bbo.iloc[i]
                    bid = float(row.get(bid_col, 0) or 0)
                    ask = float(row.get(ask_col, 0) or 0)
                    if bid > 1e6:
                        bid /= 1e9
                    if ask > 1e6:
                        ask /= 1e9
                    if bid > 0 and ask > 0 and 50 < (bid + ask) / 2 < 200:
                        F_proxy = (bid + ask) / 2
                        log(f"forward proxy F = {F_proxy:.2f} (from {psym} bbo-1m mid, row -{len(cl_bbo)-i})")
                        break
            except Exception as e:
                log(f"bbo-1m fallback failed for {psym}: {e}")

    # 3) Last-resort: fixed sane crude level (futures probes failed). The live
    # futures BBO will refine ATM in practice; this only seeds strike selection.
    if F_proxy is None or not (50 < F_proxy < 200):
        F_proxy = 85.0
        log(f"forward proxy F = {F_proxy:.2f} (LAST RESORT default; CL futures probes failed)")

    # Construct the narrow ATM chain: front 2 option expiries (from the front/
    # next CL future month codes, e.g. CLN6 -> LON6) x (ATM +/- strikes_window)
    # at $0.50 steps x {Call, Put}. Raw symbol format (validated against the live
    # feed): "LO<MonthYear> <C|P><strike*100>", e.g. "LON6 C8700" = $87.00 call.
    now = datetime.now(timezone.utc)
    opt_exps = list(dict.fromkeys([_opt_expiry_code(front_fut_sym), _opt_expiry_code(next_fut_sym)]))
    step = 0.5
    base = round(F_proxy / step) * step
    lo_symbols: list[str] = []
    lo_meta: dict[str, dict] = {}
    for exp_code in opt_exps:
        exp_dt = _approx_lo_expiry(exp_code, now)
        tte = max(1e-6, (exp_dt - now).total_seconds() / (365.25 * 86400))
        for k in range(-strikes_window, strikes_window + 1):
            strike = base + k * step
            if strike <= 0:
                continue
            code = f"{int(round(strike * 100))}"
            for cp, is_call in (("C", True), ("P", False)):
                sym = f"LO{exp_code} {cp}{code}"
                lo_symbols.append(sym)
                lo_meta[sym] = {
                    "strike": float(strike),
                    "is_call": is_call,
                    "expiry_iso": exp_dt.date().isoformat(),
                    "tte_years": tte,
                }

    log(f"constructed {len(lo_symbols)} LO symbols across expiries {opt_exps} "
        f"(ATM={base:.2f}, +/-{strikes_window} x $0.50, C+P) -- no parent definition pull")
    return front_fut_sym, next_fut_sym, lo_symbols, lo_meta


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
        self.front_fut_sym = ""

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
            "front_fut": hub.front_fut_sym,
        })


async def serve(hub: Hub, host: str, port: int) -> None:
    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = writer.get_extra_info("peername")
        log(f"client connected: {peer}")
        # Latest-wins: replace previous writer if any
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

def run_live(hub: Hub, api_key: str, front_fut: str, next_fut: str, lo_symbols: list[str], lo_meta: dict[str, dict], loop: asyncio.AbstractEventLoop) -> None:
    """Auto-reconnecting wrapper. The Databento Live session can drop (e.g.
    'Slow client detected', or the weekend CME close); when it does we wait with
    backoff and re-establish the subscription instead of dying permanently."""
    backoff = 2
    while True:
        try:
            _run_live_once(hub, api_key, front_fut, next_fut, lo_symbols, lo_meta, loop)
        except Exception as e:
            log(f"run_live wrapper caught: {e}")
        log(f"live stream down; reconnecting in {backoff}s")
        time.sleep(backoff)
        backoff = min(backoff * 2, 30)


def _run_live_once(hub: Hub, api_key: str, front_fut: str, next_fut: str, lo_symbols: list[str], lo_meta: dict[str, dict], loop: asyncio.AbstractEventLoop) -> None:
    """Blocking: pulls from databento.Live and emits events into the hub until
    the stream errors or ends (then the wrapper reconnects)."""
    live = db.Live(key=api_key)

    # CL futures: mbp-1 + trades for FRONT + NEXT contract (roll-period coverage).
    # `fut_syms` set is used downstream to accept BBOs for either.
    fut_syms = {front_fut, next_fut}
    live.subscribe(
        dataset="GLBX.MDP3",
        schema="mbp-1",
        stype_in="raw_symbol",
        symbols=list(fut_syms),
    )
    live.subscribe(
        dataset="GLBX.MDP3",
        schema="trades",
        stype_in="raw_symbol",
        symbols=list(fut_syms),
    )
    # NOTE: mbp-10 (depth) is NOT subscribed — the GLBX entitlement on this key
    # is mbp-1/trades/definition/statistics only ("Not authorized for mbp-10").
    # Book imbalance is derived downstream from the mbp-1 top-of-book bid_sz/ask_sz.
    # LO chain: mbp-1 only (no need for trades on options for VOLGUARD v1)
    # Subscribe in chunks of 100 to avoid hitting any per-subscribe symbol cap.
    chunk = 100
    for i in range(0, len(lo_symbols), chunk):
        live.subscribe(
            dataset="GLBX.MDP3",
            schema="mbp-1",
            stype_in="raw_symbol",
            symbols=lo_symbols[i:i + chunk],
        )

    # Note: databento.Live auto-starts on iteration; calling start() before
    # iterating raises "Cannot start iteration after streaming has started".
    log(f"connected to GLBX.MDP3; streaming {1 + len(lo_symbols)} symbols")

    # Build a fast symbol-id -> meta lookup. databento records carry
    # instrument_id (int); symbology mapping comes via SymbolMappingMsg.
    sym_by_id: dict[int, str] = {}

    def push(ev: dict) -> None:
        asyncio.run_coroutine_threadsafe(asyncio.sleep(0), loop)  # ensure loop alive
        hub.emit(ev)

    try:
        for rec in live:
            cls = type(rec).__name__
            # Symbol mapping
            if cls == "SymbolMappingMsg":
                try:
                    sym_by_id[int(rec.instrument_id)] = str(rec.stype_out_symbol)
                except Exception:
                    pass
                continue
            # Heartbeat / system messages
            if cls in ("SystemMsg", "ErrorMsg"):
                continue
            # MBP-1 BBO
            if cls in ("MBP1Msg", "Mbp1Msg"):
                iid = int(getattr(rec, "instrument_id", 0))
                sym = sym_by_id.get(iid, "")
                if not sym:
                    continue
                # levels[0] is the top
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
                if sym in fut_syms:
                    # Lock onto whichever futures sym delivers BBOs first; from
                    # then on, only emit that one to the bot (avoids confusing
                    # the bot's `front_fut_sym` auto-detect with two symbols).
                    if not hub.front_fut_sym:
                        hub.front_fut_sym = sym
                        log(f"locked active CL future: {sym}")
                    if sym != hub.front_fut_sym:
                        continue
                    hub.n_fut_evts += 1
                    push({
                        "t": "bbo",
                        "sym": sym,
                        "ts": ts_ns,
                        "bid": bid,
                        "ask": ask,
                        "bid_sz": bid_sz,
                        "ask_sz": ask_sz,
                    })
                else:
                    meta = lo_meta.get(sym)
                    if meta is None:
                        continue
                    hub.n_lo_evts += 1
                    push({
                        "t": "lo_bbo",
                        "sym": sym,
                        "ts": ts_ns,
                        "bid": bid,
                        "ask": ask,
                        "strike": meta["strike"],
                        "is_call": meta["is_call"],
                        "expiry_iso": meta["expiry_iso"],
                        "tte_years": meta["tte_years"],
                    })
                continue
            # Trades
            if cls in ("TradeMsg",):
                iid = int(getattr(rec, "instrument_id", 0))
                sym = sym_by_id.get(iid, "")
                # Only emit trades for the locked active futures sym
                if not hub.front_fut_sym or sym != hub.front_fut_sym:
                    continue
                try:
                    px = _scale_px(rec.price)
                    sz = int(rec.size)
                    side_raw = str(getattr(rec, "side", "N"))
                except Exception:
                    continue
                if px <= 0 or sz <= 0:
                    continue
                # Databento side: 'B'=buy aggressor, 'A'=sell aggressor, 'N'=none
                side = "B" if side_raw == "B" else ("A" if side_raw == "A" else "N")
                ts_ns = int(getattr(rec, "ts_event", 0)) or time.time_ns()
                hub.n_fut_evts += 1
                push({"t": "trade", "sym": sym, "ts": ts_ns, "px": px, "sz": sz, "side": side})
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
    # ATM ± N strikes per expiry. CL options have 0.5$ strike spacing, so 40 ⇒
    # F ± $20 of coverage. This is robust to a couple weeks of forward drift
    # without re-resolving the chain. Previous default (20 ⇒ F ± $10) caused
    # the chain to go one-sided when the forward drifted >$10 from startup F
    # — observed 2026-05-25: sidecar resolved at F=98.26 on 05-21, F dropped
    # to 91.23 by 05-25, 25Δ put (~K=84) was outside the subscribed range,
    # dRR_30m returned None for >24h. See reviews/sidecar_chain_bug_fix.md.
    parser.add_argument("--strikes-window", type=int, default=40)
    args = parser.parse_args()

    load_dotenv()
    api_key = os.environ.get("DATABENTO_API_KEY")
    if not api_key:
        log("ERROR: DATABENTO_API_KEY not set; aborting")
        return 1

    front_fut, next_fut, lo_syms, lo_meta = resolve_universe(api_key, strikes_window=args.strikes_window)

    hub = Hub()
    # Don't pre-fill front_fut_sym; let the live loop lock onto the first
    # futures sym that actually delivers BBOs (handles roll period).
    hub.front_fut_sym = ""

    loop = asyncio.get_running_loop()

    # Run databento.Live consumer in a thread
    import threading
    th = threading.Thread(
        target=run_live,
        args=(hub, api_key, front_fut, next_fut, lo_syms, lo_meta, loop),
        name="databento-live",
        daemon=True,
    )
    th.start()

    # Graceful shutdown
    stop_evt = asyncio.Event()

    def _sig(*_a):
        log("shutdown signal received")
        stop_evt.set()

    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(s, _sig)
        except (NotImplementedError, RuntimeError):
            # Windows: signal handlers limited; rely on KeyboardInterrupt
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
