# crude-alpha-stream

A paid, real-time **WebSocket stream of derived crude-oil alpha** — CME
options-skew + futures microstructure — monetized **per-minute over a Tempo MPP
payment channel** (one on-chain deposit, off-chain vouchers, settle-on-close).

```
┌────────────────────┐    NDJSON over TCP       ┌──────────────────────────┐
│   sidecar          │  :5051 (internal only)   │   stream                 │
│  Databento GLBX    │ ───────────────────────► │  • computes alpha frame  │
│  CL futures + LO   │                          │  • Tempo MPP pay-gate     │
│  options (live)    │                          │  • WS /ws/stream/:sym    │
└────────────────────┘                          │  • dashboard /            │
                                                 └───────────┬──────────────┘
                                                    :7070 (public ingress)
                                                             │
                                                paying subscribers (bots, UI)
```

- **Price:** `$0.001 / minute`
- **Payment:** Tempo MPP only (cumulative EIP-712 vouchers). **One env toggle**
  flips between **mainnet** (default) and **testnet**.
- **Licensing:** only *derived* signals leave the stream. Raw Databento data
  never leaves the compose-internal sidecar (`:5051` is never exposed).

---

## What it serves

~1 Hz `snapshot` frames. Populated today from the Databento sidecar:

| Field | Meaning |
|---|---|
| `cme_fwd_mid` | CME front-future mid (live forward) |
| `skew_signal` | EWMA risk-reversal proxy in [-1, 1] (>0 bullish, <0 bearish) |
| `rr25` | 25-delta risk reversal (vol pts, put-call-parity forward) |
| `atm_iv`, `iv_term_slope`, `signed_flow_10`, `imb_5`, `funding` | additional alpha terms |

`health` is `ok` when the chain is fresh, else `stale_chain`.
`GET /skills` lists symbols, price, and payment info.

---

## Subscribe (quick start)

```bash
npm i mppx viem ws

# MAINNET (real pathUSD) — wallet must hold pathUSD on Tempo mainnet:
STREAM_HOST=34.104.223.186:7070 PRIVATE_KEY=0xYOURKEY node subscribe.mjs

# TESTNET (free — auto-funds from the Tempo faucet):
TEMPO_NETWORK=testnet STREAM_HOST=34.104.223.186:7070 node subscribe.mjs
```

`subscribe.mjs` opens one payment channel, pays $0.001 each minute (one channel,
many vouchers — the mppx *multi-fetch* pattern), prints live alpha, and on
`Ctrl-C` closes + settles on-chain (refunding the unused deposit). Full walk-through:
**[GUIDE.md](./GUIDE.md)**.

### Protocol (any language)

1. **Pay a voucher** (opens the channel on first call) with an mppx session:
   `session.fetch('http://<host>:7070/mpp/session?symbol=CL')` →
   `{ ok, symbol, token, expiresAt, minutes }`.
2. **Open the data WS** with the token:
   `ws://<host>:7070/ws/stream/CL?token=<token>` (node clients may instead send
   `Authorization: Bearer <token>`), then `{"t":"subscribe","symbols":["CL"]}`.
3. **Extend** on `{"t":"payment-need-voucher"}`: pay another voucher (same
   channel) and send `{"t":"authorization","token":"<new token>"}`.
4. **Close**: `session.close()` → on-chain settle + refund.

---

## Mainnet ⇄ testnet (the single toggle)

Set **one** env var on the server:

```bash
MPP_TESTNET=false   # Tempo MAINNET (default)
MPP_TESTNET=true    # Tempo testnet (Moderato)
```

| | Mainnet (`false`) | Testnet (`true`) |
|---|---|---|
| chain id | 4217 | 42431 |
| RPC | https://rpc.tempo.xyz | https://rpc.moderato.tempo.xyz |
| pathUSD | `0x20C0…E8b50` | `0x20c0…0000` |
| explorer | https://explore.tempo.xyz | https://explore.testnet.tempo.xyz |
| faucet | none (real funds) | open (`tempo_fundAddress`) |

The browser dashboard follows the same toggle (`window.MPP_TESTNET = true`
forces testnet). The recipient wallet (`MPP_RECIPIENT_KEY`) must hold pathUSD on
the selected network to pay settle/close gas — on mainnet that means real funds.

---

## Run / deploy

```bash
cp .env.example .env     # set DATABENTO_API_KEY, MPP_RECIPIENT_KEY, MPP_SECRET_KEY
docker compose up -d --build
```

- Dashboard `http://localhost:7070` · Health `/health` · Catalog `/skills`
- **Single instance only** — the sidecar holds exactly one Databento Live
  session; do not horizontally scale.
- For browser `wss://`, front `stream:7070` with a TLS terminator (e.g. Caddy)
  and drop the host port so only the proxy is public.

Live reference deployment: GCE VM in Tokyo, `http://34.104.223.186:7070`.

---

## Configuration (env)

| Var | Default | Notes |
|---|---|---|
| `DATABENTO_API_KEY` | — | **required**; live GLBX.MDP3 key (sidecar) |
| `STRIKES_WINDOW` | `40` | ATM strikes per side (±N at $0.50) |
| `STREAM_PORT` | `7070` | host port to publish on |
| `SIDECAR_EMIT_MS` | `1000` | snapshot cadence |
| `MPP_TESTNET` | `false` | **the mainnet/testnet toggle** |
| `MPP_RECIPIENT_KEY` | — | recipient signing key (pays settle gas). **secret** |
| `MPP_SECRET_KEY` | — | HMAC secret for stream tokens. **secret** |
| `MPP_MINUTES_PER_PAYMENT` | `1` | minutes granted per paid voucher |
| `MPP_AMOUNT` | `0.001` | pathUSD per voucher |

---

## Repo layout

```
crude-alpha-stream/
├─ docker-compose.yml        # sidecar (internal) + stream (public), single instance
├─ .env.example              # copy → .env  (real .env is gitignored)
├─ README.md · GUIDE.md      # this + the subscriber walk-through
├─ subscribe.mjs             # reference MPP multi-fetch subscribe client
├─ sidecar/                  # Databento → NDJSON TCP feed (Python)
└─ stream/                   # MPP-paid WebSocket stream (TypeScript)
   └─ src/                   # config · types · mpp · stream-token · ws-gateway · sidecar-source · server
```

---

## Security

- **Never expose the sidecar (`:5051`)** — compose-internal by design.
- `DATABENTO_API_KEY`, `MPP_RECIPIENT_KEY`, `MPP_SECRET_KEY`, and all
  `.mpp-*-key` files are **secrets** — keep them in `.env` (gitignored), never commit.
- The recipient wallet must be one you control and funded with pathUSD on the
  selected Tempo network before charging real subscribers.
```
