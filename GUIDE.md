# Subscribing to the Crude Alpha Stream (Tempo MPP)

Live CME-options-derived crude alpha (WTI `CL`, Brent `BRENT`) over a WebSocket,
paid per-minute over a **Tempo MPP payment channel**. No accounts, no API keys —
you open one on-chain payment channel and stream pay-as-you-go.

- **Price:** `$0.001 / minute`
- **Payment rail:** Tempo MPP (cumulative EIP-712 vouchers; one channel, many
  fetches). **Mainnet by default; one toggle flips to testnet.**
- **Endpoint:** `http://<host>:7070` (default host `34.104.223.186:7070`)

---

## How it works (multi-fetch payment channel)

```
1. open channel        → ONE on-chain deposit (escrow locks maxDeposit)
2. pay a voucher       → POST /mpp/session?symbol=CL   → server returns a stream token   ┐ repeat
3. stream data         → WS /ws/stream/CL?token=<token> → live alpha snapshots           │ every
4. window ending       → pay another voucher (same channel, cumulative) → extend in-band  ┘ minute
5. close               → ONE on-chain settle: server keeps spent, refunds the rest
```

Only **two** on-chain transactions for the whole session (open + close); every
per-minute payment in between is an off-chain signed voucher. This is the mppx
[multi-fetch](https://github.com/wevm/mppx/tree/main/examples/session) pattern.

---

## Quick start (script)

```bash
npm i mppx viem ws

# MAINNET (real pathUSD) — wallet must already hold pathUSD on Tempo mainnet:
STREAM_HOST=34.104.223.186:7070 PRIVATE_KEY=0xYOURKEY node subscribe.mjs

# TESTNET (free — auto-funds from the Tempo faucet):
TEMPO_NETWORK=testnet STREAM_HOST=34.104.223.186:7070 node subscribe.mjs
```

`subscribe.mjs` (in this repo) opens the channel, pays $0.001 each minute on the
single channel, prints live `skew / rr25 / forward / atm_iv`, and on `Ctrl-C`
closes + settles on-chain (refunding the unused deposit).

| env | default | meaning |
|-----|---------|---------|
| `TEMPO_NETWORK` | `mainnet` | `mainnet` or `testnet` — the **flick toggle** |
| `STREAM_HOST` | `34.104.223.186:7070` | stream host:port |
| `SYMBOL` | `CL` | `CL` (WTI) or `BRENT` |
| `PRIVATE_KEY` | (generated) | payer key; **required on mainnet**, pre-funded with pathUSD |
| `MAX_DEPOSIT` | `0.1` | pathUSD locked in the channel (≈100 min @ $0.001/min) |

---

## Funding (mainnet)

Tempo mainnet has **no faucet** — fund the payer wallet with real **pathUSD**
(`0x20C000000000000000000000b9537d11c60E8b50`, 6 decimals) before subscribing.
`MAX_DEPOSIT` is locked into the channel on open and the unused remainder is
refunded on close.

On **testnet** the script calls the open faucet automatically (free pathUSD), so
you can validate the full flow end-to-end with no real money first.

---

## Manual flow (any language)

1. **Pay a voucher** (opens the channel on the first call):

   Use an mppx client (`tempo.session({ client, maxDeposit })`) and
   `session.fetch('http://<host>:7070/mpp/session?symbol=CL')`. The JSON response is:

   ```json
   { "ok": true, "symbol": "CL", "token": "CL.<exp>.<hmac>", "expiresAt": 1750000000000, "minutes": 1 }
   ```

2. **Open the data WS** with the token:

   ```
   ws://<host>:7070/ws/stream/CL?token=<token>
   ```
   (node clients may instead send `Authorization: Bearer <token>`.)
   Then send `{"t":"subscribe","symbols":["CL"]}`. You'll receive
   `{"t":"snapshot","data":{…}}` frames live.

3. **Extend** before expiry: when you get `{"t":"payment-need-voucher"}`, pay
   another voucher (step 1, same session/channel) and send
   `{"t":"authorization","token":"<new token>"}` over the WS.

4. **Close**: `session.close()` → on-chain settle + refund.

---

## Switching mainnet ⇄ testnet (operator)

A single env toggle on the **server** selects the network:

```bash
# docker-compose.yml / .env on the stream host
MPP_TESTNET=false   # Tempo MAINNET (default)
MPP_TESTNET=true    # Tempo testnet (Moderato)
MPP_RECIPIENT_KEY=0x...   # recipient signing key (pays settle/close gas in stablecoin)
```

| | Mainnet (`MPP_TESTNET=false`) | Testnet (`MPP_TESTNET=true`) |
|---|---|---|
| chain id | 4217 | 42431 |
| RPC | https://rpc.tempo.xyz | https://rpc.moderato.tempo.xyz |
| pathUSD | 0x20C0…E8b50 | 0x20c0…0000 |
| explorer | https://explore.tempo.xyz | https://explore.testnet.tempo.xyz |
| faucet | none (real funds) | open (`tempo_fundAddress`) |

The browser dashboard follows the same toggle (default mainnet;
`window.MPP_TESTNET = true` forces testnet).

> The recipient wallet (`MPP_RECIPIENT_KEY`) must hold pathUSD on the selected
> network to pay settle/close gas. On mainnet that means **real funds**.
