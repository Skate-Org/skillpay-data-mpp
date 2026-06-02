#!/usr/bin/env node
/**
 * Subscribe to the Crude Alpha Stream and pay over a Tempo MPP payment channel.
 *
 * Model (mppx "multi-fetch": one channel, many paid vouchers):
 *   1. Open ONE payment channel (on-chain deposit, once) via tempo.session().
 *   2. Each minute, make a paid fetch to /mpp/session  → server returns a signed
 *      stream token. Vouchers are CUMULATIVE on the single channel (no per-minute
 *      on-chain tx) — this is the multi-fetch pattern.
 *   3. Open the data WS with ?token=… and receive live alpha snapshots. When the
 *      server warns the window is ending, pay another voucher and send it in-band
 *      to extend — no reconnect, no new channel.
 *   4. On exit, close() the channel → server settles on-chain, unused deposit is
 *      refunded.
 *
 * Price: $0.001 / minute (set by the stream).
 *
 * Install:  npm i mppx viem ws
 * Run:
 *   # Mainnet (default) — PRIVATE_KEY must hold pathUSD on Tempo mainnet:
 *   STREAM_HOST=34.104.223.186:7070 PRIVATE_KEY=0x... node subscribe.mjs
 *
 *   # Testnet (auto-funds from the faucet, no real money):
 *   TEMPO_NETWORK=testnet STREAM_HOST=34.104.223.186:7070 node subscribe.mjs
 */
import { tempo } from 'mppx/client';
import { createClient, http } from 'viem';
import { generatePrivateKey, privateKeyToAccount } from 'viem/accounts';
import { Chain, Actions } from 'viem/tempo';
import WebSocket from 'ws';

// ── config ───────────────────────────────────────────────────────────────────
const NETWORK = (process.env.TEMPO_NETWORK ?? 'mainnet').toLowerCase(); // mainnet | testnet
const IS_TESTNET = NETWORK === 'testnet';
const HOST = process.env.STREAM_HOST ?? '34.104.223.186:7070';
const SYMBOL = process.env.SYMBOL ?? 'CL';
const MAX_DEPOSIT = process.env.MAX_DEPOSIT ?? '0.1'; // pathUSD locked in the channel (≈100 min at $0.001/min)
const HTTP_BASE = `http://${HOST}`;
const WS_BASE = `ws://${HOST}`;

const chain = IS_TESTNET ? Chain.testnet : Chain.mainnet;
const account = privateKeyToAccount((process.env.PRIVATE_KEY) ?? generatePrivateKey());
const client = createClient({ account, chain, pollingInterval: 1000, transport: http() });

console.log(`Crude Alpha subscriber · ${IS_TESTNET ? 'Tempo TESTNET' : 'Tempo MAINNET'} · ${HOST} · ${SYMBOL}`);
console.log(`payer: ${account.address}`);

// On testnet, top up from the faucet (free). On mainnet, the wallet must already
// hold pathUSD — there is no faucet.
if (IS_TESTNET) {
  console.log('funding from testnet faucet…');
  try { await Actions.faucet.fundSync(client, { account, timeout: 30_000 }); } catch (e) { console.warn('faucet:', e.message); }
}

// One channel for the whole session (multi-fetch).
const session = tempo.session({ client, maxDeposit: MAX_DEPOSIT });

// Pay one voucher → get a fresh stream token. Repeated calls reuse the SAME
// channel and bump the cumulative voucher (the multi-fetch pattern).
async function payForToken() {
  const res = await session.fetch(`${HTTP_BASE}/mpp/session?symbol=${SYMBOL}`);
  if (!res.ok) throw new Error(`payment failed: ${res.status} ${await res.text()}`);
  const body = await res.json();
  console.log(`paid voucher · cumulative=${session.cumulative ?? '?'} · token good until ${new Date(body.expiresAt).toLocaleTimeString()}`);
  return body.token;
}

let ws;
async function connect() {
  const token = await payForToken(); // first on-chain deposit happens here (once)
  ws = new WebSocket(`${WS_BASE}/ws/stream/${SYMBOL}?token=${encodeURIComponent(token)}`);

  ws.on('open', () => ws.send(JSON.stringify({ t: 'subscribe', symbols: [SYMBOL] })));
  ws.on('message', async (raw) => {
    const msg = JSON.parse(raw.toString());
    if (msg.t === 'snapshot') {
      const f = msg.data.fields ?? msg.data;
      const g = (k) => (f[k]?.v ?? f[k] ?? null);
      console.log(`[${SYMBOL}] skew=${fmt(g('skew_signal'))} rr25=${fmt(g('rr25'))} fwd=${fmt(g('cme_fwd_mid'))} atm_iv=${fmt(g('atm_iv'))}`);
    } else if (msg.t === 'payment-need-voucher') {
      // window ending → pay another voucher on the SAME channel and extend in-band
      try { ws.send(JSON.stringify({ t: 'authorization', token: await payForToken() })); }
      catch (e) { console.error('top-up failed:', e.message); ws.close(); }
    } else if (msg.t === 'payment-receipt') {
      // extended
    } else if (msg.t === 'error') {
      console.error('server error:', msg.code, msg.message);
    }
  });
  ws.on('close', () => console.log('ws closed'));
  ws.on('error', (e) => console.error('ws error:', e.message));
}

const fmt = (x) => (x == null ? 'null' : Number(x).toFixed(4));

// graceful close → settle on-chain, refund unused deposit
let closing = false;
async function shutdown() {
  if (closing) return; closing = true;
  console.log('\nclosing channel + settling on-chain…');
  try { if (ws) ws.close(); const r = await session.close(); console.log('settle:', JSON.stringify(r)); }
  catch (e) { console.error('close error:', e.message); }
  process.exit(0);
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

await connect();
