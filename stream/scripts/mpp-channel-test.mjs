// Proves the real mppx payment channel on Tempo testnet end-to-end (no browser):
// stands up a minimal mppx-gated server, then drives a sessionManager client
// (funded private key) through open(deposit) → paid fetch (voucher) → close().
// Prints the on-chain tx hashes so we can confirm them on the explorer.
import http from 'node:http';
import { readFileSync } from 'node:fs';
import { createWalletClient, createPublicClient, http as vhttp, defineChain } from 'viem';
import { privateKeyToAccount } from 'viem/accounts';
import * as Server from 'mppx/server';
// sessionManager isn't in the public export map; deep-import the file directly.
import { sessionManager } from '../node_modules/mppx/dist/tempo/client/index.js';

const PK = readFileSync(new URL('../.mpp-test-key', import.meta.url), 'utf8').trim();
const account = privateKeyToAccount(PK);
const RECIPIENT_PK = readFileSync(new URL('../.mpp-recipient-key', import.meta.url), 'utf8').trim();
const recipientAccount = privateKeyToAccount(RECIPIENT_PK);
const RECIPIENT = recipientAccount.address;
const PATHUSD = '0x20c0000000000000000000000000000000000000';
const CHAIN_ID = 42431;
const RPC = 'https://rpc.moderato.tempo.xyz';

const tempoChain = defineChain({
  id: CHAIN_ID, name: 'Tempo Testnet',
  nativeCurrency: { name: 'gas', symbol: 'GAS', decimals: 18 },
  rpcUrls: { default: { http: [RPC] } },
});
const walletClient = createWalletClient({ account, chain: tempoChain, transport: vhttp(RPC) });
const recipientWallet = createWalletClient({ account: recipientAccount, chain: tempoChain, transport: vhttp(RPC) });
const publicClient = createPublicClient({ chain: tempoChain, transport: vhttp(RPC) });

// ── minimal mppx-gated server ───────────────────────────────────────────────
const { Mppx, tempo, Request } = Server; // NOTE: use the GLOBAL Response, not Server.Response
const mppx = Mppx.create({
  methods: [tempo({ account: recipientAccount, client: recipientWallet, testnet: true, currency: PATHUSD })],
  secretKey: 'dev-secret-please-change',
});

const fetchHandler = async (req) => {
  const result = await mppx.compose(['tempo/session', { amount: '0.05', unitType: 'tick' }])(req);
  if (result.status === 402) return result.challenge;
  return result.withReceipt(
    new Response(JSON.stringify({ ok: true, t: Date.now() }), { headers: { 'content-type': 'application/json' } })
  );
};
const server = http.createServer(Request.toNodeListener(fetchHandler));
await new Promise((r) => server.listen(8090, r));
console.log('mppx server on :8090, payer', account.address);

// ── client: open channel → paid fetch → close ──────────────────────────────
const sm = sessionManager({
  client: walletClient,
  decimals: 6,
  maxDeposit: '0.5',
  onChannelUpdate: (e) => console.log('[channel]', JSON.stringify(e).slice(0, 200)),
});
try {
  console.log('--- paid fetch (will open channel + deposit on-chain) ---');
  const res = await sm.fetch('http://localhost:8090/tick');
  console.log('fetch status', res.status, 'channelId', sm.channelId, 'cumulative', String(sm.cumulative));
  console.log('--- close channel (on-chain) ---');
  const receipt = await sm.close();
  console.log('close receipt', JSON.stringify(receipt).slice(0, 300));
} catch (e) {
  console.log('ERR', e?.message || e, '\n', (e?.stack || '').slice(0, 400));
} finally {
  server.close();
  process.exit(0);
}
