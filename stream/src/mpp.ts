/**
 * Server-side MPP rail on Tempo (proven config from scripts/mpp-channel-test.mjs).
 * Mounts a session-gated endpoint that the dashboard's mppx client hits to open
 * a payment channel (on-chain deposit) and settle/close. Mirrors the working
 * harness: Mppx.create + tempo({account, client, testnet}) + compose session.
 *
 * Enabled only when MPP_RECIPIENT_KEY is set (the recipient's signing key, which
 * also pays settle/close gas in stablecoin). Without it, the route is skipped so
 * the stream still runs.
 */
import type { Express } from 'express';
import { createWalletClient, http as vhttp, defineChain } from 'viem';
import { privateKeyToAccount } from 'viem/accounts';
import * as MppServer from 'mppx/server';
import { mintStreamToken } from './stream-token.js';

const MINUTES_PER_PAYMENT = Number(process.env.MPP_MINUTES_PER_PAYMENT ?? 1);

// MAINNET by default (set MPP_TESTNET=true to use Tempo testnet).
const IS_TESTNET = process.env.MPP_TESTNET === 'true';
const RPC = process.env.MPP_RPC ?? (IS_TESTNET ? 'https://rpc.moderato.tempo.xyz' : 'https://rpc.tempo.xyz');
const CHAIN_ID = Number(process.env.MPP_CHAIN_ID ?? (IS_TESTNET ? 42431 : 4217));
// pathUSD is the canonical TIP-20 stablecoin — same well-known address on both
// Tempo nets. (The mppx SDK *default* mainnet currency is USDC 0x20C0…E8b50, but
// we charge in pathUSD; override with MPP_CURRENCY if you want USDC.)
const PATHUSD = (process.env.MPP_CURRENCY ??
  '0x20c0000000000000000000000000000000000000') as `0x${string}`;
const AMOUNT = process.env.MPP_AMOUNT ?? '0.001'; // per paid request/unit (channel deposit covers it)

const tempoChain = defineChain({
  id: CHAIN_ID,
  name: IS_TESTNET ? 'Tempo Testnet' : 'Tempo',
  nativeCurrency: { name: 'Tempo Gas', symbol: 'GAS', decimals: 18 },
  rpcUrls: { default: { http: [RPC] } },
});

export function mountMpp(app: Express): { enabled: boolean; recipient?: string } {
  const key = process.env.MPP_RECIPIENT_KEY;
  if (!key) {
    console.log('[mpp] MPP_RECIPIENT_KEY not set — session route disabled');
    return { enabled: false };
  }
  const { Mppx, tempo, Request } = MppServer as any;
  const recipientAccount = privateKeyToAccount(key as `0x${string}`);
  const recipientWallet = createWalletClient({ account: recipientAccount, chain: tempoChain, transport: vhttp(RPC) });

  const mppx = Mppx.create({
    methods: [tempo({ account: recipientAccount, client: recipientWallet, testnet: IS_TESTNET, currency: PATHUSD })],
    secretKey: process.env.MPP_SECRET_KEY ?? 'crude-alpha-stream-dev-secret',
  });

  const fetchHandler = async (req: Request) => {
    const result = await mppx.compose(['tempo/session', { amount: AMOUNT, unitType: 'session' }])(req);
    if (result.status === 402) return result.challenge;
    // paid → mint a stream-access token (bridges payment → WS data)
    const symbol = new URL(req.url).searchParams.get('symbol') ?? 'CL';
    const token = mintStreamToken(symbol, MINUTES_PER_PAYMENT);
    const expiresAt = Date.now() + MINUTES_PER_PAYMENT * 60_000;
    return result.withReceipt(
      new Response(JSON.stringify({ ok: true, symbol, token, expiresAt, minutes: MINUTES_PER_PAYMENT }), {
        headers: { 'content-type': 'application/json' },
      })
    );
  };

  const listener = Request.toNodeListener(fetchHandler);
  app.all('/mpp/session', (req, res) => listener(req, res));
  console.log(`[mpp] session route ENABLED → recipient ${recipientAccount.address} (Tempo ${IS_TESTNET ? 'testnet' : 'MAINNET'} chain ${CHAIN_ID}, currency ${PATHUSD})`);
  return { enabled: true, recipient: recipientAccount.address };
}
