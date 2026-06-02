/**
 * Browser MPP client (bundled by esbuild → public/mpp.js).
 *
 * IMPORTANT: vanilla MetaMask CANNOT sign Tempo payment-channel transactions —
 * they use Tempo's native tx type 0x76 with `feeToken` (gas paid in stablecoin)
 * and require `eth_signTransaction`, neither of which MetaMask supports. So the
 * dashboard uses an in-browser Tempo wallet: a local viem account (persisted in
 * localStorage, auto-funded from the testnet faucet) that signs the channel ops
 * locally — exactly the flow proven in scripts/mpp-channel-test.mjs.
 *
 *   connect()      → load/create local Tempo wallet + faucet-fund it
 *   openSession()  → paid fetch ⇒ on-chain DEPOSIT (channel opens)
 *   closeSession() → on-chain CLOSE (settle + refund)
 * Both are real transactions on explore.testnet.tempo.xyz.
 */
import { createWalletClient, http, defineChain } from 'viem';
import { generatePrivateKey, privateKeyToAccount } from 'viem/accounts';
import { sessionManager } from '../../node_modules/mppx/dist/tempo/client/index.js';

// MAINNET. (window.MPP_TESTNET=true switches the dashboard back to testnet.)
const IS_TESTNET = (globalThis as any).MPP_TESTNET === true;
const RPC = IS_TESTNET ? 'https://rpc.moderato.tempo.xyz' : 'https://rpc.tempo.xyz';
const EXPLORER = IS_TESTNET ? 'https://explore.testnet.tempo.xyz' : 'https://explore.tempo.xyz';
const PATHUSD = IS_TESTNET
  ? '0x20c0000000000000000000000000000000000000'
  : '0x20C000000000000000000000b9537d11c60E8b50';
const tempoChain = defineChain({
  id: IS_TESTNET ? 42431 : 4217,
  name: IS_TESTNET ? 'Tempo Testnet' : 'Tempo',
  nativeCurrency: { name: 'Tempo Gas', symbol: 'GAS', decimals: 18 },
  rpcUrls: { default: { http: [RPC] } },
  blockExplorers: { default: { name: 'Tempo', url: EXPLORER } },
});

type SM = ReturnType<typeof sessionManager>;
const state: { addr: string | null; sm: SM | null; channelId: string | null; onUpdate: ((e: any) => void) | null } = {
  addr: null, sm: null, channelId: null, onUpdate: null,
};

function loadKey(): `0x${string}` {
  const k = 'oilalpha:tempo:key';
  let pk = localStorage.getItem(k) as `0x${string}` | null;
  if (!pk) { pk = generatePrivateKey(); localStorage.setItem(k, pk); }
  return pk;
}

async function faucet(addr: string): Promise<void> {
  // Faucet exists on TESTNET only. On mainnet the wallet must be pre-funded with
  // real pathUSD — there is no auto-fund.
  if (!IS_TESTNET) { console.warn('[mpp] mainnet: fund', addr, 'with pathUSD manually — no faucet'); return; }
  try {
    await fetch(RPC, { method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tempo_fundAddress', params: [addr] }) });
  } catch { /* best effort */ }
}

async function pathBal(addr: string): Promise<number> {
  try {
    const r = await fetch(RPC, { method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'eth_call',
        params: [{ to: PATHUSD, data: '0x70a08231' + addr.toLowerCase().replace('0x', '').padStart(64, '0') }, 'latest'] }) });
    const j = await r.json();
    return j?.result && j.result !== '0x' ? Number(BigInt(j.result)) / 1e6 : 0;
  } catch { return 0; }
}

async function connect(): Promise<string> {
  const account = privateKeyToAccount(loadKey());
  state.addr = account.address;
  const bal = await pathBal(account.address);
  if (bal < 1) await faucet(account.address); // fund if low
  const walletClient = createWalletClient({ account, chain: tempoChain, transport: http(RPC) });
  state.sm = sessionManager({
    client: walletClient,        // local account derived from the client → signs locally
    decimals: 6,
    maxDeposit: (window as any).MPP_MAX_DEPOSIT || '0.5',
    onChannelUpdate: (entry: any) => { state.channelId = entry?.channelId ?? state.channelId; state.onUpdate?.(entry); },
  });
  return state.addr!;
}

async function openSession(sessionUrl: string): Promise<{ channelId: string | null; status: number }> {
  if (!state.sm) throw new Error('connect() first');
  const res: any = await state.sm.fetch(sessionUrl);
  state.channelId = (state.sm as any).channelId ?? state.channelId;
  return { channelId: state.channelId, status: res.status };
}

async function closeSession(): Promise<any> {
  if (!state.sm) return null;
  return state.sm.close();
}

(globalThis as any).MPP = {
  connect, openSession, closeSession, EXPLORER, faucet, pathBal,
  get addr() { return state.addr; },
  get channelId() { return state.channelId; },
  get cumulative() { return state.sm ? String((state.sm as any).cumulative) : '0'; },
  onChannelUpdate(cb: (e: any) => void) { state.onUpdate = cb; },
};
