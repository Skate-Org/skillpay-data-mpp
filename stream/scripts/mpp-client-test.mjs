// Client-only: drive a real channel against the DEPLOYED /mpp/session endpoint.
import { readFileSync } from 'node:fs';
import { createWalletClient, http as vhttp, defineChain } from 'viem';
import { privateKeyToAccount } from 'viem/accounts';
import { sessionManager } from '../node_modules/mppx/dist/tempo/client/index.js';

const ENDPOINT = process.env.MPP_URL || 'http://34.104.223.186:7070/mpp/session';
const account = privateKeyToAccount(readFileSync(new URL('../.mpp-test-key', import.meta.url), 'utf8').trim());
const tempo = defineChain({ id: 42431, name: 'Tempo Testnet', nativeCurrency: { name: 'gas', symbol: 'GAS', decimals: 18 }, rpcUrls: { default: { http: ['https://rpc.moderato.tempo.xyz'] } } });
const walletClient = createWalletClient({ account, chain: tempo, transport: vhttp('https://rpc.moderato.tempo.xyz') });

const sm = sessionManager({ client: walletClient, decimals: 6, maxDeposit: '0.5',
  onChannelUpdate: (e) => console.log('[channel-update]', JSON.stringify(e)) });
try {
  console.log('payer', account.address, '→', ENDPOINT);
  const res = await sm.fetch(ENDPOINT);
  console.log('OPEN: fetch status', res.status, 'channelId', sm.channelId, 'cumulative', String(sm.cumulative));
  const receipt = await sm.close();
  console.log('CLOSE receipt:', JSON.stringify(receipt));
  // pull the on-chain tx hashes for this address from the explorer
  await new Promise((r) => setTimeout(r, 4000));
  const ex = await fetch(`https://explore.testnet.tempo.xyz/api/v2/addresses/${account.address}/transactions`);
  const j = await ex.json();
  const txs = (j.items || j.result || []).slice(0, 6);
  console.log('\n=== recent on-chain txs for ' + account.address + ' ===');
  for (const t of txs) {
    const h = t.hash || t.transaction_hash;
    const m = t.method || t.decoded_input?.method_call || (t.to?.hash || t.to) || '';
    console.log('  https://explore.testnet.tempo.xyz/tx/' + h + '   ' + m);
  }
} catch (e) { console.log('ERR', e?.message || e); }
process.exit(0);
