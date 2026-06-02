/**
 * Short-lived stream access tokens that bridge an MPP payment → WS data access.
 *
 * Flow: a subscriber pays one voucher to /mpp/session (real on-chain on Tempo
 * mainnet). On success the server mints a token good for N minutes (= what was
 * paid at $0.001/min). The WS gateway accepts ?token=<t> and grants a session of
 * the remaining time. Pay-as-you-go: re-POST /mpp/session each minute to extend.
 *
 * Token = `<symbol>.<expiresAtMs>.<hmacHex>` signed with MPP_SECRET_KEY (HMAC-
 * SHA256). Stateless, so any process that shares the secret can verify it.
 */
import { createHmac, timingSafeEqual } from 'node:crypto';

const SECRET = () => process.env.MPP_SECRET_KEY ?? 'crude-alpha-stream-dev-secret';

function sign(payload: string): string {
  return createHmac('sha256', SECRET()).update(payload).digest('hex');
}

export function mintStreamToken(symbol: string, minutes: number): string {
  const exp = Date.now() + Math.max(1, minutes) * 60_000;
  const payload = `${symbol}.${exp}`;
  return `${payload}.${sign(payload)}`;
}

export function verifyStreamToken(token: string, symbol: string): { ok: boolean; expiresAt?: number } {
  const parts = token.split('.');
  if (parts.length !== 3) return { ok: false };
  const [sym, expStr, mac] = parts;
  if (sym !== symbol) return { ok: false };
  const exp = Number(expStr);
  if (!Number.isFinite(exp) || exp <= Date.now()) return { ok: false };
  const expected = sign(`${sym}.${expStr}`);
  const a = Buffer.from(mac, 'hex');
  const b = Buffer.from(expected, 'hex');
  if (a.length !== b.length || !timingSafeEqual(a, b)) return { ok: false };
  return { ok: true, expiresAt: exp };
}
