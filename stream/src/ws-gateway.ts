/**
 * WS gateway — adapted from skate-skillpay's WsProxyService.
 *
 * The MPP handshake (parse Authorization: Payment, verify ERC-3009, wait
 * settlement, derive session minutes) is ported almost verbatim. The ONE
 * structural change: the repo's `pipeToUpstream` (connect client↔external WS)
 * becomes `pipeToAlphaStream` — we are the producer, so we fan our own
 * AlphaSnapshots out to the paying client instead of piping a third-party feed.
 */
import type { IncomingMessage, Server } from 'http';
import type { Socket } from 'net';
import WebSocket, { WebSocketServer } from 'ws';

import { CONFIG, MIN_SESSION_MS, STREAM_SKILLS_MAP } from './config.js';
import { verifyStreamToken } from './stream-token.js';
import type { AlphaSource, ClientMessage, ServerMessage } from './types.js';

export class WsGateway {
  private readonly wss = new WebSocketServer({ noServer: true });

  constructor(private readonly source: AlphaSource) {}

  /** Attach the upgrade handler to an existing http.Server. */
  attach(httpServer: Server): void {
    httpServer.on('upgrade', (req, socket, head) => {
      this.handleUpgrade(req, socket as Socket, head).catch((err) => {
        console.error('[gateway] upgrade error:', (err as Error).message);
        this.reject(socket as Socket, 500, 'Internal Server Error');
      });
    });
    console.log('[gateway] WS upgrade handler registered (route: /ws/stream/:symbol)');
  }

  private async handleUpgrade(
    req: IncomingMessage,
    socket: Socket,
    head: Buffer
  ): Promise<void> {
    const url = new URL(req.url ?? '', 'http://localhost');
    const match = /\/ws\/stream\/([^/?]+)/.exec(url.pathname);
    if (!match) return this.reject(socket, 404, 'Not Found');
    const symbol = decodeURIComponent(match[1]);
    const skill = STREAM_SKILLS_MAP.get(symbol);
    if (!skill) return this.reject(socket, 404, 'Unknown symbol');

    // Auth: accept EITHER a paid MPP stream token OR an allowlisted API key.
    //  - token: minted by the Tempo MPP session route. The subscriber pays one
    //    voucher to POST /mpp/session?symbol=:symbol (on-chain on Tempo, mainnet
    //    or testnet per the MPP_TESTNET toggle), receives a token, and opens the
    //    WS with ?token=<t> (browsers) or `Authorization: Bearer <t>` (node).
    //    The session = the token's remaining (paid-for) minutes.
    //  - apiKey: a static allowlisted key (CONFIG.apiKeys) for unmetered access.
    //    Sent as ?apiKey=<k> (browsers) or `X-API-Key: <k>` (node). Its session
    //    runs for CONFIG.apiKeySessionMinutes regardless of payment.
    const authHeader = req.headers.authorization;
    const headerToken = authHeader?.startsWith('Bearer ') ? authHeader.slice('Bearer '.length) : undefined;
    const token = url.searchParams.get('token') ?? headerToken;
    const apiKey = url.searchParams.get('apiKey') ?? (req.headers['x-api-key'] as string | undefined);

    let sessionDurationMs: number;
    let reference: string;

    if (apiKey && CONFIG.apiKeys.includes(apiKey)) {
      sessionDurationMs = Math.max(MIN_SESSION_MS, CONFIG.apiKeySessionMinutes * 60_000);
      reference = `apikey:${symbol}`;
      console.log(`[gateway] API-key session start sym=${symbol} ms=${sessionDurationMs}`);
    } else if (token) {
      const tv = verifyStreamToken(token, symbol);
      if (!tv.ok) return this.reject(socket, 402, 'Invalid or expired stream token — pay at /mpp/session');
      sessionDurationMs = Math.max(MIN_SESSION_MS, (tv.expiresAt ?? Date.now()) - Date.now());
      reference = `mpp:${symbol}`;
      console.log(`[gateway] MPP session start sym=${symbol} ms=${sessionDurationMs}`);
    } else {
      // No credential at all → 402 with the pay route (an API key is the
      // alternative for callers who are not paying per-minute).
      socket.write(
        'HTTP/1.1 402 Payment Required\r\n' +
          'Content-Type: application/json\r\n' +
          `X-MPP-Session-Route: /mpp/session?symbol=${symbol}\r\n\r\n`
      );
      socket.destroy();
      return;
    }

    this.wss.handleUpgrade(req, socket, head, (clientWs) => {
      this.pipeToAlphaStream(clientWs, symbol, sessionDurationMs, reference);
    });
  }

  /**
   * Replaces the repo's pipeToUpstream. Fan the alpha producer out to the paid
   * client. Two per-connection sets govern delivery:
   *   - `entitled`: symbols the client has PAID for (starts as the URL symbol;
   *     grows on an in-band `authorization` top-up for another symbol).
   *   - `active`:   subset of `entitled` the client is currently subscribed to.
   * A snapshot is delivered only when `snap.sym ∈ active`. `subscribe` /
   * `unsubscribe` mutate `active` (guarded by `entitled`); both are acked with a
   * `subscription` frame echoing the resulting sets so the client can verify.
   * Session timer warns (payment-need-voucher) then closes when the pre-paid
   * window expires — the in-band top-up path (design doc §5).
   */
  private pipeToAlphaStream(
    clientWs: WebSocket,
    symbol: string,
    sessionDurationMs: number,
    reference: string,
    txHash?: string
  ): void {
    const send = (m: ServerMessage) => {
      if (clientWs.readyState === WebSocket.OPEN) clientWs.send(JSON.stringify(m));
    };

    // entitlement = what was paid for; active = what is being streamed right now.
    const entitled = new Set<string>([symbol]);
    const active = new Set<string>([symbol]);
    const ackSubscription = () =>
      send({ t: 'subscription', active: [...active], entitled: [...entitled] });

    const expiresAt = Date.now() + sessionDurationMs;
    send({ t: 'payment-receipt', reference, expiresAt });
    ackSubscription();

    // snapshot on connect for each active symbol
    for (const s of active) {
      const first = this.source.latest(s);
      if (first) send({ t: 'snapshot', data: first });
    }

    // live fanout — gated on the active set (mutated by subscribe/unsubscribe)
    const unsub = this.source.subscribe((snap) => {
      if (active.has(snap.sym)) send({ t: 'snapshot', data: snap });
    });

    // warn ~10s before expiry so the client can top up in-band; re-armable so an
    // in-band `authorization` (fresh MPP token) extends the live session.
    let warnTimer: ReturnType<typeof setTimeout>;
    let expiryTimer: ReturnType<typeof setTimeout>;
    const rearm = (expiresAt: number) => {
      clearTimeout(warnTimer);
      clearTimeout(expiryTimer);
      const ms = Math.max(0, expiresAt - Date.now());
      warnTimer = setTimeout(
        () => send({ t: 'payment-need-voucher', reason: 'session window ending; send authorization to extend' }),
        Math.max(0, ms - 10_000)
      );
      expiryTimer = setTimeout(() => {
        console.log(`[gateway] session expired sym=${symbol}`);
        clientWs.close(1000, 'Session expired');
      }, ms);
    };
    rearm(Date.now() + sessionDurationMs);

    const cleanup = () => {
      clearTimeout(warnTimer);
      clearTimeout(expiryTimer);
      unsub();
    };

    // client → server: subscribe / unsubscribe + in-band voucher top-up
    clientWs.on('message', (raw) => {
      let msg: ClientMessage;
      try {
        msg = JSON.parse(raw.toString()) as ClientMessage;
      } catch {
        return send({ t: 'error', code: 'bad_message', message: 'invalid JSON' });
      }

      if (msg.t === 'subscribe') {
        const reqs = Array.isArray(msg.symbols) ? msg.symbols : [];
        const denied = reqs.filter((s) => !entitled.has(s));
        if (denied.length) {
          send({ t: 'error', code: 'not_entitled', message: `not paid for: ${denied.join(', ')}` });
        }
        for (const s of reqs) {
          if (entitled.has(s)) {
            const wasActive = active.has(s);
            active.add(s);
            if (!wasActive) {
              const snap = this.source.latest(s); // snapshot-on-(re)subscribe
              if (snap) send({ t: 'snapshot', data: snap });
            }
          }
        }
        return ackSubscription();
      }

      if (msg.t === 'unsubscribe') {
        const reqs = Array.isArray(msg.symbols) ? msg.symbols : [];
        for (const s of reqs) active.delete(s);
        return ackSubscription();
      }

      if (msg.t === 'authorization') {
        // in-band top-up: present a FRESH stream token (from another paid
        // /mpp/session voucher on the same channel) to extend the session.
        const fresh = (msg as any).token as string | undefined;
        const tv = fresh ? verifyStreamToken(fresh, symbol) : { ok: false as const };
        if (!tv.ok || !tv.expiresAt) {
          return send({ t: 'error', code: 'topup_failed', message: 'invalid or expired token' });
        }
        rearm(tv.expiresAt);
        send({ t: 'payment-receipt', reference: `mpp:${symbol}`, expiresAt: tv.expiresAt });
        console.log(`[gateway] session extended sym=${symbol} until ${new Date(tv.expiresAt).toISOString()}`);
        return;
      }

      send({ t: 'error', code: 'unknown_message', message: `unhandled type` });
    });

    clientWs.on('close', (code) => {
      console.log(`[gateway] client closed sym=${symbol} code=${code}`);
      cleanup();
    });
    clientWs.on('error', (err) => {
      console.error(`[gateway] client error sym=${symbol}: ${err.message}`);
      cleanup();
    });
  }

  private reject(socket: Socket, code: number, message: string): void {
    socket.write(
      `HTTP/1.1 ${code} ${message}\r\nContent-Type: application/json\r\n\r\n` +
        JSON.stringify({ error: message })
    );
    socket.destroy();
  }
}
