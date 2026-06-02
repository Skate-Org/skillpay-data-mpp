/**
 * Entry point — the paid WebSocket alpha stream.
 *
 * Pipeline:  Databento sidecar (NDJSON over TCP) → SidecarSource → WS gateway
 *            → MPP-paid subscribers.
 *
 * Wires the producer + MPP payment rail + WS gateway onto one http.Server and
 * serves the HFT dashboard (public/). The only data source in this repo is the
 * Databento sidecar (SIDECAR_HOST:SIDECAR_PORT).
 */
import express from 'express';
import { createServer } from 'http';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { existsSync } from 'fs';

import { CONFIG, STREAM_SKILLS } from './config.js';
import { SidecarSource } from './sidecar-source.js';
import { WsGateway } from './ws-gateway.js';
import { mountMpp } from './mpp.js';

const __dirname = dirname(fileURLToPath(import.meta.url));
const PUBLIC_DIR = join(__dirname, '..', 'public');

const app = express();
app.disable('x-powered-by');

// Brand logo resolver — serves public/skate_logo.<ext> for any /skate[_-]logo.*
// request (the dashboard references /skate_logo.png; the file may be any format).
const LOGO_NAMES = ['skate_logo', 'skate-logo'];
const LOGO_EXTS = ['png', 'jpg', 'jpeg', 'webp', 'svg', 'avif'];
app.get(/^\/skate[_-]logo\.[a-z0-9]+$/i, (_req, res) => {
  for (const n of LOGO_NAMES)
    for (const e of LOGO_EXTS) {
      const p = join(PUBLIC_DIR, `${n}.${e}`);
      if (existsSync(p)) return res.sendFile(p);
    }
  res.status(404).end();
});

// Static HFT dashboard. GET / serves public/index.html. HTML is sent with
// no-cache so dashboard updates always reach the browser (assets can cache).
app.use(
  express.static(PUBLIC_DIR, {
    setHeaders: (res, p) => {
      if (p.endsWith('.html')) res.setHeader('Cache-Control', 'no-cache, must-revalidate');
    },
  })
);

const source = new SidecarSource();
// Single payment rail: the real Tempo MPP payment-channel (/mpp/session).
// Network (mainnet/testnet) is selected by the MPP_TESTNET env toggle (see mpp.ts).
const mpp = mountMpp(app);

// ── REST: health + skill catalog ──────────────────────────────────────────────

app.get('/health', (_req, res) => res.json({ status: 'ok', mpp: mpp.enabled, network: CONFIG.chainLabel }));

app.get('/skills', (_req, res) => {
  res.json(
    STREAM_SKILLS.map((s) => ({
      symbol: s.symbol,
      name: s.name,
      pricePerMinuteUsd: s.pricePerMinuteUsd,
      channels: s.channels,
      live: source.latest(s.symbol)?.health ?? 'unknown',
      payment: { rail: 'tempo-mpp', network: CONFIG.chainLabel, sessionRoute: '/mpp/session', recipient: mpp.recipient ?? null },
    }))
  );
});

// ── HTTP server + WS upgrade ─────────────────────────────────────────────────

const httpServer = createServer(app);
const gateway = new WsGateway(source); // gated only by the Tempo MPP stream token
gateway.attach(httpServer);

source.start();

httpServer.listen(CONFIG.port, () => {
  console.log(`[server] crude-alpha-stream listening on :${CONFIG.port}`);
  console.log(`[server] skills:    ${STREAM_SKILLS.map((s) => s.symbol).join(', ')}`);
  console.log(`[server] source:    sidecar ${process.env.SIDECAR_HOST ?? '127.0.0.1'}:${process.env.SIDECAR_PORT ?? 5051}`);
  console.log(`[server] payment:   Tempo MPP (${CONFIG.chainLabel}) — pay at POST /mpp/session?symbol=:symbol`);
  console.log(`[server] stream:    WS  /ws/stream/:symbol?token=<from /mpp/session>`);
});

const shutdown = () => {
  console.log('[server] shutting down');
  source.stop();
  httpServer.close(() => process.exit(0));
};
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
