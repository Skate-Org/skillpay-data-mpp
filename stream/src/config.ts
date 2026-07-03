/**
 * Single source of truth for the alpha-stream skills (mirrors the repo's
 * skills.config.ts pattern). Each entry defines a symbol, its per-minute price,
 * and which channels it exposes.
 */
import type { StreamSkillConfig } from './types.js';

export const STREAM_SKILLS: StreamSkillConfig[] = [
  {
    symbol: 'CL',
    name: 'WTI Crude Alpha (CME options × HL microstructure)',
    pricePerMinuteUsd: 0.001, // $0.001/min — Tempo mainnet
    channels: ['oil.options', 'oil.structure', 'oil.micro', 'regime'],
  },
  {
    symbol: 'BRENT',
    name: 'Brent Crude Alpha (CME options × HL microstructure)',
    pricePerMinuteUsd: 0.001,
    channels: ['oil.options', 'oil.structure', 'oil.micro', 'regime'],
  },
  {
    symbol: 'GOLD',
    name: 'Gold Alpha (COMEX options × HL microstructure)',
    pricePerMinuteUsd: 0.001,
    channels: ['metals.options', 'metals.structure', 'metals.micro', 'regime'],
  },
  {
    symbol: 'SILVER',
    name: 'Silver Alpha (COMEX options × HL microstructure)',
    pricePerMinuteUsd: 0.001,
    channels: ['metals.options', 'metals.structure', 'metals.micro', 'regime'],
  },
];

export const STREAM_SKILLS_MAP = new Map<string, StreamSkillConfig>(
  STREAM_SKILLS.map((s) => [s.symbol, s])
);

// ── env / wiring ──────────────────────────────────────────────────────────
export const CONFIG = {
  port: Number(process.env.PORT ?? 7070),
  // Where the alpha producer sources data. Default: the running oil1 dashboard.
  oil1DashboardUrl: process.env.OIL1_DASHBOARD_URL ?? 'http://localhost:5000/api/state',
  producePollMs: Number(process.env.PRODUCE_POLL_MS ?? 1000),
  // ── Payment: Tempo MPP only (rail in mpp.ts). ONE toggle picks the network. ──
  // MPP_TESTNET=true → Tempo testnet (Moderato); default/false → Tempo mainnet.
  isTestnet: process.env.MPP_TESTNET === 'true',
  chainLabel: process.env.MPP_TESTNET === 'true' ? 'Tempo Testnet' : 'Tempo Mainnet',
  // ── API-key access (alternative to a paid MPP stream token) ────────────────
  // A client may open the WS with ?apiKey=<k> instead of a paid ?token=. Keys
  // are a static allowlist from env (comma/space separated). API-key sessions
  // are NOT payment-metered; each runs for STREAM_API_KEY_SESSION_MINUTES.
  apiKeys: (process.env.STREAM_API_KEYS ?? '')
    .split(/[\s,]+/)
    .map((k) => k.trim())
    .filter(Boolean),
  apiKeySessionMinutes: Number(process.env.STREAM_API_KEY_SESSION_MINUTES ?? 1440),
};

export const MIN_SESSION_MS = 60_000;
