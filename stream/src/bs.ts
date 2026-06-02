/**
 * Black-76 analytics for options on futures (interest rate ≈ 0, which is fine
 * for short-dated relative skew). Used to turn the sidecar's option BBOs into
 * implied vols, the 25-delta risk reversal, and ATM IV.
 */

/** Standard normal CDF (Abramowitz & Stegun 7.1.26). */
export function normCdf(x: number): number {
  const t = 1 / (1 + 0.2316419 * Math.abs(x));
  const d = 0.3989422804014327 * Math.exp((-x * x) / 2);
  const p =
    d * t * (0.319381530 + t * (-0.356563782 + t * (1.781477937 + t * (-1.821255978 + t * 1.330274429))));
  return x >= 0 ? 1 - p : p;
}

/** Black-76 option price (r = 0). */
export function black76(F: number, K: number, T: number, sigma: number, isCall: boolean): number {
  if (T <= 0 || sigma <= 0) return Math.max(0, isCall ? F - K : K - F);
  const sqrtT = Math.sqrt(T);
  const d1 = (Math.log(F / K) + 0.5 * sigma * sigma * T) / (sigma * sqrtT);
  const d2 = d1 - sigma * sqrtT;
  return isCall ? F * normCdf(d1) - K * normCdf(d2) : K * normCdf(-d2) - F * normCdf(-d1);
}

/** Implied vol via bisection. Returns null if the price is unsolvable
 * (below intrinsic / outside [1e-3, 5.0] vol). */
export function impliedVol(
  price: number,
  F: number,
  K: number,
  T: number,
  isCall: boolean
): number | null {
  if (price <= 0 || F <= 0 || K <= 0 || T <= 0) return null;
  const intrinsic = Math.max(0, isCall ? F - K : K - F);
  if (price < intrinsic - 1e-6) return null;
  let lo = 1e-3;
  let hi = 5.0;
  const plo = black76(F, K, T, lo, isCall) - price;
  const phi = black76(F, K, T, hi, isCall) - price;
  if (plo > 0 || phi < 0) return null; // price outside model range
  for (let i = 0; i < 64; i++) {
    const mid = 0.5 * (lo + hi);
    const pm = black76(F, K, T, mid, isCall) - price;
    if (Math.abs(pm) < 1e-7) return mid;
    if (pm > 0) hi = mid;
    else lo = mid;
  }
  return 0.5 * (lo + hi);
}

/** Black-76 delta (r = 0). Call ∈ (0,1), put ∈ (-1,0). */
export function delta(F: number, K: number, T: number, sigma: number, isCall: boolean): number {
  if (T <= 0 || sigma <= 0) return isCall ? 1 : -1;
  const d1 = (Math.log(F / K) + 0.5 * sigma * sigma * T) / (sigma * Math.sqrt(T));
  return isCall ? normCdf(d1) : normCdf(d1) - 1;
}

/** Linear-interpolate y at target x over points sorted ascending by x.
 * Clamps to the endpoints if target is outside the range. null if <1 point. */
export function interp(points: Array<{ x: number; y: number }>, xt: number): number | null {
  if (points.length === 0) return null;
  const p = [...points].sort((a, b) => a.x - b.x);
  if (xt <= p[0]!.x) return p[0]!.y;
  if (xt >= p[p.length - 1]!.x) return p[p.length - 1]!.y;
  for (let i = 1; i < p.length; i++) {
    if (xt <= p[i]!.x) {
      const a = p[i - 1]!;
      const b = p[i]!;
      const w = (xt - a.x) / (b.x - a.x);
      return a.y + w * (b.y - a.y);
    }
  }
  return p[p.length - 1]!.y;
}

/** Like interp but returns null if the target is OUTSIDE the data range (no
 * extrapolation/clamping). Use for 25Δ extraction so an illiquid wing can't be
 * clamped into a garbage risk-reversal. Requires the target to be bracketed. */
export function interpStrict(points: Array<{ x: number; y: number }>, xt: number): number | null {
  if (points.length < 2) return null;
  const p = [...points].sort((a, b) => a.x - b.x);
  if (xt < p[0]!.x || xt > p[p.length - 1]!.x) return null;
  return interp(p, xt);
}
