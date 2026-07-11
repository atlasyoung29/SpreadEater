export function fallbackPolymarketUrl(marketSlug: string | null | undefined) {
  if (!marketSlug) {
    return null;
  }
  return `https://polymarket.com/event/${marketSlug}`;
}
