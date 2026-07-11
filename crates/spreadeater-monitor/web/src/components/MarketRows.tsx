import { Link } from "react-router-dom";
import {
  formatIdentifier,
  formatMoney,
  formatNumber,
  formatPercent,
  formatSizeExpression,
  formatYieldPercent,
} from "../lib/format";
import { PolymarketLink } from "./PolymarketLink";
import type { MarketSummary } from "../types";

export function MarketRows({
  markets,
  emptyLabel,
  mode,
}: {
  markets: MarketSummary[];
  emptyLabel: string;
  mode: "orders" | "positions" | "watchlist";
}) {
  if (markets.length === 0) {
    return <div className="timeline-empty">{emptyLabel}</div>;
  }

  return (
    <div className="market-list">
      {markets.map((market) => {
        const rewardYield = formatYieldPercent(
          market.expected_reward_usd_day,
          market.open_order_notional_usd,
        );
        const costPerShare = costPerShareForMarket(market);

        return (
          <article key={market.condition_id} className="market-row">
            <div className="market-row__identity">
              <span className="market-row__status">{market.decision_status ?? "unknown"}</span>
              <h3>
                <Link to={`/markets/${market.condition_id}`}>
                  {market.question ?? market.market_slug ?? market.condition_id}
                </Link>
              </h3>
              <div className="market-row__subline">
                <PolymarketLink marketSlug={market.market_slug} fallbackToSpan>
                  {formatIdentifier(market.condition_id)}
                </PolymarketLink>
              </div>
              {mode === "watchlist" ? (
                <p className="market-row__reason">{marketReason(market)}</p>
              ) : null}
            </div>
            <dl className="market-row__metrics">
              {mode !== "positions" ? (
                <>
                  <div>
                    <dt>orders</dt>
                    <dd>{market.open_order_count}</dd>
                  </div>
                  <div>
                    <dt>size</dt>
                    <dd>
                      {formatSizeExpression(
                        costPerShare,
                        market.open_order_share_size,
                        market.open_order_notional_usd,
                      )}
                    </dd>
                  </div>
                  <div className="metric-emphasis metric-emphasis--reward">
                    <dt>reward / day</dt>
                    <dd className="metric-stack">
                      <span>{formatMoney(market.expected_reward_usd_day)}</span>
                      <span className="metric-stack__sub">yield/day {rewardYield}</span>
                    </dd>
                  </div>
                </>
              ) : null}
              {mode !== "orders" ? (
                <>
                  <div>
                    <dt>yes / no</dt>
                    <dd>
                      {formatNumber(market.yes_size)} / {formatNumber(market.no_size)}
                    </dd>
                  </div>
                  <div>
                    <dt>net</dt>
                    <dd>{formatNumber(market.net_exposure)}</dd>
                  </div>
                </>
              ) : null}
              <div className="metric-emphasis metric-emphasis--edge">
                <dt>edge</dt>
                <dd>{formatMoney(market.expected_edge_usd)}</dd>
              </div>
              <div className="metric-emphasis metric-emphasis--edge">
                <dt>edge %</dt>
                <dd>{formatPercent(market.expected_edge_pct)}</dd>
              </div>
            </dl>
          </article>
        );
      })}
    </div>
  );
}

function marketReason(market: MarketSummary) {
  if (market.open_order_count > 0) {
    return "Resting order live.";
  }
  if (Math.abs(Number(market.yes_size)) > 0 || Math.abs(Number(market.no_size)) > 0) {
    return "Inventory present.";
  }
  return market.latest_reason ?? "Awaiting next recalculation.";
}

function costPerShareForMarket(market: MarketSummary) {
  const shares = Number(market.open_order_share_size);
  const value = Number(market.open_order_notional_usd);
  if (!Number.isFinite(shares) || !Number.isFinite(value) || shares <= 0) {
    return null;
  }
  return value / shares;
}
