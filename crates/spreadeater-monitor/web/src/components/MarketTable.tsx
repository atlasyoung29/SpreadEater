import { Link } from "react-router-dom";
import type { MarketSummary } from "../types";
import {
  formatIdentifier,
  formatMoney,
  formatNumber,
  formatTimestamp,
  formatYieldPercent,
} from "../lib/format";
import { PolymarketLink } from "./PolymarketLink";

export function MarketTable({
  rows,
  mode,
  emptyLabel,
}: {
  rows: MarketSummary[];
  mode: "open-orders" | "inventory" | "watchlist" | "preview-orders" | "preview-inventory";
  emptyLabel: string;
}) {
  if (rows.length === 0) {
    return <div className="table-empty">{emptyLabel}</div>;
  }

  const isInventory = mode === "inventory" || mode === "preview-inventory";
  const isPreview = mode.startsWith("preview");

  return (
    <div className="data-table-wrap">
      <table className="data-table">
        <thead>
          <tr>
            <th>market</th>
            {!isInventory ? <th>orders</th> : null}
            {!isInventory ? <th>size</th> : null}
            {isInventory ? <th>inventory</th> : null}
            {isInventory ? <th>open</th> : null}
            <th className="metric-hot">reward/day</th>
            <th className="metric-hot">yield/day</th>
            <th className="metric-hot">edge</th>
            <th>status</th>
            {!isPreview ? <th>latest reason</th> : null}
            <th>updated</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((market) => {
            const sizeExpression = buildSizeExpression(market);
            const rewardYield = formatYieldPercent(
              market.expected_reward_usd_day,
              market.open_order_notional_usd,
            );
            return (
              <tr key={market.condition_id}>
                <td className="market-cell">
                  <Link to={`/markets/${market.condition_id}`} className="market-cell__title">
                    {market.question ?? market.market_slug ?? market.condition_id}
                  </Link>
                  <div className="market-cell__meta">
                    <PolymarketLink marketSlug={market.market_slug} fallbackToSpan>
                      {formatIdentifier(market.condition_id)}
                    </PolymarketLink>
                    {market.market_slug ? <span>{market.market_slug}</span> : null}
                  </div>
                </td>
                {!isInventory ? <td>{market.open_order_count}</td> : null}
                {!isInventory ? <td>{sizeExpression}</td> : null}
                {isInventory ? (
                  <td>
                    YES {formatNumber(market.yes_size)} / NO {formatNumber(market.no_size)}
                    <div className="row-note">
                      net {formatNumber(market.net_exposure)} | sets{" "}
                      {formatNumber(market.complete_sets)}
                    </div>
                  </td>
                ) : null}
                {isInventory ? <td>{market.open_order_count}</td> : null}
                <td className="metric-hot">{formatMoney(market.expected_reward_usd_day)}</td>
                <td className="metric-hot">{rewardYield}</td>
                <td className="metric-hot">
                  <div>{formatMoney(market.expected_edge_usd)}</div>
                  <div className="row-note">{formatNumber(market.expected_edge_pct)}%</div>
                </td>
                <td>
                  <span className={`tone-pill tone-pill--${marketTone(market)}`}>
                    {market.halted ? "halted" : (market.decision_status ?? "watching")}
                  </span>
                </td>
                {!isPreview ? <td>{marketReason(market)}</td> : null}
                <td>{formatTimestamp(market.last_event_at)}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function buildSizeExpression(market: MarketSummary) {
  const shares = Number(market.open_order_share_size);
  const value = Number(market.open_order_notional_usd);
  if (!Number.isFinite(shares) || shares <= 0 || !Number.isFinite(value)) {
    return "n/a";
  }
  const price = value / shares;
  return `${formatMoney(price)} x ${formatNumber(shares)} = ${formatMoney(value)}`;
}

function marketReason(market: MarketSummary) {
  if (market.halted) {
    return market.halt_reason ?? "halted";
  }
  if (market.open_order_count > 0) {
    return "resting orders active";
  }
  if (Math.abs(Number(market.yes_size)) > 0 || Math.abs(Number(market.no_size)) > 0) {
    return "inventory present";
  }
  return market.latest_reason ?? "awaiting recalculation";
}

function marketTone(market: MarketSummary) {
  if (market.halted) {
    return "danger";
  }
  if (market.open_order_count > 0) {
    return "live";
  }
  if (Math.abs(Number(market.yes_size)) > 0 || Math.abs(Number(market.no_size)) > 0) {
    return "inventory";
  }
  return "muted";
}
