import { Link } from "react-router-dom";
import {
  formatIdentifier,
  formatMoney,
  formatNumber,
  formatSizeExpression,
  formatYieldPercent,
  multiplyPayloadNumbers,
  readPayloadNumber,
  readPayloadText,
} from "../lib/format";
import { PolymarketLink } from "./PolymarketLink";
import type { EventListItem, MarketSummary } from "../types";

export function ActivityList({
  items,
  marketsByCondition,
  kind,
  emptyLabel,
}: {
  items: EventListItem[];
  marketsByCondition: Map<string, MarketSummary>;
  kind: "orders" | "fills";
  emptyLabel: string;
}) {
  if (items.length === 0) {
    return <div className="timeline-empty">{emptyLabel}</div>;
  }

  return (
    <div className="activity-list">
      {items.map((item) => {
        const market = item.condition_id
          ? marketsByCondition.get(item.condition_id)
          : undefined;
        const marketSlug = market?.market_slug ?? item.market_slug;
        const sizeKey = kind === "orders" ? "size" : "fill_size";
        const priceKey = kind === "orders" ? "price" : "fill_price";
        const shareSize = readPayloadNumber(item.payload[sizeKey]);
        const price = readPayloadNumber(item.payload[priceKey]);
        const value = multiplyPayloadNumbers(item.payload[priceKey], item.payload[sizeKey]);
        const rewardPerDay = kind === "orders" ? market?.expected_reward_usd_day : null;
        const yieldPerDay =
          kind === "orders" ? formatYieldPercent(rewardPerDay, value) : null;
        const orderStatus = kind === "orders" ? describeOrderStatus(item) : null;

        return (
          <article key={item.id} className="activity-row">
            <div className="activity-row__meta">
              <span>{new Date(item.occurred_at).toLocaleTimeString()}</span>
              <span>{kind === "orders" ? "order" : "fill"}</span>
            </div>
            <div className="activity-row__body">
              <div className="activity-row__headline">
                <div className="activity-row__headline-main">
                  <Link to={item.condition_id ? `/markets/${item.condition_id}` : "/"}>
                    {item.question ?? item.market_slug ?? item.condition_id ?? "market"}
                  </Link>
                  <div className="activity-row__subline">
                    <PolymarketLink marketSlug={marketSlug}>Polymarket</PolymarketLink>
                    {item.order_id ? (
                      <span className="activity-row__mono">
                        order {formatIdentifier(item.order_id, 6, 4)}
                      </span>
                    ) : null}
                  </div>
                </div>
                {orderStatus ? (
                  <div className="activity-row__status">
                    <span className={`state-badge state-badge--${orderStatus.tone}`}>
                      {orderStatus.label}
                    </span>
                    {orderStatus.note ? (
                      <span className="activity-row__status-note">{orderStatus.note}</span>
                    ) : null}
                  </div>
                ) : null}
              </div>
              <dl className="activity-row__metrics">
                <div>
                  <dt>size</dt>
                  <dd>{formatSizeExpression(price, shareSize, value)}</dd>
                </div>
                <div>
                  <dt>{kind === "orders" ? "leg" : "outcome"}</dt>
                  <dd>
                    {readPayloadText(item.payload[kind === "orders" ? "leg" : "outcome"])}
                  </dd>
                </div>
                <div>
                  <dt>side</dt>
                  <dd>
                    {readPayloadText(item.payload.side)}
                  </dd>
                </div>
                {kind === "orders" ? (
                  <div className="metric-emphasis metric-emphasis--reward">
                    <dt>reward / day</dt>
                    <dd className="metric-stack">
                      <span>{formatMoney(rewardPerDay)}</span>
                      <span className="metric-stack__sub">yield/day {yieldPerDay}</span>
                    </dd>
                  </div>
                ) : null}
              </dl>
            </div>
          </article>
        );
      })}
    </div>
  );
}

function describeOrderStatus(item: EventListItem) {
  switch (item.order_state) {
    case "open":
      return { label: "active", tone: "active", note: "resting on book" };
    case "submitted":
      return { label: "submitted", tone: "active", note: "awaiting open acknowledgement" };
    case "partially_filled": {
      const filledText =
        item.order_matched_size !== null && item.order_size !== null
          ? `${formatNumber(item.order_matched_size)} / ${formatNumber(item.order_size)} filled`
          : "partially filled";
      return { label: "active partial", tone: "active", note: filledText };
    }
    case "filled":
      return { label: "filled", tone: "filled", note: "no longer active" };
    case "cancelled":
      return {
        label: "cancelled",
        tone: "cancelled",
        note: humanizeReason(item.order_cancel_reason ?? item.reason_code),
      };
    case "replaced":
      return {
        label: "replaced",
        tone: "replaced",
        note: item.replacement_order_id
          ? `rolled to ${formatIdentifier(item.replacement_order_id, 6, 4)}`
          : humanizeReason(item.order_cancel_reason ?? item.reason_code),
      };
    default:
      return { label: "unknown", tone: "muted", note: null };
  }
}

function humanizeReason(reason: string | null | undefined) {
  if (!reason) {
    return "no reason recorded";
  }
  return reason
    .replace(/_/g, " ")
    .replace(/([a-z])([A-Z])/g, "$1 $2")
    .toLowerCase();
}
