import { Link } from "react-router-dom";
import { formatMoney, formatNumber } from "../lib/format";
import { PolymarketLink } from "./PolymarketLink";
import type { EventListItem } from "../types";

interface EventTimelineProps {
  items: EventListItem[];
}

export function EventTimeline({ items }: EventTimelineProps) {
  if (items.length === 0) {
    return <div className="timeline-empty">No timeline items yet.</div>;
  }

  return (
    <div className="timeline">
      {items.map((item) => (
        <article key={item.id} className={`timeline-row timeline-row--${item.priority}`}>
          <div className="timeline-row__meta">
            <span>{new Date(item.occurred_at).toLocaleTimeString()}</span>
            <span>{item.event_type}</span>
          </div>
          <div className="timeline-row__content">
            <p>{item.question ?? item.market_slug ?? item.condition_id ?? "event"}</p>
            <strong className="timeline-row__summary">{describeEvent(item)}</strong>
            <div className="timeline-row__links">
              {item.trace_id ? <Link to={`/traces/${item.trace_id}`}>trace</Link> : null}
              {item.condition_id ? <Link to={`/markets/${item.condition_id}`}>market</Link> : null}
              <PolymarketLink marketSlug={item.market_slug}>polymarket</PolymarketLink>
              {item.reason_code ? <span>{item.reason_code}</span> : null}
            </div>
          </div>
        </article>
      ))}
    </div>
  );
}

function describeEvent(item: EventListItem) {
  switch (item.event_type) {
    case "order_submitted":
      return [
        readPayload(item, "side"),
        readPayload(item, "leg"),
        formatNumber(readPayload(item, "size")),
        "@",
        formatMoney(readPayload(item, "price")),
      ].join(" ");
    case "fill_detected":
      return [
        readPayload(item, "outcome"),
        "fill",
        formatNumber(readPayload(item, "fill_size")),
        "@",
        formatMoney(readPayload(item, "fill_price")),
      ].join(" ");
    default:
      return item.reason_code ?? item.order_id ?? item.trace_id ?? "event";
  }
}

function readPayload(item: EventListItem, key: string) {
  const value = item.payload[key];
  if (typeof value === "string") {
    return value;
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  return "n/a";
}
