import { useState } from "react";
import { HistoryTable } from "../components/HistoryTable";
import { PaginationControls } from "../components/PaginationControls";
import { fetchHistory } from "../lib/api";
import { usePagedResource } from "../lib/usePagedResource";

export function HistoryPage() {
  const [q, setQ] = useState("");
  const [category, setCategory] = useState("");
  const [eventType, setEventType] = useState("");
  const [priority, setPriority] = useState("");
  const [runId, setRunId] = useState("");
  const [traceId, setTraceId] = useState("");
  const [conditionId, setConditionId] = useState("");
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(100);

  const query = {
    q,
    category,
    event_type: eventType,
    priority,
    run_id: runId,
    trace_id: traceId,
    condition_id: conditionId,
    page,
    page_size: pageSize,
  };

  const resource = usePagedResource({
    queryKey: JSON.stringify(query),
    autoRefresh: page === 1,
    loader: () => fetchHistory(query),
  });

  return (
    <section className="page page--ops">
      <section className="section-card section-card--dense">
        <div className="section-card__header">
          <div>
            <p className="eyebrow">history</p>
            <h1 className="section-card__title">Unified audit trail</h1>
          </div>
          <div className="section-card__meta">
            <span>newest rows first</span>
            {page > 1 ? (
              <button type="button" className="inline-link inline-link--button" onClick={resource.refresh}>
                refresh this page
              </button>
            ) : null}
          </div>
        </div>

        <div className="toolbar toolbar--sticky">
          <label className="toolbar-field toolbar-field--search">
            <span>search</span>
            <input
              value={q}
              onChange={(event) => {
                setPage(1);
                setQ(event.target.value);
              }}
              placeholder="market, condition, order id, trace id, or reason"
            />
          </label>
          <label className="toolbar-field">
            <span>category</span>
            <select value={category} onChange={(event) => { setPage(1); setCategory(event.target.value); }}>
              <option value="">all</option>
              <option value="orders">orders</option>
              <option value="fills">fills</option>
              <option value="hedges">hedges</option>
              <option value="decisions">decisions</option>
              <option value="alerts">alerts</option>
            </select>
          </label>
          <label className="toolbar-field">
            <span>event</span>
            <input
              value={eventType}
              onChange={(event) => {
                setPage(1);
                setEventType(event.target.value);
              }}
              placeholder="order_submitted"
            />
          </label>
          <label className="toolbar-field">
            <span>priority</span>
            <select value={priority} onChange={(event) => { setPage(1); setPriority(event.target.value); }}>
              <option value="">all</option>
              <option value="critical">critical</option>
              <option value="high">high</option>
              <option value="normal">normal</option>
              <option value="low">low</option>
            </select>
          </label>
          <label className="toolbar-field">
            <span>run</span>
            <input
              value={runId}
              onChange={(event) => {
                setPage(1);
                setRunId(event.target.value);
              }}
              placeholder="run id"
            />
          </label>
          <label className="toolbar-field">
            <span>trace</span>
            <input
              value={traceId}
              onChange={(event) => {
                setPage(1);
                setTraceId(event.target.value);
              }}
              placeholder="trace id"
            />
          </label>
          <label className="toolbar-field">
            <span>market</span>
            <input
              value={conditionId}
              onChange={(event) => {
                setPage(1);
                setConditionId(event.target.value);
              }}
              placeholder="condition id"
            />
          </label>
        </div>

        {resource.error ? <div className="error-banner">{resource.error}</div> : null}
        {resource.loading && !resource.data ? <div className="table-empty">Loading history...</div> : null}

        {resource.data ? (
          <>
            <PaginationControls
              page={page}
              pageSize={pageSize}
              total={resource.data.total}
              onPageChange={setPage}
              onPageSizeChange={(size) => {
                setPage(1);
                setPageSize(size);
              }}
            />
            <HistoryTable rows={resource.data.items} emptyLabel="No history rows matched the current filters." />
          </>
        ) : null}
      </section>
    </section>
  );
}
