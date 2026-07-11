import { useState } from "react";
import { MarketTable } from "../components/MarketTable";
import { PaginationControls } from "../components/PaginationControls";
import { fetchOpenOrders } from "../lib/api";
import { usePagedResource } from "../lib/usePagedResource";

export function OpenOrdersPage() {
  const [q, setQ] = useState("");
  const [status, setStatus] = useState("");
  const [side, setSide] = useState("");
  const [role, setRole] = useState("");
  const [halted, setHalted] = useState("");
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(100);

  const query = { q, status, side, role, halted, page, page_size: pageSize };
  const resource = usePagedResource({
    queryKey: JSON.stringify(query),
    autoRefresh: page === 1,
    loader: () => fetchOpenOrders(query),
  });

  return (
    <section className="page page--ops">
      <section className="section-card section-card--dense">
        <div className="section-card__header">
          <div>
            <p className="eyebrow">open orders</p>
            <h1 className="section-card__title">Grouped live orders</h1>
          </div>
          <div className="section-card__meta">
            <span>page 1 auto-refreshes</span>
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
              placeholder="market name, slug, or condition id"
            />
          </label>
          <label className="toolbar-field">
            <span>status</span>
            <select value={status} onChange={(event) => { setPage(1); setStatus(event.target.value); }}>
              <option value="">all</option>
              <option value="active">active</option>
              <option value="open">open</option>
              <option value="partial">partial</option>
              <option value="submitted">submitted</option>
            </select>
          </label>
          <label className="toolbar-field">
            <span>side</span>
            <select value={side} onChange={(event) => { setPage(1); setSide(event.target.value); }}>
              <option value="">all</option>
              <option value="buy">buy</option>
              <option value="sell">sell</option>
            </select>
          </label>
          <label className="toolbar-field">
            <span>role</span>
            <select value={role} onChange={(event) => { setPage(1); setRole(event.target.value); }}>
              <option value="">all</option>
              <option value="bid_entry">bid entry</option>
              <option value="ask_inventory">inventory ask</option>
            </select>
          </label>
          <label className="toolbar-field">
            <span>halted</span>
            <select value={halted} onChange={(event) => { setPage(1); setHalted(event.target.value); }}>
              <option value="">all</option>
              <option value="true">halted</option>
              <option value="false">not halted</option>
            </select>
          </label>
        </div>

        {resource.error ? <div className="error-banner">{resource.error}</div> : null}
        {resource.loading && !resource.data ? <div className="table-empty">Loading open orders...</div> : null}

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
            <MarketTable
              rows={resource.data.items}
              mode="open-orders"
              emptyLabel="No open order markets matched the current filters."
            />
          </>
        ) : null}
      </section>
    </section>
  );
}
