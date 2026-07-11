import { useState } from "react";
import { MarketTable } from "../components/MarketTable";
import { PaginationControls } from "../components/PaginationControls";
import { fetchInventory } from "../lib/api";
import { usePagedResource } from "../lib/usePagedResource";

export function InventoryPage() {
  const [q, setQ] = useState("");
  const [neutrality, setNeutrality] = useState("");
  const [hasOpenOrders, setHasOpenOrders] = useState("");
  const [halted, setHalted] = useState("");
  const [exposureSide, setExposureSide] = useState("");
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(100);

  const query = {
    q,
    neutrality,
    has_open_orders: hasOpenOrders,
    halted,
    exposure_side: exposureSide,
    page,
    page_size: pageSize,
  };

  const resource = usePagedResource({
    queryKey: JSON.stringify(query),
    autoRefresh: page === 1,
    loader: () => fetchInventory(query),
  });

  return (
    <section className="page page--ops">
      <section className="section-card section-card--dense">
        <div className="section-card__header">
          <div>
            <p className="eyebrow">inventory</p>
            <h1 className="section-card__title">Live positions</h1>
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
            <span>neutrality</span>
            <select value={neutrality} onChange={(event) => { setPage(1); setNeutrality(event.target.value); }}>
              <option value="">all</option>
              <option value="true">neutral</option>
              <option value="false">non-neutral</option>
            </select>
          </label>
          <label className="toolbar-field">
            <span>open orders</span>
            <select value={hasOpenOrders} onChange={(event) => { setPage(1); setHasOpenOrders(event.target.value); }}>
              <option value="">all</option>
              <option value="true">with open orders</option>
              <option value="false">no open orders</option>
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
          <label className="toolbar-field">
            <span>exposure</span>
            <select value={exposureSide} onChange={(event) => { setPage(1); setExposureSide(event.target.value); }}>
              <option value="">all</option>
              <option value="yes">yes-heavy</option>
              <option value="no">no-heavy</option>
              <option value="flat">flat</option>
            </select>
          </label>
        </div>

        {resource.error ? <div className="error-banner">{resource.error}</div> : null}
        {resource.loading && !resource.data ? <div className="table-empty">Loading inventory...</div> : null}

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
              mode="inventory"
              emptyLabel="No inventory markets matched the current filters."
            />
          </>
        ) : null}
      </section>
    </section>
  );
}
