import { useState } from "react";
import { MarketTable } from "../components/MarketTable";
import { PaginationControls } from "../components/PaginationControls";
import { fetchWatchlist } from "../lib/api";
import { usePagedResource } from "../lib/usePagedResource";

export function WatchlistPage() {
  const [q, setQ] = useState("");
  const [halted, setHalted] = useState("");
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(100);

  const query = { q, halted, page, page_size: pageSize };
  const resource = usePagedResource({
    queryKey: JSON.stringify(query),
    autoRefresh: page === 1,
    loader: () => fetchWatchlist(query),
  });

  return (
    <section className="page page--ops">
      <section className="section-card section-card--dense">
        <div className="section-card__header">
          <div>
            <p className="eyebrow">watchlist</p>
            <h1 className="section-card__title">Market board</h1>
          </div>
          <div className="section-card__meta">
            <span>latest non-entry reason stays visible</span>
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
            <span>halted</span>
            <select value={halted} onChange={(event) => { setPage(1); setHalted(event.target.value); }}>
              <option value="">all</option>
              <option value="true">halted</option>
              <option value="false">not halted</option>
            </select>
          </label>
        </div>

        {resource.error ? <div className="error-banner">{resource.error}</div> : null}
        {resource.loading && !resource.data ? <div className="table-empty">Loading watchlist...</div> : null}

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
              mode="watchlist"
              emptyLabel="No watchlist markets matched the current filters."
            />
          </>
        ) : null}
      </section>
    </section>
  );
}
