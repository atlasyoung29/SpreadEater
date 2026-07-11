export function PaginationControls({
  page,
  pageSize,
  total,
  onPageChange,
  onPageSizeChange,
}: {
  page: number;
  pageSize: number;
  total: number;
  onPageChange: (page: number) => void;
  onPageSizeChange: (size: number) => void;
}) {
  const totalPages = Math.max(1, Math.ceil(total / pageSize));

  return (
    <div className="pager">
      <div className="pager__summary">
        <span>{total} rows</span>
        <span>
          page {Math.min(page, totalPages)} of {totalPages}
        </span>
      </div>
      <div className="pager__controls">
        <label className="toolbar-field toolbar-field--inline">
          <span>page size</span>
          <select
            value={pageSize}
            onChange={(event) => {
              onPageSizeChange(Number(event.target.value));
            }}
          >
            <option value={50}>50</option>
            <option value={100}>100</option>
            <option value={250}>250</option>
          </select>
        </label>
        <button
          type="button"
          className="toolbar-button"
          disabled={page <= 1}
          onClick={() => onPageChange(page - 1)}
        >
          newer
        </button>
        <button
          type="button"
          className="toolbar-button"
          disabled={page >= totalPages}
          onClick={() => onPageChange(page + 1)}
        >
          older
        </button>
      </div>
    </div>
  );
}
