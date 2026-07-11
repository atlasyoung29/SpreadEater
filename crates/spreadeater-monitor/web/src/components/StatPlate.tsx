interface StatPlateProps {
  label: string;
  value: string | number;
  accent?: "signal" | "ink" | "neutral" | "edge" | "reward";
  meta?: string;
}

export function StatPlate({
  label,
  value,
  accent = "neutral",
  meta,
}: StatPlateProps) {
  return (
    <article className={`stat-plate stat-plate--${accent}`}>
      <p>{label}</p>
      <strong>{value}</strong>
      {meta ? <span className="stat-plate__meta">{meta}</span> : null}
    </article>
  );
}
