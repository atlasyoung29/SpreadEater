export function formatMoney(value: string | number | null | undefined) {
  if (value === null || value === undefined) {
    return "n/a";
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return "n/a";
  }
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(parsed);
}

export function formatNumber(value: string | number | null | undefined) {
  if (value === null || value === undefined) {
    return "n/a";
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return "n/a";
  }
  return new Intl.NumberFormat("en-US", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(parsed);
}

export function formatPercent(value: string | number | null | undefined) {
  if (value === null || value === undefined) {
    return "n/a";
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return "n/a";
  }
  return `${formatNumber(parsed)}%`;
}

export function formatYieldPercent(
  rewardPerDay: string | number | null | undefined,
  capitalDeployed: string | number | null | undefined,
) {
  const reward = toFiniteNumber(rewardPerDay);
  const capital = toFiniteNumber(capitalDeployed);
  if (reward === null || capital === null || capital <= 0) {
    return "n/a";
  }
  return formatPercent((reward / capital) * 100);
}

export function formatAgeMs(value: string | number | null | undefined) {
  const parsed = toFiniteNumber(value);
  if (parsed === null) {
    return "n/a";
  }
  if (parsed < 1_000) {
    return `${Math.round(parsed)}ms`;
  }
  if (parsed < 60_000) {
    return `${(parsed / 1_000).toFixed(1)}s`;
  }
  return `${Math.floor(parsed / 60_000)}m ${Math.round((parsed % 60_000) / 1_000)}s`;
}

export function formatTimestamp(value: string | null | undefined) {
  if (!value) {
    return "n/a";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "n/a";
  }
  return date.toLocaleString();
}

export function formatBoolean(value: boolean | null | undefined) {
  if (value === null || value === undefined) {
    return "n/a";
  }
  return value ? "yes" : "no";
}

export function formatIdentifier(
  value: string | null | undefined,
  head = 8,
  tail = 6,
) {
  if (!value) {
    return "n/a";
  }
  if (value.length <= head + tail + 3) {
    return value;
  }
  return `${value.slice(0, head)}...${value.slice(-tail)}`;
}

export function multiplyPayloadNumbers(left: unknown, right: unknown): number | null {
  const leftNumber = readPayloadNumber(left);
  const rightNumber = readPayloadNumber(right);
  if (leftNumber === null || rightNumber === null) {
    return null;
  }
  return leftNumber * rightNumber;
}

export function sumRoundedMoneyValues(
  values: Array<string | number | null | undefined>,
) {
  const cents = values.reduce<number>((total, value) => {
    const parsed = toFiniteNumber(value);
    if (parsed === null) {
      return total;
    }
    return total + Math.round(parsed * 100);
  }, 0);
  return cents / 100;
}

export function formatSizeExpression(
  price: string | number | null | undefined,
  shares: string | number | null | undefined,
  value?: string | number | null | undefined,
) {
  const priceNumber = toFiniteNumber(price);
  const shareNumber = toFiniteNumber(shares);
  const valueNumber = toFiniteNumber(value);

  if (priceNumber === null || shareNumber === null) {
    return "n/a";
  }

  const totalValue = valueNumber ?? priceNumber * shareNumber;
  return `${formatMoney(priceNumber)} x ${formatNumber(shareNumber)} = ${formatMoney(totalValue)}`;
}

export function readPayloadNumber(value: unknown): number | null {
  return toFiniteNumber(value);
}

export function readPayloadText(value: unknown) {
  if (typeof value === "string") {
    return value;
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  return "n/a";
}

export function normalizeLevel(value: string | null | undefined) {
  if (!value) {
    return "unknown";
  }
  return value.toLowerCase();
}

function toFiniteNumber(value: unknown): number | null {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : null;
  }
  if (typeof value === "string") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}
