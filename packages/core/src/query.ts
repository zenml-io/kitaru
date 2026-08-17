export type QueryPrimitive = string | number | boolean;
export type QueryValue =
  | QueryPrimitive
  | readonly QueryPrimitive[]
  | null
  | undefined;
export type QueryParameters = Readonly<Record<string, QueryValue>>;

export function encodeQuery(parameters?: QueryParameters): string {
  if (parameters === undefined) {
    return "";
  }

  const query = new URLSearchParams();
  for (const [name, value] of Object.entries(parameters)) {
    if (value === undefined || value === null) {
      continue;
    }
    const values = Array.isArray(value) ? value : [value];
    for (const item of values) {
      query.append(name, String(item));
    }
  }
  const encoded = query.toString();
  return encoded.length === 0 ? "" : `?${encoded}`;
}
