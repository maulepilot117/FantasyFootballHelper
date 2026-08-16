export type Health = { status: string; version: string; season: number };

export async function fetchHealth(fetchImpl: typeof fetch = fetch): Promise<Health> {
  const res = await fetchImpl("/api/v1/healthz");
  if (!res.ok) throw new Error(`health ${res.status}`);
  return (await res.json()) as Health;
}
