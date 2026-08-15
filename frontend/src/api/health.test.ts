import { expect, test } from "bun:test";
import { fetchHealth } from "./health";

test("fetchHealth parses the backend health payload", async () => {
  const fake = (async () =>
    new Response(JSON.stringify({ status: "ok", version: "0.1.0", season: 2026 }), {
      status: 200,
      headers: { "content-type": "application/json" },
    })) as unknown as typeof fetch;
  const h = await fetchHealth(fake);
  expect(h).toEqual({ status: "ok", version: "0.1.0", season: 2026 });
});

test("fetchHealth throws on non-2xx", async () => {
  const fake = (async () => new Response("nope", { status: 503 })) as unknown as typeof fetch;
  await expect(fetchHealth(fake)).rejects.toThrow("health 503");
});
