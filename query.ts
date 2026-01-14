export const config = {
  runtime: "nodejs",
};

import type { VercelRequest, VercelResponse } from "@vercel/node";

function setCors(req: VercelRequest, res: VercelResponse) {
  const origin = req.headers.origin || "";

  const allowlist = new Set([
    "https://brickhouser3.github.io",
    // keep these if you want, but we'll also allow any localhost port below
    "http://localhost:3000",
    "http://localhost:3001",
  ]);

  const allowLocalhost =
    origin.startsWith("http://localhost:") ||
    origin.startsWith("http://127.0.0.1:");

  if (allowlist.has(origin) || allowLocalhost) {
    res.setHeader("Access-Control-Allow-Origin", origin);
  }

  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS, GET");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  res.setHeader("Access-Control-Max-Age", "86400");
  res.setHeader("Vary", "Origin");
  res.setHeader("Cache-Control", "no-store");
}


function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

type KpiRequestV1 = {
  contract_version: "kpi_request.v1";
  kpi: string; // "volume" etc
  // later: filters, date_range, etc
};

export default async function handler(req: VercelRequest, res: VercelResponse) {
  setCors(req, res);

  res.setHeader("x-mc-api", "query.ts");
  res.setHeader("x-mc-origin", req.headers.origin || "(none)");

  // ✅ Proper preflight
  if (req.method === "OPTIONS") return res.status(204).end();

  // ✅ Simple health check
  if (req.method === "GET") return res.status(200).json({ ok: true });

  if (req.method !== "POST") {
    return res.status(405).json({ ok: false, error: "Method not allowed" });
  }

  try {
    const host = process.env.DATABRICKS_HOST;
    const token = process.env.DATABRICKS_TOKEN;
    const warehouseId = process.env.WAREHOUSE_ID;

    if (!host || !token || !warehouseId) {
      return res.status(500).json({
        ok: false,
        error: "Missing Databricks env vars",
        hasHost: !!host,
        hasToken: !!token,
        hasWarehouseId: !!warehouseId,
      });
    }

    // ✅ Parse body for BOTH text/plain and application/json
    const body: any =
      typeof req.body === "string" ? JSON.parse(req.body || "{}") : req.body;

    const parsed = body as Partial<KpiRequestV1>;

    if (body?.ping === true) {
  return res.status(200).json({
    ok: true,
    mode: "ping",
    now: new Date().toISOString(),
    received: body,
  });
}


    // ✅ Route by contract (keeps your UI stable as you add more KPIs)
    let statement =
      "select max(cal_dt) as value from vip.bir.bir_weekly_ind"; // default

    if (parsed?.contract_version === "kpi_request.v1") {
      switch (parsed?.kpi) {
        case "volume":
          // TODO: replace with real volume KPI SQL
          // keep alias "value" so your normalization stays consistent
          statement = "select max(cal_dt) as value from vip.bir.bir_weekly_ind";
          break;

        default:
          return res.status(400).json({
            ok: false,
            error: `Unknown kpi '${parsed?.kpi}'`,
          });
      }
    }

    // ✅ Submit statement
    const submitRes = await fetch(`${host}/api/2.0/sql/statements`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
        Accept: "application/json",
      },
      body: JSON.stringify({
        statement,
        warehouse_id: warehouseId,
      }),
    });

    const submitted = await submitRes.json();

    if (!submitRes.ok) {
      return res.status(submitRes.status).json({
        ok: false,
        error: "Databricks submit failed",
        dbx: submitted,
      });
    }

    const statementId: string | undefined = submitted?.statement_id;
    if (!statementId) {
      return res.status(502).json({
        ok: false,
        error: "Databricks did not return statement_id",
        dbx: submitted,
      });
    }

    // ✅ Poll until SUCCEEDED (short timeout)
    const deadlineMs = Date.now() + 12_000;
    let last = submitted;

    while (Date.now() < deadlineMs) {
      const state = last?.status?.state;

      // If result is already attached, we’re done
      if (state === "SUCCEEDED" && last?.result?.data_array) break;

      // Terminal failure states
      if (state === "FAILED" || state === "CANCELED") break;

      await sleep(350);

      const pollRes = await fetch(`${host}/api/2.0/sql/statements/${statementId}`, {
        method: "GET",
        headers: {
          Authorization: `Bearer ${token}`,
          Accept: "application/json",
        },
      });

      last = await pollRes.json();
    }

    const finalState = last?.status?.state;

    if (finalState !== "SUCCEEDED" || !last?.result?.data_array) {
      return res.status(502).json({
        ok: false,
        error: "Databricks statement did not return results",
        state: finalState,
        statement_id: statementId,
        dbx: last,
      });
    }

    // ✅ Return a stable shape for your hook: raw.result.data_array[0][0]
    return res.status(200).json({
      ok: true,
      result: last.result,
      statement_id: statementId,
      state: finalState,
    });
  } catch (err: any) {
    return res.status(500).json({
      ok: false,
      error: "Databricks query failed",
      details: err?.message ?? "unknown",
    });
  }
}
