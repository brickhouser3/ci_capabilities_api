import type { VercelRequest, VercelResponse } from "@vercel/node";

const API_VERSION = "2026-01-14_corsfix_v4"; // ✅ Bumped version

function setCors(req: VercelRequest, res: VercelResponse) {
  // NUCLEAR DEBUGGING: Allow everyone
  res.setHeader("Access-Control-Allow-Origin", "*"); 
  
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, Accept, x-mc-api");
  res.setHeader("Access-Control-Max-Age", "86400");
  res.setHeader("Access-Control-Expose-Headers", "x-mc-api,x-mc-origin,x-mc-version");
  res.setHeader("Cache-Control", "no-store");
}

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

type KpiRequestV1 = {
  contract_version: "kpi_request.v1";
  kpi: string;
};

export default async function handler(req: VercelRequest, res: VercelResponse) {
  // ✅ Always set CORS + debug headers first, for every path
  setCors(req, res);

  res.setHeader("x-mc-api", "query.ts");
  res.setHeader("x-mc-origin", String(req.headers.origin || "(none)"));
  res.setHeader("x-mc-version", API_VERSION);

  // ✅ Preflight must return with headers already set
  if (req.method === "OPTIONS") return res.status(204).end();

  // ✅ Health
  if (req.method === "GET") {
    return res.status(200).json({
      ok: true,
      now: new Date().toISOString(),
      version: API_VERSION,
    });
  }

  if (req.method !== "POST") {
    return res.status(405).json({ ok: false, error: "Method not allowed" });
  }

  try {
    const body: any =
      typeof req.body === "string" ? JSON.parse(req.body || "{}") : req.body;

    if (body?.ping === true) {
      return res.status(200).json({
        ok: true,
        mode: "ping",
        now: new Date().toISOString(),
        version: API_VERSION,
        received: body,
      });
    }

    const host = process.env.DATABRICKS_HOST;
    const token = process.env.DATABRICKS_TOKEN;
    const warehouseId = process.env.WAREHOUSE_ID;

    if (!host || !token || !warehouseId) {
      return res.status(500).json({
        ok: false,
        error: "Missing Databricks env vars",
        version: API_VERSION,
        hasHost: !!host,
        hasToken: !!token,
        hasWarehouseId: !!warehouseId,
      });
    }

    const parsed = body as Partial<KpiRequestV1>;

    let statement =
      "select max(cal_dt) as value from vip.bir.bir_weekly_ind"; // default

    if (parsed?.contract_version === "kpi_request.v1") {
      switch (parsed?.kpi) {
        case "volume":
          statement =
            "select max(cal_dt) as value from vip.bir.bir_weekly_ind";
          break;
        default:
          return res.status(400).json({
            ok: false,
            error: `Unknown kpi '${parsed?.kpi}'`,
            version: API_VERSION,
          });
      }
    }

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
        version: API_VERSION,
        dbx: submitted,
      });
    }

    const statementId: string | undefined = submitted?.statement_id;
    if (!statementId) {
      return res.status(502).json({
        ok: false,
        error: "Databricks did not return statement_id",
        version: API_VERSION,
        dbx: submitted,
      });
    }

    const deadlineMs = Date.now() + 12_000;
    let last = submitted;

    while (Date.now() < deadlineMs) {
      const state = last?.status?.state;

      if (state === "SUCCEEDED" && last?.result?.data_array) break;
      if (state === "FAILED" || state === "CANCELED") break;

      await sleep(350);

      const pollRes = await fetch(
        `${host}/api/2.0/sql/statements/${statementId}`,
        {
          method: "GET",
          headers: {
            Authorization: `Bearer ${token}`,
            Accept: "application/json",
          },
        }
      );

      last = await pollRes.json();
    }

    const finalState = last?.status?.state;

    if (finalState !== "SUCCEEDED" || !last?.result?.data_array) {
      return res.status(502).json({
        ok: false,
        error: "Databricks statement did not return results",
        version: API_VERSION,
        state: finalState,
        statement_id: statementId,
        dbx: last,
      });
    }

    return res.status(200).json({
      ok: true,
      version: API_VERSION,
      result: last.result,
      statement_id: statementId,
      state: finalState,
    });
  } catch (err: any) {
    return res.status(500).json({
      ok: false,
      error: "Databricks query failed",
      version: API_VERSION,
      details: err?.message ?? "unknown",
    });
  }
}