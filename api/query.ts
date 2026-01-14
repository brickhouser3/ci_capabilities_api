import type { VercelRequest, VercelResponse } from "@vercel/node";

const API_VERSION = "2026-01-14_corsfix_secure_final";

function setCors(req: VercelRequest, res: VercelResponse) {
  const origin = req.headers.origin || "";

  // ✅ Security Check: Only allow Localhost and your GitHub Pages
  const isLocalhost =
    origin.startsWith("http://localhost") ||
    origin.startsWith("https://localhost") ||
    origin.startsWith("http://127.0.0.1");

  const allowedDomains = ["https://brickhouser3.github.io"];

  // Only set the header if the origin matches our allowlist
  if (isLocalhost || allowedDomains.includes(origin)) {
    res.setHeader("Access-Control-Allow-Origin", origin);
  }

  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  
  // ✅ Keep 'Accept' to prevent browser blocks
  res.setHeader(
    "Access-Control-Allow-Headers", 
    "Content-Type, Authorization, Accept, x-mc-api, x-mc-version"
  );

  // ✅ Added 'Access-Control-Allow-Origin' here so your UI shows the value instead of "(missing)"
  res.setHeader(
    "Access-Control-Expose-Headers", 
    "x-mc-api, x-mc-origin, x-mc-version, Content-Length, Access-Control-Allow-Origin"
  );
  
  res.setHeader("Access-Control-Max-Age", "86400");
}

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

type KpiRequestV1 = {
  contract_version: "kpi_request.v1";
  kpi: string;
};

export default async function handler(req: VercelRequest, res: VercelResponse) {
  setCors(req, res);

  // Handle Preflight
  if (req.method === "OPTIONS") {
    return res.status(200).end();
  }

  // Debug headers
  res.setHeader("x-mc-api", "query.ts");
  res.setHeader("x-mc-version", API_VERSION);

  try {
    if (req.method === "GET") {
      return res.status(200).json({
        ok: true,
        now: new Date().toISOString(),
        version: API_VERSION,
        status: "operational"
      });
    }

    if (req.method !== "POST") {
      return res.status(405).json({ ok: false, error: "Method not allowed" });
    }

    const body: any = typeof req.body === "string" ? JSON.parse(req.body || "{}") : req.body;

    if (body?.ping === true) {
      return res.status(200).json({
        ok: true,
        mode: "ping",
        version: API_VERSION,
        received_origin: req.headers.origin || "unknown"
      });
    }

    // --- DATABRICKS LOGIC ---
    const host = process.env.DATABRICKS_HOST;
    const token = process.env.DATABRICKS_TOKEN;
    const warehouseId = process.env.WAREHOUSE_ID;

    if (!host || !token || !warehouseId) {
      return res.status(500).json({
        ok: false,
        error: "Server configuration error",
        version: API_VERSION
      });
    }

    const parsed = body as Partial<KpiRequestV1>;
    let statement = "select max(cal_dt) as value from vip.bir.bir_weekly_ind"; 

    if (parsed?.contract_version === "kpi_request.v1" && parsed?.kpi === "volume") {
      statement = "select max(cal_dt) as value from vip.bir.bir_weekly_ind";
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
        dbx_msg: submitted?.message
      });
    }

    const statementId = submitted?.statement_id;
    if (!statementId) throw new Error("No statement_id returned");

    const deadlineMs = Date.now() + 12_000;
    let last = submitted;

    while (Date.now() < deadlineMs) {
      const state = last?.status?.state;
      if (state === "SUCCEEDED" || state === "FAILED" || state === "CANCELED") break;
      
      await sleep(500);
      
      const pollRes = await fetch(`${host}/api/2.0/sql/statements/${statementId}`, {
        method: "GET",
        headers: { Authorization: `Bearer ${token}` }
      });
      last = await pollRes.json();
    }

    if (last?.status?.state !== "SUCCEEDED") {
      return res.status(502).json({
        ok: false,
        error: "Query timed out or failed",
        state: last?.status?.state
      });
    }

    return res.status(200).json({
      ok: true,
      result: last.result,
      version: API_VERSION
    });

  } catch (err: any) {
    console.error("API Crash:", err);
    return res.status(500).json({
      ok: false,
      error: "Internal Server Error",
      details: err.message
    });
  }
}