import type { VercelRequest, VercelResponse } from "@vercel/node";

const API_VERSION = "2026-01-15_filters_v2_safe";

function setCors(req: VercelRequest, res: VercelResponse) {
  const origin = req.headers.origin || "";
  
  // ✅ ALLOW LOCALHOST explicitly
  const allowedDomains = [
      "https://brickhouser3.github.io", 
      "http://localhost:3000", 
      "http://localhost:3001",
      "http://127.0.0.1:3000"
  ];
  
  if (allowedDomains.some(d => origin.startsWith(d))) {
    res.setHeader("Access-Control-Allow-Origin", origin);
  }
  
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, x-mc-api, x-mc-version");
}

export default async function handler(req: VercelRequest, res: VercelResponse) {
  setCors(req, res);
  if (req.method === "OPTIONS") return res.status(200).end();

  // ✅ 1. Method Guard
  if (req.method !== "POST") {
      return res.status(405).json({ ok: false, error: "Method not allowed. Please use POST." });
  }

  try {
    // ✅ 2. Safe Body Parsing (Prevents the "undefined" crash)
    const rawBody = req.body;
    const body = rawBody 
        ? (typeof rawBody === "string" ? JSON.parse(rawBody) : rawBody) 
        : {};

    const { dimension, table } = body;

    // ✅ 3. Validation Check
    if (!dimension || !table) {
        return res.status(400).json({ ok: false, error: "Missing required fields: 'dimension' or 'table'" });
    }

    // --- SECURITY: Allowlist ---
    const ALLOWED_COLS = ["wslr_nbr", "mktng_st_cd", "sls_regn_cd", "channel"];
    const ALLOWED_TABLES = ["mbmc_actuals_volume", "mbmc_actuals_revenue", "mbmc_actuals_distro"];

    if (!ALLOWED_COLS.includes(dimension) || !ALLOWED_TABLES.includes(table)) {
        return res.status(400).json({ ok: false, error: `Invalid dimension '${dimension}' or table '${table}'` });
    }

    // --- CREDENTIALS ---
    const host = process.env.DATABRICKS_HOST;
    const token = process.env.DATABRICKS_TOKEN;
    const warehouseId = process.env.WAREHOUSE_ID;

    // --- SQL: Get Distinct Values ---
    const sql = `
      SELECT DISTINCT ${dimension} as label 
      FROM commercial_dev.capabilities.${table} 
      WHERE ${dimension} IS NOT NULL 
      ORDER BY ${dimension} ASC 
      LIMIT 2000
    `;

    // --- EXECUTE ---
    const submitRes = await fetch(`${host}/api/2.0/sql/statements`, {
      method: "POST",
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
      body: JSON.stringify({ statement: sql, warehouse_id: warehouseId }),
    });

    const submitted = await submitRes.json();
    const statementId = submitted?.statement_id;

    if (!statementId) {
        throw new Error(`Databricks Error: ${submitted?.message || "No statement_id returned"}`);
    }

    // Polling logic
    let state = "PENDING";
    let result = null;
    const start = Date.now();
    
    while (state !== "SUCCEEDED" && state !== "FAILED" && Date.now() - start < 10000) {
        await new Promise(r => setTimeout(r, 200));
        const poll = await fetch(`${host}/api/2.0/sql/statements/${statementId}`, {
            headers: { Authorization: `Bearer ${token}` }
        });
        const json = await poll.json();
        state = json.status.state;
        if (state === "SUCCEEDED") result = json.result;
    }

    if (!result) throw new Error("Query timed out or failed");

    // Format for Dropdown
    const options = result.data_array.map((row: string[]) => ({
        label: row[0],
        value: row[0]
    }));

    return res.status(200).json({ ok: true, options });

  } catch (err: any) {
    console.error("Filter API Error:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
}