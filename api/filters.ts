import type { VercelRequest, VercelResponse } from "@vercel/node";

const API_VERSION = "2026-01-15_filters_v3_patience";

function setCors(req: VercelRequest, res: VercelResponse) {
  const origin = req.headers.origin || "";
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

function sleep(ms: number) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

export default async function handler(req: VercelRequest, res: VercelResponse) {
  setCors(req, res);
  if (req.method === "OPTIONS") return res.status(200).end();

  if (req.method !== "POST") {
      return res.status(405).json({ ok: false, error: "Method not allowed. Please use POST." });
  }

  try {
    const rawBody = req.body;
    const body = rawBody 
        ? (typeof rawBody === "string" ? JSON.parse(rawBody) : rawBody) 
        : {};

    const { dimension, table } = body;

    if (!dimension || !table) {
        return res.status(400).json({ ok: false, error: "Missing required fields" });
    }

    // Security Allowlist (Make sure your columns are here)
    const ALLOWED_COLS = ["wslr_nbr", "mktng_st_cd", "sls_regn_cd", "channel", "megabrand"];
    const ALLOWED_TABLES = ["mbmc_actuals_volume", "mbmc_actuals_revenue", "mbmc_actuals_distro"];

    if (!ALLOWED_COLS.includes(dimension) || !ALLOWED_TABLES.includes(table)) {
        return res.status(400).json({ ok: false, error: `Invalid dimension '${dimension}' or table` });
    }

    const host = process.env.DATABRICKS_HOST;
    const token = process.env.DATABRICKS_TOKEN;
    const warehouseId = process.env.WAREHOUSE_ID;

    // ✅ FIXED SQL: ORDER BY the alias 'label'
    const sql = `
      SELECT DISTINCT ${dimension} as label 
      FROM commercial_dev.capabilities.${table} 
      WHERE ${dimension} IS NOT NULL 
      ORDER BY label ASC 
      LIMIT 2000
    `;

    // --- SUBMIT ---
    const submitRes = await fetch(`${host}/api/2.0/sql/statements`, {
      method: "POST",
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
      body: JSON.stringify({ statement: sql, warehouse_id: warehouseId, wait_timeout: "0s" }),
    });

    const submitted = await submitRes.json();
    const statementId = submitted?.statement_id;

    if (!statementId) {
        throw new Error(`Databricks Error: ${submitted?.message || "No statement_id returned"}`);
    }

    // --- POLLING ---
    const DEADLINE = Date.now() + 45000; 
    let state = "PENDING";
    let result = null;
    
    while (Date.now() < DEADLINE) {
        const poll = await fetch(`${host}/api/2.0/sql/statements/${statementId}`, {
            headers: { Authorization: `Bearer ${token}` }
        });
        const json = await poll.json();
        state = json.status?.state;

        if (state === "SUCCEEDED") {
            result = json.result;
            break;
        }
        if (["FAILED", "CANCELED", "CLOSED"].includes(state)) {
            throw new Error(`Query failed with state: ${state}. Error: ${json.status?.error?.message}`);
        }
        await sleep(1000);
    }

    if (!result) {
        await fetch(`${host}/api/2.0/sql/statements/${statementId}/cancel`, {
             method: "POST", headers: { Authorization: `Bearer ${token}` } 
        });
        return res.status(504).json({ ok: false, error: "Query timed out" });
    }

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