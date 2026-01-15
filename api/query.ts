import type { VercelRequest, VercelResponse } from "@vercel/node";

const API_VERSION = "2026-01-15_agg_logic_v4";

/* ======================================================
   1. KPI CONFIGURATION
====================================================== */
const KPI_MAP: Record<
  string,
  { 
    table: string; 
    col: string; 
    hasChannel: boolean; 
    geoColumn: string;
    agg: "SUM" | "AVG"; // ✅ NEW: Controls math logic
  }
> = {
  // --- ADDITIVE METRICS (SUM) ---
  volume: {
    table: "mbmc_actuals_volume",
    col: "STRs",
    hasChannel: true,
    geoColumn: "WSLR_NBR",
    agg: "SUM"
  },
  revenue: {
    table: "mbmc_actuals_revenue",
    col: "net_rev",
    hasChannel: false,
    geoColumn: "WSLR_NBR",
    agg: "SUM"
  },
  displays: {
    table: "mbmc_actuals_displays",
    col: "displays",
    hasChannel: true,
    geoColumn: "WSLR_NBR",
    agg: "SUM"
  },

  // --- NON-ADDITIVE METRICS (AVG) ---
  share: {
    table: "mbmc_actuals_bir",
    col: "shr",
    hasChannel: true,
    geoColumn: "WSLR_NBR",
    agg: "AVG" // ✅ Fixes "200% share" issue
  },
  adshare: {
    table: "mbmc_actuals_ads",
    col: "ad_share",
    hasChannel: true,
    geoColumn: "KAM",
    agg: "AVG"
  },
  
  // --- DISTRO (RATES & SNAPSHOTS) ---
  // Using AVG prevents double-counting across months (e.g. Jan+Feb PODs)
  pods: {
    table: "mbmc_actuals_distro",
    col: "pods",
    hasChannel: true,
    geoColumn: "WSLR_NBR",
    agg: "SUM"
  },
  taps: {
    table: "mbmc_actuals_distro",
    col: "taps",
    hasChannel: true,
    geoColumn: "WSLR_NBR",
    agg: "SUM"
  },
  avd: {
    table: "mbmc_actuals_distro",
    col: "avd",
    hasChannel: true,
    geoColumn: "WSLR_NBR",
    agg: "AVG"
  },
};

type KpiRequestV1 = {
  contract_version: "kpi_request.v1";
  kpi: string;
  groupBy?: "time" | "megabrand" | "region" | "state" | "wholesaler" | "channel";
  max_month?: string;
  scope?: "MTD" | "YTD";
  filters?: {
    megabrand?: string[];
    wholesaler_id?: string[];
    channel?: string[];
  };
};

function setCors(req: VercelRequest, res: VercelResponse) {
  const origin = req.headers.origin || "";
  const isLocalhost = origin.startsWith("http://localhost") || origin.startsWith("https://localhost") || origin.startsWith("http://127.0.0.1");
  const allowedDomains = ["https://brickhouser3.github.io"];

  if (isLocalhost || allowedDomains.includes(origin)) {
    res.setHeader("Access-Control-Allow-Origin", origin);
  }
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, Accept, x-mc-api, x-mc-version");
  res.setHeader("Access-Control-Expose-Headers", "x-mc-api, x-mc-origin, x-mc-version, Content-Length, Access-Control-Allow-Origin");
  res.setHeader("Access-Control-Max-Age", "86400");
}

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

export default async function handler(req: VercelRequest, res: VercelResponse) {
  setCors(req, res);
  if (req.method === "OPTIONS") return res.status(200).end();

  res.setHeader("x-mc-api", "query.ts");
  res.setHeader("x-mc-version", API_VERSION);

  try {
    if (req.method === "GET") return res.status(200).json({ ok: true, status: "operational", version: API_VERSION });
    if (req.method !== "POST") return res.status(405).json({ ok: false, error: "Method not allowed" });

    const body = typeof req.body === "string" ? JSON.parse(req.body || "{}") : req.body;
    if (body?.ping === true) return res.status(200).json({ ok: true, mode: "ping" });

    // --- CREDENTIALS ---
    const host = process.env.DATABRICKS_HOST;
    const token = process.env.DATABRICKS_TOKEN;
    const warehouseId = process.env.WAREHOUSE_ID;

    if (!host || !token || !warehouseId) return res.status(500).json({ ok: false, error: "Server missing Databricks credentials" });

    const { kpi, filters, groupBy = "time", max_month = "202512", scope = "YTD" } = body as KpiRequestV1;

    // --- 2. RESOLVE CONFIG ---
    const config = KPI_MAP[kpi];
    if (!config) return res.status(400).json({ ok: false, error: `KPI '${kpi}' is not configured in API map.` });

    const tableName = `commercial_dev.capabilities.${config.table}`;
    const colCy = `${config.col}_CY`;
    const colLy = `${config.col}_LY`;
    const AGG_FUNC = config.agg; // "SUM" or "AVG"

    // --- 3. FILTER LOGIC ---
    const conditions: string[] = ["1=1"];

    if (scope === "MTD") {
      conditions.push(`cal_yr_mo_nbr = ${max_month}`);
    } else {
      const startOfYear = max_month.substring(0, 4) + "01";
      conditions.push(`cal_yr_mo_nbr BETWEEN ${startOfYear} AND ${max_month}`);
    }

    if (filters?.megabrand && filters.megabrand.length > 0) {
      const list = filters.megabrand.map((s) => `'${s.replace(/'/g, "''")}'`).join(",");
      conditions.push(`megabrand IN (${list})`);
    }

    // --- 4. DYNAMIC GROUPING ---
    let selectClause = "";
    let groupByClause = "";
    let orderByClause = "ORDER BY val_cy DESC";

    switch (groupBy) {
      case "megabrand":
        selectClause = `megabrand as dimension`;
        groupByClause = `GROUP BY megabrand`;
        break;

      case "region":
        selectClause = `sls_regn_cd as dimension`;
        groupByClause = `GROUP BY sls_regn_cd`;
        break;

      case "state":
        selectClause = `mktng_st_cd as dimension`;
        groupByClause = `GROUP BY mktng_st_cd`;
        break;

      case "wholesaler":
        const geoCol = config.geoColumn;
        selectClause = `${geoCol} as dimension`;
        groupByClause = `GROUP BY ${geoCol}`;
        break;

      case "channel":
        if (!config.hasChannel) {
          selectClause = `'All Channels' as dimension`;
          groupByClause = ``;
        } else {
          selectClause = `channel as dimension`;
          groupByClause = `GROUP BY channel`;
        }
        break;

      case "time":
      default:
        selectClause = `cal_yr_mo_nbr as dimension`;
        groupByClause = `GROUP BY cal_yr_mo_nbr`;
        orderByClause = "ORDER BY cal_yr_mo_nbr ASC";
        break;
    }

    // --- 5. ASSEMBLE SQL WITH CORRECT AGGREGATION ---
    // ✅ Uses AVG() or SUM() based on metric config
    const finalSql = `
      SELECT ${selectClause},
      ${AGG_FUNC}(${colCy}) as val_cy,
      ${AGG_FUNC}(${colLy}) as val_ly
      FROM ${tableName}
      WHERE ${conditions.join(" AND ")}
      ${groupByClause}
      ${orderByClause}
      LIMIT 1000
    `;

    // --- 6. EXECUTE ---
    const submitRes = await fetch(`${host}/api/2.0/sql/statements`, {
      method: "POST",
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
      body: JSON.stringify({ statement: finalSql, warehouse_id: warehouseId }),
    });

    const submitted = await submitRes.json();
    if (!submitRes.ok) return res.status(submitRes.status).json({ ok: false, error: "Databricks submit failed", dbx_msg: submitted?.message, sql: finalSql });

    const statementId = submitted?.statement_id;
    if (!statementId) throw new Error("No statement_id returned");

    const deadlineMs = Date.now() + 15000;
    let last = submitted;

    while (Date.now() < deadlineMs) {
      const state = last?.status?.state;
      if (["SUCCEEDED", "FAILED", "CANCELED"].includes(state)) break;
      await sleep(350);
      const pollRes = await fetch(`${host}/api/2.0/sql/statements/${statementId}`, {
        method: "GET",
        headers: { Authorization: `Bearer ${token}` }
      });
      last = await pollRes.json();
    }

    if (last?.status?.state !== "SUCCEEDED") return res.status(502).json({ ok: false, error: "Query timed out or failed", state: last?.status?.state });

    return res.status(200).json({ ok: true, result: last.result, version: API_VERSION, meta: { sql: finalSql } });

  } catch (err: any) {
    console.error("API Crash:", err);
    return res.status(500).json({ ok: false, error: "Internal Server Error", details: err.message });
  }
}