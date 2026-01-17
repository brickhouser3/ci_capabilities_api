import type { VercelRequest, VercelResponse } from "@vercel/node";

// 1. Helper to sanitize inputs (prevent SQL injection)
const sanitize = (val: string) => val.replace(/[^a-zA-Z0-9_]/g, "");

// 2. CORS Helper
function setCors(req: VercelRequest, res: VercelResponse) {
  const origin = req.headers.origin || "";
  res.setHeader("Access-Control-Allow-Origin", origin);
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
}

export default async function handler(req: VercelRequest, res: VercelResponse) {
  setCors(req, res);
  if (req.method === "OPTIONS") return res.status(200).end();

  try {
    const body = typeof req.body === "string" ? JSON.parse(req.body) : req.body;
    const { kpi, groupBy, max_month, scope, filters } = body;

    if (!kpi || !max_month) {
        return res.status(400).json({ ok: false, error: "Missing required params" });
    }

    // --- A. DATE LOGIC ---
    // Calculate the start month based on scope
    // If scope is 'MTD', we only want the specific max_month.
    // If scope is 'YTD', we want Jan of that year up to max_month.
    let dateCondition = "";
    if (scope === "MTD") {
        dateCondition = `month = '${max_month}'`;
    } else {
        // YTD Logic: Assume format "YYYYMM"
        const year = max_month.substring(0, 4);
        const startMonth = `${year}01`; // January
        dateCondition = `month >= '${startMonth}' AND month <= '${max_month}'`;
    }

    // --- B. AO / PRODUCT LOGIC ---
    // If include_ao is TRUE, we show everything.
    // If include_ao is FALSE (default), we restrict to Core brands only.
    // *Adjust 'is_core' to match your actual column name in Databricks*
    let productCondition = "";
    if (filters?.include_ao === true) {
        productCondition = "1=1"; // No filter (Show All)
    } else {
        // Example: Only show rows where brand_type is Core
        // You might need to change 'brand_type' to your actual column
        productCondition = "brand_type = 'Core'"; 
    }

    // --- C. DYNAMIC WHERE CLAUSE ---
    const whereParts = [dateCondition, productCondition];

    // Add standard filters
    if (filters?.megabrand?.length) {
        const brands = filters.megabrand.map((b: string) => `'${b}'`).join(",");
        whereParts.push(`megabrand IN (${brands})`);
    }
    if (filters?.region?.length) {
        const regions = filters.region.map((r: string) => `'${r}'`).join(",");
        whereParts.push(`sls_regn_cd IN (${regions})`);
    }
    if (filters?.state?.length) {
        const states = filters.state.map((s: string) => `'${s}'`).join(",");
        whereParts.push(`mktng_st_cd IN (${states})`);
    }
    if (filters?.wholesaler_id?.length) {
        const wslrs = filters.wholesaler_id.map((w: string) => `'${w}'`).join(",");
        whereParts.push(`wslr_nbr IN (${wslrs})`);
    }
    if (filters?.channel?.length) {
        const chans = filters.channel.map((c: string) => `'${c}'`).join(",");
        whereParts.push(`channel IN (${chans})`);
    }

    const whereClause = whereParts.filter(Boolean).join(" AND ");

    // --- D. BUILD SQL ---
    // Determine table based on KPI
    let table = "mbmc_actuals_volume"; // default
    if (kpi === "revenue") table = "mbmc_actuals_revenue";
    if (kpi === "share") table = "mbmc_actuals_share";
    // ... add mappings for pods/taps if they are in different tables

    // Determine Group By column
    let groupCol = "month"; // default for trend
    if (groupBy === "region") groupCol = "sls_regn_cd";
    if (groupBy === "state") groupCol = "mktng_st_cd";
    if (groupBy === "wholesaler") groupCol = "wslr_nbr";
    if (groupBy === "channel") groupCol = "channel";
    if (groupBy === "megabrand") groupCol = "megabrand";
    if (groupBy === "total") groupCol = "'Total'"; // Constant for single row sum

    // Select Clause
    // If we group by time, we sort by time. Otherwise sort by Value DESC.
    const orderBy = groupBy === "time" ? "ORDER BY 1 ASC" : "ORDER BY 2 DESC";
    
    // Aggregation: usually SUM for Vol/Rev, AVG for Share
    const aggFunc = ["share", "adshare", "avd"].includes(kpi) ? "AVG" : "SUM";

    const sql = `
      SELECT 
        ${groupCol} as key,
        ${aggFunc}(value_cy) as val_cy,
        ${aggFunc}(value_ly) as val_ly
      FROM commercial_dev.capabilities.${table}
      WHERE ${whereClause}
      GROUP BY 1
      ${orderBy}
      LIMIT 5000
    `;

    // --- E. EXECUTE (Databricks) ---
    const host = process.env.DATABRICKS_HOST;
    const token = process.env.DATABRICKS_TOKEN;
    const warehouseId = process.env.WAREHOUSE_ID;

    const queryRes = await fetch(`${host}/api/2.0/sql/statements`, {
      method: "POST",
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
      body: JSON.stringify({ statement: sql, warehouse_id: warehouseId, wait_timeout: "30s" }),
    });

    const queryJson = await queryRes.json();
    
    // Quick polling logic (Simplified for brevity)
    // For production, use the full loop we discussed previously
    let result = queryJson.result;
    if (!result && queryJson.statement_id) {
         // Simple wait for short queries
         await new Promise(r => setTimeout(r, 1500));
         const poll = await fetch(`${host}/api/2.0/sql/statements/${queryJson.statement_id}`, { 
             headers: { Authorization: `Bearer ${token}` } 
         });
         const pollJson = await poll.json();
         result = pollJson.result;
    }

    if (!result) throw new Error("Query execution pending or failed");

    return res.status(200).json({ ok: true, result });

  } catch (err: any) {
    console.error("Query Error:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
}