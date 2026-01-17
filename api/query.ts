import type { VercelRequest, VercelResponse } from "@vercel/node";

/* ==========================================================================
   HELPER: CORS SETUP
   ========================================================================== */
function setCors(req: VercelRequest, res: VercelResponse) {
  const origin = req.headers.origin || "";
  res.setHeader("Access-Control-Allow-Origin", origin);
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, x-mc-api, x-mc-version");
}

/* ==========================================================================
   MAIN HANDLER
   ========================================================================== */
export default async function handler(req: VercelRequest, res: VercelResponse) {
  setCors(req, res);
  if (req.method === "OPTIONS") return res.status(200).end();

  if (req.method !== "POST") {
    return res.status(405).json({ ok: false, error: "Method not allowed. Use POST." });
  }

  try {
    const rawBody = req.body;
    const body = rawBody ? (typeof rawBody === "string" ? JSON.parse(rawBody) : rawBody) : {};
    
    // Destructure inputs
    const { 
        contract_version, 
        kpi, 
        groupBy, 
        max_month, 
        scope, // "MTD" or "YTD"
        filters,
        months // Optional: Support for multi-select array if passed directly
    } = body;

    if (!kpi || !max_month) {
        return res.status(400).json({ ok: false, error: "Missing required parameters: 'kpi' or 'max_month'" });
    }

    /* ----------------------------------------------------------------------
       1. TABLE RESOLUTION
       Maps the requested KPI to the specific Databricks table.
       ---------------------------------------------------------------------- */
    let table = "mbmc_actuals_volume"; // Default fallback

    switch (kpi) {
        case "volume":
            table = "mbmc_actuals_volume";
            break;
        case "revenue":
            table = "mbmc_actuals_revenue";
            break;
        case "share":
            table = "mbmc_actuals_share";
            break;
        case "adshare":
            table = "mbmc_actuals_adshare";
            break;
        case "pods":
            table = "mbmc_actuals_pods";
            break;
        case "taps":
            table = "mbmc_actuals_taps";
            break;
        case "displays":
            table = "mbmc_actuals_displays";
            break;
        case "avd":
            table = "mbmc_actuals_avd";
            break;
        default:
            // Fallback allows specialized queries if needed, or defaults to volume
            table = "mbmc_actuals_volume"; 
    }

    /* ----------------------------------------------------------------------
       2. DATE SCOPE LOGIC (MTD vs YTD)
       ---------------------------------------------------------------------- */
    // Ensure inputs are sanitized to avoid basic injection (simple alphanumeric check)
    const safeMonth = max_month.replace(/[^0-9]/g, ""); 
    
    let dateCondition = "";
    
    if (scope === "MTD") {
        // Exact month match
        dateCondition = `month = '${safeMonth}'`;
    } else {
        // YTD Logic: From Jan 01 of that year up to the max_month
        // Assumption: safeMonth format is 'YYYYMM' (e.g., '202512')
        if (safeMonth.length === 6) {
            const year = safeMonth.substring(0, 4);
            const startMonth = `${year}01`;
            dateCondition = `month >= '${startMonth}' AND month <= '${safeMonth}'`;
        } else {
            // Fallback for weird formats -> treated as MTD to prevent crashes
            dateCondition = `month = '${safeMonth}'`;
        }
    }

    /* ----------------------------------------------------------------------
       3. BUILD WHERE CLAUSE
       ---------------------------------------------------------------------- */
    const whereParts: string[] = [dateCondition];

    // --- AO (Product) Logic ---
    // If include_ao is FALSE (default), we EXCLUDE the 'AO' megabrand.
    // If include_ao is TRUE, we allow 'AO' to pass through.
    if (filters?.include_ao !== true) {
        whereParts.push(`megabrand != 'AO'`);
    }

    // --- Standard Filters ---
    if (filters?.megabrand?.length) {
        // Safe quote wrapping
        const brands = filters.megabrand.map((b: string) => `'${b.replace(/'/g, "")}'`).join(",");
        whereParts.push(`megabrand IN (${brands})`);
    }

    if (filters?.region?.length) {
        const regions = filters.region.map((r: string) => `'${r.replace(/'/g, "")}'`).join(",");
        whereParts.push(`sls_regn_cd IN (${regions})`);
    }

    if (filters?.state?.length) {
        const states = filters.state.map((s: string) => `'${s.replace(/'/g, "")}'`).join(",");
        whereParts.push(`mktng_st_cd IN (${states})`);
    }

    if (filters?.wholesaler_id?.length) {
        const wslrs = filters.wholesaler_id.map((w: string) => `'${w.replace(/'/g, "")}'`).join(",");
        whereParts.push(`wslr_nbr IN (${wslrs})`);
    }

    if (filters?.channel?.length) {
        const chans = filters.channel.map((c: string) => `'${c.replace(/'/g, "")}'`).join(",");
        whereParts.push(`channel IN (${chans})`);
    }

    // Combine all parts
    const whereClause = whereParts.filter(Boolean).join(" AND ");

    /* ----------------------------------------------------------------------
       4. CONSTRUCT SQL
       ---------------------------------------------------------------------- */
    
    // Determine Group By Column
    let groupCol = "month"; // Default for trend charts
    let orderBy = "ORDER BY 1 ASC"; // Default time sort

    switch (groupBy) {
        case "region":
            groupCol = "sls_regn_cd";
            orderBy = "ORDER BY 2 DESC"; // Sort by Value CY
            break;
        case "state":
            groupCol = "mktng_st_cd";
            orderBy = "ORDER BY 2 DESC";
            break;
        case "wholesaler":
            groupCol = "wslr_nbr";
            orderBy = "ORDER BY 2 DESC";
            break;
        case "channel":
            groupCol = "channel";
            orderBy = "ORDER BY 2 DESC";
            break;
        case "megabrand":
            groupCol = "megabrand";
            orderBy = "ORDER BY 2 DESC";
            break;
        case "total":
            groupCol = "'Total'"; // Constant string for single row aggregation
            orderBy = ""; // No sort needed for single row
            break;
        case "time":
        default:
            groupCol = "month";
            orderBy = "ORDER BY 1 ASC";
            break;
    }

    // Aggregation Function
    // Shares and Averages cannot be summed, they must be Averaged.
    // Counts (Volume, Revenue) must be Summed.
    const NON_ADDITIVE_KPIS = ["share", "adshare", "avd"];
    const aggFunc = NON_ADDITIVE_KPIS.includes(kpi) ? "AVG" : "SUM";

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

    /* ----------------------------------------------------------------------
       5. EXECUTE QUERY (DATABRICKS)
       ---------------------------------------------------------------------- */
    const host = process.env.DATABRICKS_HOST;
    const token = process.env.DATABRICKS_TOKEN;
    const warehouseId = process.env.WAREHOUSE_ID;

    // 1. Submit Query
    const submitRes = await fetch(`${host}/api/2.0/sql/statements`, {
      method: "POST",
      headers: { 
          Authorization: `Bearer ${token}`, 
          "Content-Type": "application/json" 
      },
      body: JSON.stringify({ 
          statement: sql, 
          warehouse_id: warehouseId, 
          wait_timeout: "35s" // Wait up to 35s for synchronous result
      }),
    });

    const submitJson = await submitRes.json();

    // 2. Handle Result or Polling
    let result = submitJson.result;
    const statementId = submitJson.statement_id;

    // If query is taking longer than wait_timeout, we poll once (Basic Logic)
    // Production recommendation: Use a robust polling loop here if queries are heavy.
    if (!result && statementId) {
         // Short wait before checking
         await new Promise(r => setTimeout(r, 1000));
         
         const pollRes = await fetch(`${host}/api/2.0/sql/statements/${statementId}`, { 
             headers: { Authorization: `Bearer ${token}` } 
         });
         const pollJson = await pollRes.json();
         
         if (pollJson.status?.state === "SUCCEEDED") {
             result = pollJson.result;
         } else if (pollJson.status?.state === "FAILED") {
             throw new Error(pollJson.status.error?.message || "Query failed during execution");
         }
    }

    if (!result) {
        // If still pending after poll, return empty or error
        // Ideally, frontend handles retries, but we'll throw for now.
        return res.status(202).json({ ok: false, error: "Query pending", statement_id: statementId });
    }

    return res.status(200).json({ ok: true, result });

  } catch (err: any) {
    console.error("[API Error] Query Failed:", err);
    return res.status(500).json({ ok: false, error: err.message || "Internal Server Error" });
  }
}