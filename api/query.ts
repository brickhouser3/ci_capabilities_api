import type { VercelRequest, VercelResponse } from "@vercel/node";

/**
 * Explicit CORS allowlist
 * Keep this tight and intentional
 */
const ALLOWED_ORIGINS = [
  "http://localhost:3000",
  "http://localhost:3003",
  "http://localhost:3004",
  "https://brickhouser3.github.io",
];

/**
 * Apply CORS headers safely
 */
function applyCors(req: VercelRequest, res: VercelResponse) {
  const origin = req.headers.origin;

  if (origin && ALLOWED_ORIGINS.includes(origin)) {
    res.setHeader("Access-Control-Allow-Origin", origin);
  }

  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  res.setHeader("Access-Control-Max-Age", "86400");
  res.setHeader("Vary", "Origin");
}

export default async function handler(
  req: VercelRequest,
  res: VercelResponse
) {
  // 🔐 Apply CORS on every request
  applyCors(req, res);

  // ✅ Handle browser preflight
  if (req.method === "OPTIONS") {
    return res.status(200).end();
  }

  // 🔒 Only POST allowed for real requests
  if (req.method !== "POST") {
    return res.status(405).json({ error: "Method not allowed" });
  }

  try {
    const response = await fetch(
      `${process.env.DATABRICKS_HOST}/api/2.0/sql/statements`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${process.env.DATABRICKS_TOKEN}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          statement:
            "select max(cal_dt) as max_cal_dt from vip.bir.bir_weekly_ind",
          warehouse_id: process.env.WAREHOUSE_ID,
        }),
      }
    );

    if (!response.ok) {
      const text = await response.text();
      throw new Error(`Databricks error: ${text}`);
    }

    const data = await response.json();

    return res.status(200).json(data);
  } catch (err: any) {
    console.error("❌ Databricks query failed:", err);

    return res.status(500).json({
      error: "Databricks query failed",
      details: err.message,
    });
  }
}
