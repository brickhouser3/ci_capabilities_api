export const config = {
  runtime: "nodejs",
};

import type { VercelRequest, VercelResponse } from "@vercel/node";

function setCors(res: VercelResponse) {
  res.setHeader("Access-Control-Allow-Origin", "https://brickhouser3.github.io");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS, GET");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  res.setHeader("Access-Control-Max-Age", "86400");
  res.setHeader("Vary", "Origin");
  res.setHeader("Cache-Control", "no-store");
}

export default async function handler(
  req: VercelRequest,
  res: VercelResponse
) {
  setCors(res);

  // ✅ Proper preflight
  if (req.method === "OPTIONS") {
    return res.status(204).end();
  }

  if (req.method === "GET") {
    return res.status(200).json({ ok: true });
  }

  if (req.method !== "POST") {
    return res.status(405).json({ error: "Method not allowed" });
  }

  try {
    const body =
      typeof req.body === "string" ? JSON.parse(req.body) : req.body;

    const dbxRes = await fetch(
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

    const data = await dbxRes.json();
    return res.status(200).json(data);
  } catch (err: any) {
    return res.status(500).json({
      error: "Databricks query failed",
      details: err?.message ?? "unknown",
    });
  }
}
