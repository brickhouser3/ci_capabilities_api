import type { VercelRequest, VercelResponse } from "@vercel/node";

export default async function handler(
  req: VercelRequest,
  res: VercelResponse
) {
  if (req.method === "OPTIONS") {
    return res.status(200).end();
  }

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

    const data = await response.json();
    return res.status(200).json(data);
  } catch (err: any) {
    return res.status(500).json({
      error: "Databricks query failed",
      details: err.message,
    });
  }
}
