// /api/hook.js  — Vercel Serverless Function (Node 18+)
//
// Reads Appstle loyalty metafield from Shopify and upserts contact fields in HubSpot.
//
// Required ENV:
//   SHOPIFY_STORE   e.g. "yourstore.myshopify.com"
//   SHOPIFY_TOKEN   e.g. "shpat_xxx" (read_customers + read_metafields)
//   HUBSPOT_TOKEN   e.g. "pat_xxx"   (contacts write)
//
// Optional debug: append ?debug=1 to response with raw/parsed metafield

const APPSTLE_NAMESPACE = "appstle_loyalty";
const APPSTLE_KEY = "customer_loyalty";

// Update all duplicate contacts (same email) in HubSpot (true) or just the newest (false)
const UPDATE_ALL_DUPLICATES = true;

/* ------------------------ small utils ------------------------ */

const asJson = (input, fallback = {}) => {
  try {
    if (typeof input === "string") return JSON.parse(input);
    if (typeof input === "object" && input !== null) return input;
    return fallback;
  } catch {
    return fallback;
  }
};

const bool = (v) => !!(v === true || v === "true" || v === 1 || v === "1");
const toNum = (v, d = 0) => {
  if (v === undefined || v === null) return d;
  const n = Number(typeof v === "string" ? v.replace(/,/g, "") : v);
  return Number.isFinite(n) ? n : d;
};

/**
 * Normalize Appstle payloads into a simple shape:
 * { enabled: boolean, points: number, referral: string, vipTier: string }
 *
 * Preferred keys based on your data:
 * - availablePoints         -> points
 * - referralLink            -> referral
 * - currentVipTier          -> vipTier
 * - customerStatus:"ACTIVE" -> enabled (if no explicit enabled flag)
 *
 * Fallbacks still supported: points, point_balance, pointBalance, balance, referral_link, referralUrl, referral
 */
function normalizeLoyalty(raw = {}) {
  const hasData = raw && typeof raw === "object" && Object.keys(raw).length > 0;

  const enabled =
    raw.hasOwnProperty("enabled")
      ? bool(raw.enabled)
      : (hasData || raw.customerStatus === "ACTIVE");

  const pointsCandidate =
    raw.availablePoints ??
    raw.pointBalance ??
    raw.pointsBalance ??
    raw.balance ??
    raw.points ??
    raw.point_balance ??
    (raw.creditedPoints !== undefined && raw.spentAmount !== undefined
      ? toNum(raw.creditedPoints) - toNum(raw.spentAmount)
      : undefined);

  const points = toNum(pointsCandidate, 0);

  const referral =
    raw.referralLink ??
    raw.referral_url ??
    raw.referralUrl ??
    raw.referral_link ??
    raw.referral ??
    "";

  const vipTier =
    raw.currentVipTier ??
    raw.currentVip ??
    raw.vipTier ??
    "";

  return { enabled, points, referral, vipTier };
}

/* ------------------------- Shopify ------------------------ */

function requireEnv() {
  ["SHOPIFY_STORE", "SHOPIFY_TOKEN", "HUBSPOT_TOKEN"].forEach((k) => {
    if (!process.env[k]) throw new Error(`Missing env var: ${k}`);
  });
}

async function shopifyGET(path) {
  const url = `https://${process.env.SHOPIFY_STORE}${path}`;
  const res = await fetch(url, {
    headers: {
      "X-Shopify-Access-Token": process.env.SHOPIFY_TOKEN,
      "Content-Type": "application/json",
    },
  });
  return res;
}

async function getCustomerEmailById(customerId) {
  const r = await shopifyGET(`/admin/api/2024-07/customers/${customerId}.json`);
  if (!r.ok) return null;
  const j = await r.json();
  return (j?.customer?.email || "").toLowerCase() || null;
}

async function getCustomerIdByEmail(email) {
  const q = encodeURIComponent(`email:${email}`);
  const r = await shopifyGET(`/admin/api/2024-07/customers/search.json?query=${q}`);
  if (!r.ok) return null;
  const j = await r.json();
  return j?.customers?.[0]?.id || null;
}

async function getLoyaltyByCustomerId(customerId) {
  const r = await shopifyGET(
    `/admin/api/2024-07/customers/${customerId}/metafields.json` +
      `?namespace=${APPSTLE_NAMESPACE}&key=${APPSTLE_KEY}`
  );
  if (r.status === 404) return { notFound: true };
  if (!r.ok) {
    const t = await r.text();
    throw new Error(`Shopify metafield fetch failed: ${r.status} ${t}`);
  }
  const j = await r.json();
  const mf = j?.metafields?.[0];
  if (!mf?.value) return { data: {}, raw: null };
  const parsed = typeof mf.value === "string" ? asJson(mf.value, {}) : mf.value;
  return { data: parsed || {}, raw: mf.value };
}

/* ------------------------- HubSpot ------------------------ */

async function hsSearchContactIdsByEmail(email) {
  const r = await fetch("https://api.hubapi.com/crm/v3/objects/contacts/search", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${process.env.HUBSPOT_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      filterGroups: [{ filters: [{ propertyName: "email", operator: "EQ", value: email }] }],
      properties: ["email", "createdate"],
      // HubSpot requires "DESCENDING" / "ASCENDING" — not "DESC"/"ASC"
      sorts: [{ propertyName: "createdate", direction: "DESCENDING" }],
      limit: 20,
    }),
  });
  if (!r.ok) throw new Error(`HubSpot search failed: ${r.status} ${await r.text()}`);
  const j = await r.json();
  return (j?.results || []).map((x) => x.id);
}

async function hsCreateContact(email) {
  const r = await fetch("https://api.hubapi.com/crm/v3/objects/contacts", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${process.env.HUBSPOT_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ properties: { email } }),
  });
  if (!r.ok) throw new Error(`HubSpot create failed: ${r.status} ${await r.text()}`);
  const j = await r.json();
  return j.id;
}

async function hsPatchContact(id, properties) {
  const r = await fetch(`https://api.hubapi.com/crm/v3/objects/contacts/${id}`, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${process.env.HUBSPOT_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ properties }),
  });
  if (!r.ok) throw new Error(`HubSpot patch failed: ${r.status} ${await r.text()}`);
}

/* --------------------------- handler ---------------------- */

module.exports = async (req, res) => {
  if (req.method !== "POST") {
    // Useful to confirm which build you’re hitting
    res.setHeader("x-commit", process.env.VERCEL_GIT_COMMIT_SHA || "local");
    res.setHeader("x-deployment", process.env.VERCEL_URL || "");
    return res.status(200).send("OK");
  }

  const debug = String(req.query?.debug || "") === "1";

  try {
    requireEnv();

    const topic = String(req.headers["x-shopify-topic"] || "");
    const body = asJson(req.body);

    // Extract email + customerId from webhooks we care about
    let email = "";
    let customerId = null;

    if (topic.startsWith("customers/")) {
      email = (body?.email || "").toLowerCase();
      customerId = body?.id || null;
    } else if (topic.startsWith("orders/")) {
      email = (body?.customer?.email || "").toLowerCase();
      customerId = body?.customer?.id || null;
    } else {
      return res.status(200).json({ ignored: true, topic });
    }

    // Backfill missing email if only ID was sent
    if (customerId && !email) {
      const foundEmail = await getCustomerEmailById(customerId);
      if (foundEmail) email = foundEmail;
    }
    if (!email || !customerId) {
      return res.status(400).json({ error: "Missing email or customerId after lookup", topic });
    }

    // Try metafield by ID → if 404, try via email → id → metafield again
    let loyaltyRaw = {};
    let meta = await getLoyaltyByCustomerId(customerId);
    let usedShopifyId = customerId;

    if (meta.notFound) {
      const idByEmail = await getCustomerIdByEmail(email);
      if (idByEmail) {
        usedShopifyId = idByEmail;
        meta = await getLoyaltyByCustomerId(idByEmail);
      }
    }
    if (meta?.data) loyaltyRaw = meta.data;

    const norm = normalizeLoyalty(loyaltyRaw);
    const { enabled, points, referral, vipTier } = norm;

    // HubSpot properties — make sure they exist in your portal
    const properties = {
      loyalty_enabled: enabled,
      loyalty_points: points,
      loyalty_referral_link: referral,
      // Uncomment if you add a property for VIP tier:
      // loyalty_tier: vipTier,
    };

    // Upsert by email
    let ids = await hsSearchContactIdsByEmail(email);

    if (ids.length === 0) {
      const newId = await hsCreateContact(email);
      await hsPatchContact(newId, properties);
      return res.status(200).json({
        ok: true,
        created: newId,
        email,
        topic,
        loyalty: norm,
        ...(debug ? { debug: { usedCustomerId: usedShopifyId, metafield: meta.raw, parsed: loyaltyRaw } } : {}),
      });
    }

    if (ids.length === 1 || !UPDATE_ALL_DUPLICATES) {
      const targetId = ids[0];
      await hsPatchContact(targetId, properties);
      return res.status(200).json({
        ok: true,
        updated: targetId,
        email,
        duplicates: ids.length > 1 ? ids : undefined,
        topic,
        loyalty: norm,
        ...(debug ? { debug: { usedCustomerId: usedShopifyId, metafield: meta.raw, parsed: loyaltyRaw } } : {}),
      });
    }

    // Update all duplicates
    await Promise.all(ids.map((id) => hsPatchContact(id, properties)));
    return res.status(200).json({
      ok: true,
      duplicatesUpdated: ids,
      email,
      topic,
      loyalty: norm,
      ...(debug ? { debug: { usedCustomerId: usedShopifyId, metafield: meta.raw, parsed: loyaltyRaw } } : {}),
    });
  } catch (err) {
    console.error("hook error:", err);
    return res.status(500).json({ error: String(err?.message || err) });
  }
};
