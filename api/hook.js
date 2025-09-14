// /api/hook.js  — Vercel Serverless Function (Node 18+)
// Sync Appstle loyalty data from Shopify to HubSpot.
//
// Required ENV:
//   SHOPIFY_STORE
//   SHOPIFY_TOKEN
//   SHOPIFY_WEBHOOK_SECRET
//   HUBSPOT_TOKEN
//
// Properties written in HubSpot (must exist):
//   loyalty_enabled   (boolean)
//   loyalty_points    (number)
//   loyalty_referral_link (text/URL)
//   loyalty_tier      (text)
//
// Optional query params:
//   ?debug=1         -> include raw/parsed metafield in response
//   ?skip_hmac=1     -> (testing only) bypass Shopify HMAC verification

const crypto = require("crypto");

const APPSTLE_NAMESPACE = "appstle_loyalty";
const APPSTLE_KEY = "customer_loyalty";

// Update all duplicate HubSpot contacts with same email
const UPDATE_ALL_DUPLICATES = true;

/* ------------------------ utils ------------------------ */

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

function requireEnv() {
  ["SHOPIFY_STORE", "SHOPIFY_TOKEN", "HUBSPOT_TOKEN", "SHOPIFY_WEBHOOK_SECRET"].forEach(
    (k) => {
      if (!process.env[k]) throw new Error(`Missing env var: ${k}`);
    }
  );
}

function normalizeLoyalty(raw = {}) {
  const hasData = raw && typeof raw === "object" && Object.keys(raw).length > 0;

  const enabled = raw.hasOwnProperty("enabled")
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

/* ------------------- raw body & HMAC ------------------- */

async function readRawBody(req) {
  // Try reading stream; fallback to best-effort stringify if already parsed
  try {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    if (chunks.length) return Buffer.concat(chunks).toString("utf8");
  } catch {}
  return typeof req.body === "string" ? req.body : JSON.stringify(req.body || {});
}

function verifyShopifyHmac(rawBody, headerHmac) {
  const secret = process.env.SHOPIFY_WEBHOOK_SECRET || "";
  if (!secret || !headerHmac) return false;
  const digest = crypto.createHmac("sha256", secret).update(rawBody, "utf8").digest("base64");
  try {
    return crypto.timingSafeEqual(Buffer.from(digest), Buffer.from(String(headerHmac)));
  } catch {
    return false;
  }
}

/* ------------------------- Shopify ------------------------ */

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
      // Use HubSpot's required enum values:
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
    res.setHeader("x-commit", process.env.VERCEL_GIT_COMMIT_SHA || "local");
    res.setHeader("x-deployment", process.env.VERCEL_URL || "");
    return res.status(200).send("OK");
  }

  requireEnv();

  // Read raw body for HMAC, then parse JSON
  const rawBody = await readRawBody(req);

  // Optional testing bypass
  const skipHmac = String(req.query?.skip_hmac || "") === "1";

  const headerHmac = req.headers["x-shopify-hmac-sha256"];
  if (!skipHmac && !verifyShopifyHmac(rawBody, headerHmac)) {
    return res.status(401).json({ error: "Invalid HMAC" });
  }

  const body = asJson(rawBody, {});
  const debug = String(req.query?.debug || "") === "1";
  const topic = String(req.headers["x-shopify-topic"] || "");

  // Extract email + customerId for supported topics
  let email = "";
  let customerId = null;

  if (topic.startsWith("customers/")) {
    email = (body?.email || "").toLowerCase();
    customerId = body?.id || null;
  } else if (topic === "orders/paid" || topic.startsWith("orders/")) {
    email = (body?.email || body?.customer?.email || "").toLowerCase();
    customerId = body?.customer?.id || null;
  } else {
    // topics like discounts/update usually have no customer context — ignore
    return res.status(200).json({ ignored: true, topic });
  }

  // Backfill missing parts
  if (customerId && !email) {
    const foundEmail = await getCustomerEmailById(customerId);
    if (foundEmail) email = foundEmail;
  }
  if (!email) {
    // Try via email->id if present in order payload
    const orderEmail = (body?.email || "").toLowerCase();
    if (orderEmail) {
      email = orderEmail;
      if (!customerId) customerId = await getCustomerIdByEmail(email);
    }
  }
  if (!email || !customerId) {
    return res.status(400).json({ error: "Missing email or customerId after lookup", topic });
  }

  // Fetch metafield by ID; if 404, resolve by email then retry
  let usedShopifyId = customerId;
  let meta = await getLoyaltyByCustomerId(customerId);
  if (meta.notFound) {
    const idByEmail = await getCustomerIdByEmail(email);
    if (idByEmail) {
      usedShopifyId = idByEmail;
      meta = await getLoyaltyByCustomerId(idByEmail);
    }
  }

  const rawLoyalty = meta?.data || {};
  const norm = normalizeLoyalty(rawLoyalty);
  const { enabled, points, referral, vipTier } = norm;

  const properties = {
    loyalty_enabled: enabled,
    loyalty_points: points,
    loyalty_referral_link: referral,
    loyalty_tier: vipTier,
  };

  // Upsert HubSpot contact(s)
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
      ...(debug ? { debug: { usedCustomerId: usedShopifyId, metafield: meta.raw, parsed: rawLoyalty } } : {}),
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
      ...(debug ? { debug: { usedCustomerId: usedShopifyId, metafield: meta.raw, parsed: rawLoyalty } } : {}),
    });
  }

  await Promise.all(ids.map((id) => hsPatchContact(id, properties)));
  return res.status(200).json({
    ok: true,
    duplicatesUpdated: ids,
    email,
    topic,
    loyalty: norm,
    ...(debug ? { debug: { usedCustomerId: usedShopifyId, metafield: meta.raw, parsed: rawLoyalty } } : {}),
  });
};
