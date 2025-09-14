// Vercel Serverless Function (Node 18+)
// Receives Shopify webhooks, pulls Appstle loyalty data, updates HubSpot.
//
// Required env vars (Production):
//   SHOPIFY_STORE   e.g. "epicmarks.myshopify.com"
//   SHOPIFY_TOKEN   e.g. "shpat_xxx" (needs read_customers + read_metafields)
//   HUBSPOT_TOKEN   e.g. "pat_xxx"   (needs contacts write)
//
// Optional: turn on HMAC later (verify X-Shopify-Hmac-Sha256).

const APPSTLE_NAMESPACE = "appstle_loyalty";
const APPSTLE_KEY = "customer_loyalty";

// If HubSpot has duplicate emails and you'd prefer to update only one,
// set this false to update only the most recently created contact.
const UPDATE_ALL_DUPLICATES = true;

/* -------------------------- small helpers -------------------------- */

const asJson = (input, fallback = {}) => {
  try {
    if (typeof input === "string") return JSON.parse(input);
    if (typeof input === "object" && input !== null) return input;
    return fallback;
  } catch {
    return fallback;
  }
};

const bool = v => !!(v === true || v === "true" || v === 1 || v === "1");

function normalizeLoyalty(raw = {}) {
  const enabled =
    bool(raw.enabled ?? raw.active ?? raw.is_enabled ?? false);
  const points = Number(raw.points ?? raw.point_balance ?? 0) || 0;
  const referral = String(raw.referral_link ?? raw.referral ?? "");
  return { enabled, points, referral };
}

/* ---------------------------- Shopify ----------------------------- */

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
  if (!mf?.value) return { data: {} };
  const parsed = typeof mf.value === "string" ? asJson(mf.value, {}) : mf.value;
  return { data: parsed || {} };
}

async function getCustomerIdByEmail(email) {
  const q = encodeURIComponent(`email:${email}`);
  const r = await shopifyGET(`/admin/api/2024-07/customers/search.json?query=${q}`);
  if (!r.ok) return null;
  const j = await r.json();
  return j?.customers?.[0]?.id || null;
}

/* ---------------------------- HubSpot ----------------------------- */

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
      // HubSpot requires ASCENDING / DESCENDING
      sorts: [{ propertyName: "createdate", direction: "DESCENDING" }],
      limit: 10,
    }),
  });
  if (!r.ok) throw new Error(`HubSpot search failed: ${r.status} ${await r.text()}`);
  const j = await r.json();
  return (j?.results || []).map(x => x.id);
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

/* --------------------------- main handler -------------------------- */

module.exports = async (req, res) => {
  // Accept POST from Shopify; allow GET to show "OK" for uptime checks.
  if (req.method !== "POST") {
    return res.status(200).send("OK");
  }

  try {
    const topic = String(req.headers["x-shopify-topic"] || "");
    const body = asJson(req.body);

    // Extract email + customerId depending on the topic
    let email = "";
    let customerId = null;

    if (topic.startsWith("customers/")) {
      email = (body?.email || "").toLowerCase();
      customerId = body?.id || null;
    } else if (topic.startsWith("orders/")) {
      email = (body?.customer?.email || "").toLowerCase();
      customerId = body?.customer?.id || null;
    } else {
      // Ignore unrelated topics gracefully
      return res.status(200).json({ ignored: true, topic });
    }

    // If only ID provided, fetch email from Shopify.
    if (customerId && !email) {
      const foundEmail = await getCustomerEmailById(customerId);
      if (foundEmail) email = foundEmail;
    }
    if (!email || !customerId) {
      return res.status(400).json({ error: "Missing email or customerId after lookup", topic });
    }

    // Try to read loyalty by ID. If 404, try searching by email → ID → fetch again.
    let loyaltyRaw = {};
    let meta = await getLoyaltyByCustomerId(customerId);
    if (meta.notFound) {
      const idByEmail = await getCustomerIdByEmail(email);
      if (idByEmail) {
        meta = await getLoyaltyByCustomerId(idByEmail);
      }
    }
    if (meta?.data) loyaltyRaw = meta.data;

    const { enabled, points, referral } = normalizeLoyalty(loyaltyRaw);

    // Resolve HubSpot contact(s) by email and update
    const properties = {
      loyalty_enabled: enabled,
      loyalty_points: points,
      loyalty_referral_link: referral,
    };

    let ids = await hsSearchContactIdsByEmail(email);

    if (ids.length === 0) {
      const newId = await hsCreateContact(email);
      await hsPatchContact(newId, properties);
      return res.status(200).json({
        ok: true,
        created: newId,
        email,
        topic,
        loyalty: { enabled, points, referral },
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
        loyalty: { enabled, points, referral },
      });
    }

    // duplicates & UPDATE_ALL_DUPLICATES = true → patch all
    await Promise.all(ids.map(id => hsPatchContact(id, properties)));

    return res.status(200).json({
      ok: true,
      duplicatesUpdated: ids,
      email,
      topic,
      loyalty: { enabled, points, referral },
    });
  } catch (err) {
    console.error("hook error:", err);
    return res.status(500).json({ error: String(err?.message || err) });
  }
};
