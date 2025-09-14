// Vercel Serverless Function (Node 18)
// Receives Shopify webhooks, fetches Appstle loyalty metafield, updates HubSpot contact

const APPSTLE_NAMESPACE = "appstle_loyalty";
const APPSTLE_KEY = "customer_loyalty";

// Optional HMAC later:
// const crypto = require("crypto"); // then verify X-Shopify-Hmac-Sha256

module.exports = async (req, res) => {
  if (req.method !== "POST") {
    return res.status(200).send("OK");
  }

  try {
    // Shopify sends JSON; Vercel usually parses it. Fallback if needed:
    const body = typeof req.body === "object" ? req.body : JSON.parse(req.body || "{}");
    const topic = (req.headers["x-shopify-topic"] || "").toString();

    // Pull email + customerId depending on topic
    let email = "";
    let customerId = null;
    if (topic.startsWith("customers/")) {
      email = (body.email || "").toLowerCase();
      customerId = body.id;
    } else if (topic.startsWith("orders/")) {
      email = (body?.customer?.email || "").toLowerCase();
      customerId = body?.customer?.id;
    } else {
      // ignore unrelated topics without erroring
      return res.status(200).json({ ignored: true, topic });
    }
    if (!email || !customerId) {
      return res.status(400).json({ error: "Missing email or customerId", topic });
    }

    // Get Appstle loyalty metafield from Shopify
    const shopifyUrl =
      `https://${process.env.SHOPIFY_STORE}/admin/api/2024-07/customers/${customerId}/metafields.json` +
      `?namespace=${APPSTLE_NAMESPACE}&key=${APPSTLE_KEY}`;

    const mfRes = await fetch(shopifyUrl, {
      headers: {
        "X-Shopify-Access-Token": process.env.SHOPIFY_TOKEN,
        "Content-Type": "application/json"
      }
    });
    if (!mfRes.ok) {
      const t = await mfRes.text();
      throw new Error(`Shopify metafield fetch failed: ${mfRes.status} ${t}`);
    }
    const mfJson = await mfRes.json();
    const mf = mfJson.metafields?.[0];
    let loyalty = {};
    if (mf?.value) {
      try { loyalty = typeof mf.value === "string" ? JSON.parse(mf.value) : mf.value; } catch {}
    }

    // Normalize properties
    const enabled = !!(loyalty?.enabled ?? loyalty?.active ?? loyalty?.is_enabled);
    const points = Number(loyalty?.points ?? loyalty?.point_balance ?? 0);
    const referral = String(loyalty?.referral_link ?? "");

    // Batch update HubSpot contact by email
    const hsBody = {
      inputs: [{
        id: email,
        idProperty: "email",
        properties: {
          loyalty_enabled: enabled,          // checkbox
          loyalty_points: isNaN(points) ? 0 : points, // number
          loyalty_referral_link: referral    // text/url
        }
      }]
    };

    const hsRes = await fetch("https://api.hubapi.com/crm/v3/objects/contacts/batch/update", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${process.env.HUBSPOT_TOKEN}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify(hsBody)
    });
    if (!hsRes.ok) {
      const t = await hsRes.text();
      throw new Error(`HubSpot update failed: ${hsRes.status} ${t}`);
    }

    return res.status(200).json({ ok: true, topic, email, customerId });
  } catch (err) {
    return res.status(500).json({ error: String(err?.message || err) });
  }
};
