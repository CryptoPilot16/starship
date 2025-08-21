// server.js — Starship (Solana realtime, 4h windows, 68h cap) — SERVER IS SOURCE OF TRUTH FOR TIME
import express from "express";
import path from "path";
import { fileURLToPath } from "url";
import dotenv from "dotenv";
import cors from "cors";

dotenv.config();

const TOKEN = process.env.BITQUERY_TOKEN;
const PORT = process.env.PORT || 8080;
const BITQUERY_EAP = "https://streaming.bitquery.io/eap";
const WSOL_MINT = "So11111111111111111111111111111111111111112";

// === WINDOW LIMITS ===
const REALTIME_HOURS = 68;     // reduced from 72 to 68
const MAX_WINDOW_HOURS = 4;
const DEFAULT_LIMIT_PER_WINDOW = 100;
const EPS_MS = 1500;           // small tolerance for boundary drift

if (!TOKEN) {
  console.error("Missing BITQUERY_TOKEN in .env");
  process.exit(1);
}

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

const SOLANA_RT_QUERY = `
  query SolanaTradesRealtime(
    $token: String!, $since: DateTime!, $till: DateTime!, $limit: Int, $offset: Int
  ) {
    Solana(dataset: realtime) {
      DEXTradeByTokens(
        orderBy: { ascending: Block_Time }
        limit: { count: $limit, offset: $offset }
        where: {
          Block: { Time: { after: $since, before: $till } }
          Transaction: { Result: { Success: true } }
          Trade: {
            Currency: { MintAddress: { is: $token } }
            Side: { Type: { is: buy } }
            Price: { gt: 0 }
          }
        }
      ) {
        Block { Time }
        Transaction { Signature }
        Trade {
          Price
          Account { Owner }
          Side {
            Type
            Account { Address Owner }
            Currency { MintAddress }
            Amount
          }
        }
      }
    }
  }
`;

// ---------- server time helpers (SOURCE OF TRUTH) ----------
function serverNowRoundedHourUTC() {
  const now = new Date();
  now.setUTCMinutes(0, 0, 0); // top of the hour UTC
  return now;
}
function makeWindowsFromServerNow() {
  const now = serverNowRoundedHourUTC();                // e.g., 12:00:00Z
  const num = REALTIME_HOURS / MAX_WINDOW_HOURS;        // 17 windows
  const out = [];
  for (let i = 0; i < num; i++) {
    const till = new Date(now.getTime() - i * MAX_WINDOW_HOURS * 3600 * 1000);
    const since = new Date(till.getTime() - MAX_WINDOW_HOURS * 3600 * 1000);
    out.push({
      sinceISO: since.toISOString(), // use strict ISO from server
      tillISO: till.toISOString(),
    });
  }
  return out; // newest first
}

// Publish canonical windows + bounds so the client never guesses
app.get("/api/candles", (_req, res) => {
  const now = serverNowRoundedHourUTC();
  const minSince = new Date(now.getTime() - REALTIME_HOURS * 3600 * 1000);
  const windows = makeWindowsFromServerNow();
  res.json({
    nowISO: now.toISOString(),
    bounds: { minSince: minSince.toISOString(), maxTill: now.toISOString() },
    hours: { realtime: REALTIME_HOURS, perWindow: MAX_WINDOW_HOURS },
    windows, // [{sinceISO,tillISO}] newest first
  });
});

// quick time ping if you ever need it
app.get("/api/time", (_req, res) => {
  const now = new Date();
  res.json({
    nowISO: now.toISOString(),
    roundedHourISO: serverNowRoundedHourUTC().toISOString(),
  });
});

function validateWindow({ since, till }) {
  const now = serverNowRoundedHourUTC(); // SERVER CLOCK ONLY
  const minSince = new Date(now.getTime() - REALTIME_HOURS * 3600 * 1000);

  const s = new Date(since);
  const t = new Date(till);
  if (isNaN(s) || isNaN(t)) return { ok: false, reason: "invalid date" };
  if (s >= t) return { ok: false, reason: "since >= till" };

  // tolerate tiny drift at both edges
  if (s.getTime() < minSince.getTime() - EPS_MS || t.getTime() > now.getTime() + EPS_MS) {
    return { ok: false, reason: "outside 68h" };
  }

  const hours = (t.getTime() - s.getTime()) / 3600000;
  if (hours <= 0 || hours > MAX_WINDOW_HOURS + 1e-6) {
    return { ok: false, reason: "duration > 4h" };
  }
  return { ok: true };
}

async function runWindow({ token, since, till, limit }) {
  const resp = await fetch(BITQUERY_EAP, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${TOKEN}`,
    },
    body: JSON.stringify({
      query: SOLANA_RT_QUERY,
      variables: { token, since, till, limit: Number(limit), offset: 0 },
    }),
  });

  const raw = await resp.text();
  let json;
  try { json = JSON.parse(raw); } catch { json = { raw }; }
  if (!resp.ok || json.errors) throw new Error(JSON.stringify(json.errors || json));

  const rows = json?.data?.Solana?.DEXTradeByTokens || [];
  const flat = [];
  for (const r of rows) {
    const time = r?.Block?.Time;
    const signature = r?.Transaction?.Signature || "";
    const owner = r?.Trade?.Account?.Owner;
    const price = Number(r?.Trade?.Price ?? NaN);

    const sidesRaw = Array.isArray(r?.Trade?.Side) ? r.Trade.Side : (r?.Trade?.Side ? [r.Trade?.Side] : []);
    const buySides = sidesRaw.filter(s => (s?.Type || "").toLowerCase() === "buy");

    const walletsFromSides = buySides.map(s => s?.Account?.Address || s?.Account?.Owner).filter(Boolean);
    const solAmount = buySides
      .filter(s => (s?.Currency?.MintAddress || "").toLowerCase() === WSOL_MINT.toLowerCase())
      .map(s => Number(s?.Amount) || 0)
      .reduce((a, b) => a + b, 0);

    const wallets = [...new Set([owner, ...walletsFromSides].filter(Boolean))];
    if (wallets.length === 0) {
      flat.push({ time, wallet: "", signature, solAmount, price, dataset: "realtime" });
    } else {
      for (const w of wallets) {
        flat.push({ time, wallet: w, signature, solAmount, price, dataset: "realtime" });
      }
    }
  }
  return flat;
}

// POST /api/trades  body: { token, windows:[{since,till}], limitPerWindow? }
app.post("/api/trades", async (req, res) => {
  try {
    const { token, windows, limitPerWindow = DEFAULT_LIMIT_PER_WINDOW } = req.body || {};
    if (!token) return res.status(400).json({ error: "Missing fields", details: "Provide: token" });
    if (!Array.isArray(windows) || windows.length === 0) {
      return res.status(400).json({ error: "Missing fields", details: "Provide: windows[]" });
    }

    for (const w of windows) {
      const v = validateWindow(w);
      if (!v.ok) {
        const now = serverNowRoundedHourUTC();
        const minSince = new Date(now.getTime() - REALTIME_HOURS * 3600 * 1000).toISOString();
        return res.status(400).json({
          error: "Each 4h window must be within the last 68 hours and have since < till.",
          details: v.reason,
          bounds: { minSince, maxTill: now.toISOString() }
        });
      }
    }

    const all = [];
    for (const w of windows) {
      const rows = await runWindow({ token, since: w.since, till: w.till, limit: limitPerWindow });
      all.push(...rows);
    }

    // dedupe + sort
    const seen = new Set();
    const merged = [];
    for (const r of all.sort((a,b)=> new Date(a.time) - new Date(b.time))) {
      const k = `${r.signature}|${r.wallet}|${r.time}`;
      if (seen.has(k)) continue;
      seen.add(k);
      merged.push(r);
    }

    res.json({ rows: merged, dataset: "realtime", maxLookbackHours: REALTIME_HOURS });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: "Server error", details: String(e) });
  }
});

// SPA fallback
app.use((req, res, next) => {
  if (req.path.startsWith("/api/")) return next();
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

app.listen(PORT, () => {
  console.log(`Starship realtime (4h windows, 68h cap) → http://localhost:${PORT}`);
});
