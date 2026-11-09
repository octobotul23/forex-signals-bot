/* FX Signals web app: vanilla JS single-page app (hash routes), live updates over WebSocket. */
(() => {
"use strict";

// ── state & helpers ─────────────────────────────────────────
const S = {
  lang: (() => {
    let saved = null;
    try { saved = localStorage.getItem("fx_lang"); } catch { /* storage blocked */ }
    return saved || ((navigator.language || "").startsWith("ro") ? "ro" : "en");
  })(),
  info: null, me: null, settings: null, symbols: [], modesEnabled: {},
  burstApp: { enabled: true, max_trades: 40 }, burst: null, burstSeen: {}, testSig: null, learnKick: 0, stVariant: null,
  ws: null, wsState: "off", wsRetry: 0, connector: { online: false }, log: [], page: null, pageCleanup: null,
};
const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => [...root.querySelectorAll(sel)];
const esc = (v) => String(v ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const safeUrl = (u) => (/^https?:\/\//i.test(u || "") ? u : "#");
function t(key, vars) {
  let s = (I18N[S.lang] && I18N[S.lang][key]) ?? I18N.en[key] ?? key;
  if (vars) for (const [k, v] of Object.entries(vars)) s = s.replaceAll(`{${k}}`, v);
  return s;
}
// a price's decimals, as bot/symbols.py price_digits: 2 for gold, 3 for JPY pairs, else 5
const priceDigits = (sym) => { const s = String(sym || "").toUpperCase(); return s.includes("XAU") ? 2 : s.includes("JPY") ? 3 : 5; };
const fmtP = (sym, v) => (v == null ? "—" : Number(v).toFixed(priceDigits(sym)));
const fmtN = (v, d = 1) => (v == null ? "—" : Number(v).toLocaleString(S.lang === "ro" ? "ro-RO" : "en-US", { minimumFractionDigits: d, maximumFractionDigits: d }));
const fmtMoney = (v, cur) => (v == null ? "—" : Number(v).toLocaleString(S.lang === "ro" ? "ro-RO" : "en-US", { style: "currency", currency: cur || "USD", maximumFractionDigits: 2 }));
const signed = (v, d = 1) => (v == null ? "—" : (v > 0 ? "+" : "") + fmtN(v, d));
const cls = (v) => (v > 0 ? "up" : v < 0 ? "down" : "muted");
const locale = () => (S.lang === "ro" ? "ro-RO" : "en-GB");
const fmtTime = (iso) => (iso ? new Date(iso).toLocaleTimeString(locale(), { hour: "2-digit", minute: "2-digit" }) : "—");
const fmtDate = (iso) => (iso ? new Date(iso).toLocaleDateString(locale(), { day: "numeric", month: "short", year: "numeric" }) : "—");
const fmtDT = (iso) => (iso ? new Date(iso).toLocaleString(locale(), { day: "numeric", month: "short", hour: "2-digit", minute: "2-digit" }) : "—");
function ago(iso) {
  if (!iso) return "—";
  const s = (Date.now() - new Date(iso).getTime()) / 1000;
  if (s < -60) { const m = Math.round(-s / 60); return m < 90 ? t("in_m", { n: m }) : t("in_h", { n: Math.round(m / 60) }); }
  if (s < 60) return t("ago_s");
  if (s < 3600) return t("ago_m", { n: Math.floor(s / 60) });
  if (s < 86400) return t("ago_h", { n: Math.floor(s / 3600) });
  return t("ago_d", { n: Math.floor(s / 86400) });
}

class ApiError extends Error { constructor(status, code, extra) { super(code); this.status = status; this.code = code; this.extra = extra; } }
async function api(path, { method = "GET", body, headers = {} } = {}) {
  const opts = { method, credentials: "same-origin", headers: { ...headers } };
  if (method !== "GET") { opts.headers["X-FX"] = "1"; }
  if (body !== undefined) { opts.headers["Content-Type"] = "application/json"; opts.body = JSON.stringify(body); }
  let res;
  try { res = await fetch(path, opts); } catch { throw new ApiError(0, "network"); }
  let data = null;
  try { data = await res.json(); } catch { /* empty body */ }
  if (!res.ok) {
    let d = data && data.detail, code = typeof d === "string" ? d : (d && d.code) || (Array.isArray(d) ? "invalid_input" : "error");
    if (res.status === 401 && code === "not_logged_in" && S.me) { S.me = null; closeWS(); boot(); }
    throw new ApiError(res.status, code, d);
  }
  return data;
}
function errText(e) {
  const code = e && e.code;
  if (code && I18N.en["e_" + code]) return t("e_" + code) + (e.extra && e.extra.reason ? ` (${e.extra.reason})` : "");
  if (code === "network") return "Network error";
  return (e && e.status === 503 && e.message) ? e.message : t("error");
}
function toast(msg, kind = "") {
  const el = document.createElement("div");
  el.className = "toast " + kind;
  el.innerHTML = msg;
  $("#toasts").append(el);
  setTimeout(() => { el.style.opacity = "0"; el.style.transition = "opacity .4s"; setTimeout(() => el.remove(), 400); }, 4200);
}
function modal({ title, body = "", confirm = t("confirm"), cancel = t("cancel"), danger = false, onOpen } = {}) {
  return new Promise((resolve) => {
    const back = document.createElement("div");
    back.className = "modal-back";
    back.innerHTML = `<div class="modal" role="dialog" aria-modal="true"><h2>${esc(title)}</h2><div class="mbody">${body}</div>
      <div class="actions">${cancel ? `<button class="btn" data-m="no">${esc(cancel)}</button>` : ""}
      ${confirm ? `<button class="btn ${danger ? "danger" : "gold"}" data-m="yes">${esc(confirm)}</button>` : ""}</div></div>`;
    const close = (v) => { back.remove(); document.removeEventListener("keydown", key); resolve(v); };
    const key = (e) => { if (e.key === "Escape") close(null); };
    back.addEventListener("click", (e) => {
      if (e.target === back || e.target.dataset.m === "no") close(null);
      if (e.target.dataset.m === "yes") close(back.querySelector(".modal"));
    });
    document.addEventListener("keydown", key);
    document.body.append(back);
    if (onOpen) onOpen(back.querySelector(".modal"));
    const first = back.querySelector("input,textarea,select"); if (first) first.focus();
  });
}
function setLang(l) {
  S.lang = l;
  try { localStorage.setItem("fx_lang", l); } catch { /* private mode */ }
  document.documentElement.lang = l;
  if (S.me) { api("/api/me/profile", { method: "PUT", body: { lang: l } }).catch(() => {}); renderShell(); route(); } else boot();
}

// ── icons (1.5px stroke) ────────────────────────────────────
const ICON = {
  dash: '<path d="M4 13h6V4H4zM14 20h6v-9h-6zM4 20h6v-3H4zM14 7h6V4h-6z"/>',
  signals: '<path d="M3 17l5-5 4 4 8-9"/><path d="M15 7h5v5"/>',
  market: '<rect x="3" y="4" width="18" height="17" rx="2"/><path d="M3 9h18M8 2v4M16 2v4M7 13h3M7 17h6"/>',
  trades: '<path d="M4 7h13l-3-3M20 17H7l3 3"/>',
  coach: '<path d="M21 12a8 8 0 0 1-11.6 7.1L4 20l1-4.6A8 8 0 1 1 21 12z"/><path d="M9 10h6M9 14h4"/>',
  academy: '<path d="M2 9l10-5 10 5-10 5z"/><path d="M6 11v5c0 1.5 3 3 6 3s6-1.5 6-3v-5"/>',
  settings: '<circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.6 1.6 0 0 0 .3 1.8l.1.1a2 2 0 1 1-2.8 2.8l-.1-.1a1.6 1.6 0 0 0-1.8-.3 1.6 1.6 0 0 0-1 1.5V21a2 2 0 1 1-4 0v-.1a1.6 1.6 0 0 0-1-1.5 1.6 1.6 0 0 0-1.8.3l-.1.1a2 2 0 1 1-2.8-2.8l.1-.1a1.6 1.6 0 0 0 .3-1.8 1.6 1.6 0 0 0-1.5-1H3a2 2 0 1 1 0-4h.1a1.6 1.6 0 0 0 1.5-1 1.6 1.6 0 0 0-.3-1.8l-.1-.1a2 2 0 1 1 2.8-2.8l.1.1a1.6 1.6 0 0 0 1.8.3H9a1.6 1.6 0 0 0 1-1.5V3a2 2 0 1 1 4 0v.1a1.6 1.6 0 0 0 1 1.5 1.6 1.6 0 0 0 1.8-.3l.1-.1a2 2 0 1 1 2.8 2.8l-.1.1a1.6 1.6 0 0 0-.3 1.8V9a1.6 1.6 0 0 0 1.5 1H21a2 2 0 1 1 0 4h-.1a1.6 1.6 0 0 0-1.5 1z"/>',
  connector: '<path d="M9 7V3M15 7V3M7 7h10v4a5 5 0 0 1-10 0zM12 16v5"/>',
  sub: '<rect x="2" y="5" width="20" height="14" rx="2"/><path d="M2 10h20M6 15h4"/>',
  users: '<circle cx="9" cy="8" r="3.5"/><path d="M2.5 20a6.5 6.5 0 0 1 13 0M16 4.5a3.5 3.5 0 0 1 0 7M18 14a6 6 0 0 1 3.5 6"/>',
  engine: '<rect x="4" y="4" width="16" height="16" rx="2"/><rect x="9" y="9" width="6" height="6"/><path d="M9 1v3M15 1v3M9 20v3M15 20v3M20 9h3M20 14h3M1 9h3M1 14h3"/>',
  audit: '<path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><path d="M14 2v6h6M8 13h8M8 17h5"/>',
  menu: '<path d="M3 6h18M3 12h18M3 18h18"/>', logout: '<path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4M16 17l5-5-5-5M21 12H9"/>',
  alert: '<path d="M10.3 3.9L1.8 18a2 2 0 0 0 1.7 3h17a2 2 0 0 0 1.7-3L13.7 3.9a2 2 0 0 0-3.4 0zM12 9v4M12 17h.01"/>',
  info: '<circle cx="12" cy="12" r="10"/><path d="M12 16v-4M12 8h.01"/>', download: '<path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4M7 10l5 5 5-5M12 15V3"/>',
  refresh: '<path d="M23 4v6h-6M1 20v-6h6"/><path d="M3.5 9a9 9 0 0 1 14.9-3.4L23 10M1 14l4.6 4.4A9 9 0 0 0 20.5 15"/>',
  bolt: '<path d="M13 2L4 14h7l-1 8 9-12h-7z"/>', power: '<path d="M18.4 6.6a9 9 0 1 1-12.8 0M12 2v10"/>',
  learn: '<path d="M12 2a7 7 0 0 0-4 12.7V17h8v-2.3A7 7 0 0 0 12 2z"/><path d="M9 21h6M12 17v-4M10 11l2 2 2-2"/>',
};
const icon = (n) => `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${ICON[n] || ""}</svg>`;

// ── boot & auth pages ───────────────────────────────────────
async function boot() {
  // runs on every session change (load, login, logout, kick, 401): drop the previous account's live state
  S.connector = { online: false }; S.log = []; S.burst = null; S.burstSeen = {}; S.page = null; S.wsMissed = false; S.testSig = null; S.stVariant = null; S.setConnStale = false;
  // and its settings: edits not sent yet are dropped, a save still on its way is answered to the session before
  saveEpoch++; clearTimeout(saveTimer); saveTimer = null; savePatch = {}; saveSent = {}; saveLoud = false; S.settings = null;
  document.documentElement.lang = S.lang;
  try { S.info = await api("/api/public/info"); } catch { S.info = { setup_required: false, signup_open: true, trial_days: 3 }; }
  if (S.info.setup_required) return renderAuth("setup");
  try { S.me = await api("/api/auth/me"); } catch { S.me = null; }
  if (!S.me) { const h = location.hash; return renderAuth(h.startsWith("#/register") ? "register" : "login"); }
  if (S.me.lang && S.me.lang !== S.lang) { S.lang = S.me.lang; try { localStorage.setItem("fx_lang", S.lang); } catch {} document.documentElement.lang = S.lang; }
  await loadSettings();
  renderShell();
  openWS();
  if (!location.hash || location.hash.startsWith("#/login") || location.hash.startsWith("#/register")) location.hash = "#/";
  route();
}
async function loadSettings() {
  // edits not yet acknowledged by the server stay on top of what it returns; a save answered meanwhile is newer;
  // an answer to the session before (a logout or login since) is that account's: dropped
  const done = saveDone, ep = saveEpoch;
  try { const r = await api("/api/me/settings"); if (ep !== saveEpoch) return; if (done === saveDone || !S.settings) S.settings = mergeSettings(r.settings, saveSent, savePatch); S.symbols = r.symbols; S.modesEnabled = r.modes_enabled; if (r.burst_app) S.burstApp = r.burst_app; } catch { /* keep */ }
}
function langSeg() {
  return `<div class="seg" data-lang-seg><button data-lang="en" class="${S.lang === "en" ? "on" : ""}">EN</button><button data-lang="ro" class="${S.lang === "ro" ? "on" : ""}">RO</button></div>`;
}
// the platform's version as the server names it (GET /api/public/info, server/main.py VERSION): "V1.3.0" in the header
const verTag = () => (S.info && S.info.version ? `<span class="ver" title="FX Signals">V${esc(S.info.version)}</span>` : "");
function renderAuth(kind) {
  const days = S.info ? S.info.trial_days : 3;
  const head = { login: [t("login_title1"), t("login_title2"), t("login_sub")],
    register: [t("reg_title1"), t("reg_title2"), days ? t("reg_sub", { days }) : t("reg_sub_none")],
    setup: [t("setup_title1"), t("setup_title2"), t("setup_sub")] }[kind];
  const closed = kind === "register" && S.info && !S.info.signup_open;
  $("#app").innerHTML = `<div class="auth"><div class="box">
    <div class="row between" style="margin-bottom:18px"><div class="brand" style="padding:0"><div class="mark">FX</div><div class="name">${t("brand1")} <b>${t("brand2")}</b></div></div><div class="row">${verTag()}${langSeg()}</div></div>
    <div class="card"><div class="eyebrow">${kind === "setup" ? "Setup" : "FX Signals"}</div>
      <h1>${esc(head[0])} <b>${esc(head[1])}</b></h1><p class="muted">${esc(head[2])}</p>
      ${closed ? `<div class="banner" style="margin-top:16px">${icon("info")}<div>${t("signup_closed")}</div></div>` : `
      <form id="authForm" novalidate>
        ${kind === "setup" ? `<label class="field"><span>${t("setup_token")}</span><input class="input" name="token" required autocomplete="off"></label>` : ""}
        ${kind !== "login" ? `<label class="field"><span>${t("name")}</span><input class="input" name="name" autocomplete="name" maxlength="120"></label>` : ""}
        <label class="field"><span>${t("email")}</span><input class="input" type="email" name="email" required autocomplete="email"></label>
        <label class="field"><span>${t("password")}</span><input class="input" type="password" name="password" required minlength="8" autocomplete="${kind === "login" ? "current-password" : "new-password"}"></label>
        ${kind === "register" ? `<label class="check"><input type="checkbox" name="accept"><span class="muted" style="font-size:12.5px">${t("accept_terms")}</span></label>` : ""}
        <div class="err" id="authErr"></div>
        <button class="btn gold" type="submit" style="height:44px">${kind === "login" ? t("login") : kind === "register" ? t("register") : t("create_founder")}</button>
      </form>`}
      ${kind === "login" ? `<div class="switch-line">${t("no_account")} <a href="#/register">${t("register")}</a></div>` : ""}
      ${kind === "register" ? `<div class="switch-line">${t("have_account")} <a href="#/login">${t("login")}</a></div>` : ""}
    </div><p class="disclaimer">${t("disclaimer")}</p></div></div>`;
  const f = $("#authForm");
  if (!f) return;
  f.addEventListener("submit", async (e) => {
    e.preventDefault();
    const d = Object.fromEntries(new FormData(f));
    const btn = f.querySelector("button[type=submit]"); btn.disabled = true; $("#authErr").textContent = "";
    try {
      if (kind === "login") await api("/api/auth/login", { method: "POST", body: { email: d.email, password: d.password } });
      else if (kind === "register") await api("/api/auth/register", { method: "POST", body: { email: d.email, password: d.password, name: d.name || "", lang: S.lang, accept_terms: !!d.accept } });
      else await api("/api/setup", { method: "POST", body: { token: d.token, email: d.email, password: d.password, name: d.name || "" } });
      location.hash = "#/"; await boot();
    } catch (err) { $("#authErr").textContent = errText(err); btn.disabled = false; }
  });
}

// ── shell ───────────────────────────────────────────────────
const NAV = [
  ["nav_main", [["#/", "dashboard", "dash"], ["#/signals", "signals", "signals"], ["#/market", "market", "market"], ["#/trades", "trades", "trades"]]],
  ["nav_learn", [["#/coach", "coach", "coach"], ["#/academy", "academy", "academy"]]],
  ["nav_account", [["#/settings", "settings", "settings"], ["#/connector", "connector", "connector"], ["#/subscription", "subscription", "sub"]]],
  ["nav_admin", [["#/admin", "admin_users", "users"], ["#/admin/engine", "admin_engine", "engine"], ["#/admin/learning", "admin_learning", "learn"], ["#/admin/audit", "admin_audit", "audit"]], true],
];
function renderShell() {
  const me = S.me, staff = me.role === "founder" || me.role === "admin";
  $("#app").innerHTML = `<div class="shell">
    <aside class="side" id="side">
      <div class="brand"><div class="mark">FX</div><div class="name">${t("brand1")} <b>${t("brand2")}</b></div></div>
      ${NAV.filter((g) => !g[2] || staff).map(([label, items]) => `<div class="nav-label">${t(label)}</div><nav class="nav">${items.map(([href, key, ic]) =>
        `<a href="${href}" data-nav="${href}">${icon(ic)}<span>${t(key)}</span></a>`).join("")}</nav>`).join("")}
      <div class="grow"></div>
      <div class="me"><div class="who">${esc(me.name || me.email)}</div><div class="faint" style="font-size:12px">${esc(me.email)}</div>
        <div class="row" style="margin-top:8px"><span class="chip ${me.role === "client" ? "" : "gold"}">${esc(t("r_" + me.role))}</span>
        <button class="btn sm" data-action="logout">${icon("logout")}${t("logout")}</button></div></div>
    </aside>
    <div class="main">
      <div class="topbar"><div class="left"><button class="btn icon burger" data-action="menu" aria-label="Menu">${icon("menu")}</button>
        <span class="pill" id="livePill"><span class="dot" id="liveDot"></span><span id="liveText">${t("offline")}</span></span>
        <span class="pill" id="connPill" title="${t("connector")}"><span class="dot" id="connDot"></span><span id="connText">${t("connector")}</span></span>
        <span class="pill" id="mktPill"><span class="dot" id="mktDot"></span><span id="mktText"></span></span></div>
        <div class="row">${verTag()}${langSeg()}</div></div>
      <div id="subBanner"></div>
      <main class="page" id="page"></main>
    </div></div>`;
  updateLivePill(); updateConnPill(); updateMktPill(); updateSubBanner();
}
// FX market hours (v1.3.1), the same rule as bot/mt5_broker.py _fx_closed: shut Friday 17:00 to Sunday 17:00 New York
// time (21:00 or 22:00 UTC, with US DST), Christmas Day and New Year's Day (UTC days), and from 21:00 UTC on 24 and
// 31 December. Every edge of it falls on a whole UTC hour (New York is a whole number of hours off UTC)
const NY_CLOCK = new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", weekday: "short", hour: "numeric", hourCycle: "h23" });
const NY_DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];   // Python's weekday(): Monday 0
function fxClosed(ms) {
  const d = new Date(ms), p = {};
  for (const x of NY_CLOCK.formatToParts(d)) p[x.type] = x.value;
  const h = NY_DAYS.indexOf(p.weekday) * 24 + (Number(p.hour) % 24), m = d.getUTCMonth() + 1, day = d.getUTCDate();
  return (4 * 24 + 17 <= h && h < 6 * 24 + 17) || (m === 12 && day === 25) || (m === 1 && day === 1)
    || (((m === 12 && day === 24) || (m === 12 && day === 31)) && d.getUTCHours() >= 21);
}
// the next time the market opens (closed now) or shuts (open now), searched hour by hour on the UTC hours; kept until
// it is reached, so the clock reads New York's time zone once an hour, not every second
let mktNext = { at: 0, closed: null };
function mktState(now = Date.now()) {
  const closed = fxClosed(now);
  if (closed !== mktNext.closed || now >= mktNext.at) {
    let at = Math.floor(now / 3600e3) * 3600e3 + 3600e3;
    for (let i = 0; i < 24 * 14 && fxClosed(at) === closed; i++) at += 3600e3;   // two weeks: a holiday next to a weekend
    mktNext = { at, closed };
  }
  return mktNext;
}
const mktLeft = (ms) => {   // "1z 05:12:09" / "05:12:09"
  const s = Math.max(0, Math.round(ms / 1000)), d = Math.floor(s / 86400), p = (n) => String(n).padStart(2, "0");
  return `${d ? d + t("mkt_day") + " " : ""}${p(Math.floor(s / 3600) % 24)}:${p(Math.floor(s / 60) % 60)}:${p(s % 60)}`;
};
// market sessions (v1.3.2): each in its city's own hours, with that city's daylight saving (Settings, "market"); a
// session is open on the city's Monday to Friday, inside its hours, while the forex market is open. The four cities
// are a whole number of hours off UTC, so every edge falls on a whole UTC hour, as the market's own do
const SESSIONS = [["sydney", "Australia/Sydney"], ["tokyo", "Asia/Tokyo"], ["london", "Europe/London"], ["newyork", "America/New_York"]];
const MARKET_DEFAULT = { pill: true, widget: true, sessions: { sydney: { on: true, from: 7, to: 16 }, tokyo: { on: true, from: 9, to: 18 },
  london: { on: true, from: 8, to: 17 }, newyork: { on: true, from: 8, to: 17 } } };   // server/dashboard.py MARKET_DEFAULT
const CITY_CLOCK = Object.fromEntries(SESSIONS.map(([id, tz]) => [id, new Intl.DateTimeFormat("en-US", { timeZone: tz, weekday: "short", hour: "numeric", hourCycle: "h23" })]));
function mktConf() {   // the saved clock settings over the defaults (none saved yet, or a server before v1.3.2)
  const m = (S.settings && S.settings.market) || {}, ses = m.sessions || {};
  return { pill: m.pill !== false, widget: m.widget !== false,
    sessions: Object.fromEntries(SESSIONS.map(([id]) => [id, { ...MARKET_DEFAULT.sessions[id], ...(ses[id] || {}) }])) };
}
function sessOpen(id, ms, cfg) {
  const c = cfg.sessions[id]; if (!c.on) return false;
  const p = {}; for (const x of CITY_CLOCK[id].formatToParts(new Date(ms))) p[x.type] = x.value;
  const h = Number(p.hour) % 24;
  return NY_DAYS.indexOf(p.weekday) < 5 && c.from <= h && h < c.to && !fxClosed(ms);
}
// when a session opens (closed now) or shuts (open now) next, searched on the UTC hours like mktState; kept until it
// is reached, the session's hours change or the clock goes back
const SESS_NEXT = {};
function sessState(id, now, cfg) {
  const c = cfg.sessions[id], key = `${c.on}|${c.from}|${c.to}`, open = sessOpen(id, now, cfg), n = SESS_NEXT[id];
  if (!n || n.key !== key || n.open !== open || now >= n.at || now < n.since) {
    let at = Math.floor(now / 3600e3) * 3600e3 + 3600e3;
    for (let i = 0; i < 24 * 14 && sessOpen(id, at, cfg) === open; i++) at += 3600e3;
    SESS_NEXT[id] = { at, open, key, since: now };
  }
  return SESS_NEXT[id];
}
const sessIds = (cfg) => SESSIONS.map(([id]) => id).filter((id) => cfg.sessions[id].on);
// the session that opens first while the market stays open (closeAt: when it shuts), or null
function nextSession(now, cfg, closeAt) {
  let best = null;
  for (const id of sessIds(cfg)) { const s = sessState(id, now, cfg); if (!s.open && s.at < closeAt && (!best || s.at < best.at)) best = { id, at: s.at }; }
  return best;
}
// the session's opening it is in now, or its next one, in this PC's time: [opens, shuts]
function sessWindow(id, now, cfg) {
  const s = sessState(id, now, cfg);
  if (s.open) { let a = Math.floor(now / 3600e3) * 3600e3; while (sessOpen(id, a - 3600e3, cfg) && now - a < 86400e3) a -= 3600e3; return [a, s.at]; }
  let b = s.at + 3600e3; while (sessOpen(id, b, cfg) && b - s.at < 86400e3) b += 3600e3;
  return [s.at, b];
}
const dayStart = (ms) => { const d = new Date(ms); d.setHours(0, 0, 0, 0); return d.getTime(); };   // this PC's midnight
function sessSpans(id, cfg, a, b) {   // the pieces of [a, b) the session is open in, as [from, to]
  const out = []; let cur = null;
  for (let h = Math.floor(a / 3600e3) * 3600e3; h < b; h += 3600e3) {
    const open = sessOpen(id, h, cfg);
    if (open && !cur) cur = [Math.max(h, a), b];
    else if (!open && cur) { cur[1] = h; out.push(cur); cur = null; }
  }
  if (cur) out.push(cur);
  return out;
}
function sessHours(id, now, cfg) {   // "10:00 – 19:00 your time"; the weekday too when it is not today
  const [a, b] = sessWindow(id, now, cfg), loc = S.lang === "ro" ? "ro-RO" : "en-GB", hm = { hour: "2-digit", minute: "2-digit" };
  const day = dayStart(a) === dayStart(now) ? "" : new Date(a).toLocaleDateString(loc, { weekday: "short" }) + " ";
  return t("sess_hours", { a: day + new Date(a).toLocaleTimeString(loc, hm), b: new Date(b).toLocaleTimeString(loc, hm) });
}
// the Dashboard's sessions card: each session followed on a bar of this PC's day (00 to 24), with a mark at now
function sessCardHtml(now, cfg) {
  const ids = sessIds(cfg), headH = `<div class="card-head"><h2>${t("sess_card")}</h2><a href="#/settings">${t("sess_settings")} →</a></div>`;
  if (!ids.length) return headH + `<div class="empty" style="padding:16px">${t("sess_none")}</div>`;
  const a = dayStart(now), b = dayStart(a + 30 * 3600e3), pct = (ms) => ((ms - a) / (b - a)) * 100;
  const spans = Object.fromEntries(ids.map((id) => [id, sessSpans(id, cfg, a, b)]));
  const tick = (h) => { const d = new Date(a); d.setHours(h); return h === 24 ? 100 : pct(d.getTime()); };
  let both = null;
  if (spans.london && spans.newyork) for (const [x1, y1] of spans.london) for (const [x2, y2] of spans.newyork)
    if (!both && Math.max(x1, x2) < Math.min(y1, y2)) both = [Math.max(x1, x2), Math.min(y1, y2)];
  const hm = (ms) => new Date(ms).toLocaleTimeString(S.lang === "ro" ? "ro-RO" : "en-GB", { hour: "2-digit", minute: "2-digit" });
  return headH + `<div class="sess-rows">${ids.map((id) => `<div class="sess-row" data-sess="${id}">
      <div class="sess-name"><span class="dot"></span><b>${t("sess_" + id)}</b><span class="faint">${sessHours(id, now, cfg)}</span></div>
      <div class="sess-st" data-st></div>
      <div class="sess-bar">${spans[id].map(([x, y]) => `<i style="left:${pct(x).toFixed(2)}%;width:${(pct(y) - pct(x)).toFixed(2)}%"></i>`).join("")}<b class="sess-now"></b></div></div>`).join("")}
    <div class="sess-row sess-axis"><div class="sess-ticks">${[0, 6, 12, 18, 24].map((h) => `<span style="left:${tick(h).toFixed(2)}%">${String(h).padStart(2, "0")}</span>`).join("")}</div></div></div>
    ${both ? `<div class="sess-note">${t("sess_overlap", { a: hm(both[0]), b: hm(both[1]) })}</div>` : ""}`;
}
function updateSessCard(now, cfg) {   // drawn again when a session opens or shuts, the day or the settings change; else the times only
  const box = $("#sessCard"); if (!box) return;
  const ids = sessIds(cfg), st = Object.fromEntries(ids.map((id) => [id, sessState(id, now, cfg)]));
  const key = [dayStart(now), S.lang, JSON.stringify(cfg.sessions), ...ids.map((id) => st[id].at)].join("|");
  if (box.dataset.key !== key) { box.innerHTML = sessCardHtml(now, cfg); box.dataset.key = key; }
  const a = dayStart(now), x = (((now - a) / (dayStart(a + 30 * 3600e3) - a)) * 100).toFixed(2) + "%";
  for (const id of ids) {
    const row = $(`[data-sess="${id}"]`, box), s = st[id]; if (!row) continue;
    row.classList.toggle("open", s.open); $(".dot", row).className = "dot" + (s.open ? " on" : ""); $(".sess-now", row).style.left = x;
    $("[data-st]", row).innerHTML = `<b>${t(s.open ? "sess_open" : "sess_closed")}</b> · ${t(s.open ? "sess_shuts" : "sess_opens", { t: mktLeft(s.at - now) })}`;
  }
}
// the top bar: closed, the time until the market opens; open, the next session followed that opens before the market
// shuts, or else (its last stretch) the time until it shuts; "open" with no session to wait for and more than a day left
function updateMktPill() {
  const now = Date.now(), cfg = mktConf();
  updateSessCard(now, cfg);
  const pill = $("#mktPill"), dot = $("#mktDot"), txt = $("#mktText"); if (!dot) return;
  pill.classList.toggle("hidden", !cfg.pill); if (!cfg.pill) return;
  const m = mktState(now), left = m.at - now, when = new Date(m.at).toLocaleString(S.lang === "ro" ? "ro-RO" : "en-GB",
    { weekday: "short", day: "numeric", month: "short", hour: "2-digit", minute: "2-digit" });
  dot.className = "dot " + (m.closed ? "off" : left <= 86400e3 ? "warn" : "on");
  if (m.closed) { txt.textContent = t("mkt_opens", { t: mktLeft(left) }); pill.title = t("mkt_opens_at", { at: when }); return; }
  const nx = nextSession(now, cfg, m.at), open = sessIds(cfg).filter((id) => sessState(id, now, cfg).open).map((id) => t("sess_" + id));
  txt.textContent = nx ? t("mkt_next", { s: t("sess_" + nx.id), t: mktLeft(nx.at - now) }) : left <= 86400e3 ? t("mkt_shuts", { t: mktLeft(left) }) : t("mkt_open");
  pill.title = t("mkt_shuts_at", { at: when }) + " · " + (open.length ? t("mkt_open_now", { list: open.join(", ") }) : t("mkt_none_open"));
}
function updateSubBanner() {
  const el = $("#subBanner"); if (!el || !S.me) return;
  el.innerHTML = S.me.subscription_active ? "" : `<div style="padding:14px 28px 0"><div class="banner danger">${icon("alert")}<div><b>${t("sub_inactive")}</b> · ${t("sub_blocked")} <a href="#/subscription">${t("subscription")} →</a></div></div></div>`;
}
function updateLivePill() {
  const dot = $("#liveDot"), txt = $("#liveText"); if (!dot) return;
  dot.className = "dot " + (S.wsState === "on" ? "on" : S.wsState === "retry" ? "warn" : "");
  txt.textContent = S.wsState === "on" ? t("live") : S.wsState === "retry" ? t("reconnecting") : t("offline");
}
function updateConnPill() {
  const dot = $("#connDot"), txt = $("#connText"); if (!dot) return;
  const c = S.connector || {};
  dot.className = "dot " + (c.online ? "on" : "");
  txt.textContent = c.online ? `MT5 · ${c.account && c.account.demo === false ? t("real") : t("demo")}` : t("no_connector");
}

// ── router ──────────────────────────────────────────────────
const ROUTES = {
  "/": pageDashboard, "/signals": pageSignals, "/market": pageMarket, "/trades": pageTrades, "/coach": pageCoach,
  "/academy": pageAcademy, "/settings": pageSettings, "/connector": pageConnector, "/subscription": pageSubscription,
  "/admin": pageAdminUsers, "/admin/engine": pageAdminEngine, "/admin/learning": pageAdminLearning, "/admin/audit": pageAdminAudit,
};
const NEEDS_ACTIVE = new Set(["/", "/signals", "/market", "/coach"]);
function parseHash() {
  const h = location.hash.slice(1) || "/";
  const [path, qs] = h.split("?");
  return { path: path || "/", q: new URLSearchParams(qs || "") };
}
async function route() {
  if (!S.me) return;
  const { path, q } = parseHash();
  let fn = ROUTES[path] || pageDashboard;
  if (path.startsWith("/admin") && !(S.me.role === "founder" || S.me.role === "admin")) fn = pageDashboard;
  if (!S.me.subscription_active && NEEDS_ACTIVE.has(path)) fn = pageSubscription;
  if (S.pageCleanup) { try { S.pageCleanup(); } catch {} S.pageCleanup = null; }
  clearTimeout(softTimer); S.routeSeq = (S.routeSeq || 0) + 1;   // a pending soft refresh belongs to the old page
  $$(".nav a").forEach((a) => a.classList.toggle("on", a.dataset.nav === "#" + path || (path === "/" && a.dataset.nav === "#/")));
  $("#side") && $("#side").classList.remove("open");
  // each route renders into its own host: a slow render that a newer route superseded lands in a detached node
  const page = $("#page"), seq = S.routeSeq, host = document.createElement("div");
  S.page = fn;
  page.replaceChildren(host);
  host.innerHTML = `<div class="empty"><div class="spin" style="margin:auto"></div></div>`;
  try { await fn(host, q); } catch (e) {
    if (S.routeSeq === seq && host.isConnected) host.innerHTML = `<div class="card empty">${esc(errText(e))}<div style="margin-top:12px"><button class="btn" data-action="reload">${t("retry")}</button></div></div>`;
  }
  if (S.routeSeq === seq) window.scrollTo({ top: 0 });
}
const head = (eyebrow, title, sub, right = "") => `<div class="page-head"><div><div class="eyebrow">${esc(eyebrow)}</div><h1>${title}</h1>${sub ? `<p class="muted" style="margin:6px 0 0">${esc(sub)}</p>` : ""}</div><div class="row">${right}</div></div>`;
const splitTitle = (s) => { const w = s.split(" "); return w.length > 1 ? `${esc(w.slice(0, -1).join(" "))} <b>${esc(w.at(-1))}</b>` : `<b>${esc(s)}</b>`; };

// ── shared renderers ────────────────────────────────────────
const STATUS_CHIP = { open: "gold", won: "up", lost: "down", closed: "", expired: "" };
function sigCard(s, { compact = false } = {}) {
  const why = S.lang === "ro" && s.rationale_ro ? s.rationale_ro : s.rationale;
  const confirmMode = S.settings && S.settings.execution === "confirm";
  const canExec = s.status === "open" && S.connector.online && (confirmMode || S.settings?.execution === "auto");
  return `<div class="sig ${s._fresh ? "fresh" : ""}" data-sig="${s.id}">
    <div class="badge ${s.side === "buy" ? "buy" : "sell"}">${s.side === "buy" ? "BUY" : "SELL"}</div>
    <div style="min-width:0"><div class="title">${esc(s.symbol)} <span class="chip">${esc(t("mode_" + s.mode))}</span>
      <span class="chip ${STATUS_CHIP[s.status] || ""}">${esc(t("st_" + s.status))}</span>
      ${s.for_me === false ? `<span class="chip">${t("not_for_you")}</span>` : ""}</div>
      <div class="meta">${t("entry")} ${fmtP(s.symbol, s.price)} · ${t("sl")} ${fmtP(s.symbol, s.sl)}${s.tp ? ` · ${t("tp")} ${fmtP(s.symbol, s.tp)}` : ""} · ${ago(s.created_at)}
        ${s.expires_at && s.status === "open" ? ` · ${t("expires")} ${ago(s.expires_at)}` : ""}</div>
      ${s.skip_reason ? `<div class="faint" style="font-size:12px;margin-top:4px">${t("skipped_by_connector")}: ${esc(s.skip_reason)}</div>` : ""}
      ${!compact && why ? `<div class="why">${esc(why)}</div>` : ""}
      ${s.confidence != null ? `<div class="conf" style="margin-top:6px">${t("confidence")} <span class="bar"><i style="width:${Math.max(0, Math.min(100, s.confidence))}%"></i></span> ${s.confidence}%</div>` : ""}
    </div>
    <div class="act">${s.status === "open"
      ? (canExec ? `<button class="btn sm gold" data-action="exec" data-id="${s.id}">${t("execute")}</button>` : "")
      : `<span class="num ${cls(s.result_pips)}" style="font-weight:600">${signed(s.result_pips)} ${t("pips")}</span><span class="faint" style="font-size:12px">${esc(s.close_reason || "")}</span>`}
      <a class="btn sm" href="#/coach?signal=${s.id}">${t("explain")}</a></div></div>`;
}
function spark(values, w = 160, h = 34) {
  if (!values || values.length < 2) return "";
  const min = Math.min(...values), max = Math.max(...values), span = max - min || 1;
  const pts = values.map((v, i) => `${(i / (values.length - 1)) * w},${h - 3 - ((v - min) / span) * (h - 6)}`).join(" ");
  const col = values.at(-1) >= values[0] ? "var(--up)" : "var(--down)";
  return `<svg viewBox="0 0 ${w} ${h}" preserveAspectRatio="none"><polyline points="${pts}" fill="none" stroke="${col}" stroke-width="1.5" vector-effect="non-scaling-stroke"/></svg>`;
}
function lineChart(points) {   // points: [{x: Date, y: number}]
  if (points.length < 2) return "";
  const w = 600, h = 190, pad = 6;
  const ys = points.map((p) => p.y), min = Math.min(0, ...ys), max = Math.max(0, ...ys), span = max - min || 1;
  const x = (i) => pad + (i / (points.length - 1)) * (w - pad * 2), y = (v) => h - pad - ((v - min) / span) * (h - pad * 2);
  const d = points.map((p, i) => `${x(i)},${y(p.y)}`).join(" ");
  const col = ys.at(-1) >= 0 ? "var(--up)" : "var(--down)";
  return `<svg class="chart" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none"><line x1="0" x2="${w}" y1="${y(0)}" y2="${y(0)}" stroke="var(--line-2)" stroke-dasharray="3 4" vector-effect="non-scaling-stroke"/>
    <polygon points="${x(0)},${y(0)} ${d} ${x(points.length - 1)},${y(0)}" fill="${col}" opacity=".08"/><polyline points="${d}" fill="none" stroke="${col}" stroke-width="2" vector-effect="non-scaling-stroke"/></svg>`;
}
const impactBars = (imp) => `<span class="impact ${esc(imp)}" title="${esc(imp)}"><i></i><i></i><i></i></span>`;
function logList(items) {
  if (!items.length) return `<div class="empty">${t("no_log")}</div>`;
  return `<ul class="log">${items.slice(0, 60).map((l) => `<li><time>${fmtTime(l.time)}</time><span class="${esc(l.level)}">${esc(l.message)}</span></li>`).join("")}</ul>`;
}

// ── news burst ──────────────────────────────────────────────
// a gap in the stored block is filled as the server reads a v1.2.0 block (server/burst.py legacy_burst): stop in pips,
// no analysis, no max_hold_seconds (Y follows X). The server sends every key; sent back unchanged, nothing turns new
const BURST_DEF = { enabled: false, trades: 10, lead_seconds: 60, offset_pips: 7, sl_pips: 10, tp_start_pips: 4, tp_step_pips: 1.5,
  hold_seconds: 30, expire_seconds: 90, currencies: [], sl_unit: "pips", sl_money: 2, lots: 0.1, analysis: false, symbols: [] };
// the pairs a currency's releases can trade (server/burst.py BURST_PAIRS): the first is its own, traded when none is picked
const BURST_PAIRS = { USD: ["EURUSD", "XAUUSD", "GBPUSD", "USDJPY"], EUR: ["EURUSD"], GBP: ["GBPUSD", "GBPJPY"], JPY: ["USDJPY", "GBPJPY"],
  AUD: ["AUDUSD"], NZD: ["NZDUSD"], CAD: ["USDCAD"], CHF: ["USDCHF"] };
const BURST_CUR = Object.keys(BURST_PAIRS);
// [min, max, step] as server/burst.py LIMITS, SL_MONEY and MAX_HOLD (the server clamps again); Y's min is X
const BURST_RANGE = { trades: [1, 40, 1], lead_seconds: [10, 600, 1], offset_pips: [1, 100, 0.1], sl_pips: [2, 200, 0.1], tp_start_pips: [1, 500, 0.1],
  tp_step_pips: [0, 100, 0.1], hold_seconds: [5, 600, 1], max_hold_seconds: [5, 1800, 1], expire_seconds: [10, 1800, 1], lots: [0.01, 100, 0.01],
  sl_money: { pct: [0.1, 10, 0.1], usd: [1, 100000, 1] } };
const BURST_MONEY_DEF = { pips: 2, pct: 2, usd: 100 };   // sl_money a new unit starts from (SL_MONEY's defaults; pips reads pct's)
// pip settings count this many times on these pairs (bot/symbols.py SYMBOL_SCALE); they and gold need connector 1.3.0
const SYMBOL_SCALE = { XAUUSD: 5, GBPJPY: 2 };
const V130 = [1, 3, 0];
const newPair = (s) => { const u = String(s || "").toUpperCase(); return u.includes("XAU") || Object.keys(SYMBOL_SCALE).some((k) => u.startsWith(k)); };
const old130 = () => !!(S.connector && S.connector.online) && !verAtLeast(S.connector.version, V130);   // online, before 1.3.0
const pipScaleText = () => t("pip_scale", { list: Object.entries(SYMBOL_SCALE).map(([s, k]) => `${s} ×${k}`).join(", ") });
const BURST_FINAL = new Set(["closed", "expired", "skipped", "cancelled", "error"]);
const BURST_CHIP = { scheduled: "info", armed: "warn", triggered: "gold", error: "down" };
const burstSet = () => ({ ...BURST_DEF, ...(S.settings && S.settings.burst) });
// only the keys edited (v1.3.1; up to v1.3.0 the whole block): saveSettings adds them to the block, and the server keeps
// every key not sent as it has it, so a page left open elsewhere never sends back what it was served before
const saveBurst = (patch) => saveSettings({ burst: withUnits(patch, burstSet()) });
const verOk = (v) => { const m = /^(\d+)\.(\d+)/.exec(v || ""); return !!m && (+m[1] > 1 || (+m[1] === 1 && +m[2] >= 1)); };   // bursts need connector ≥ 1.1.0
// settings a connector before 1.3.0 cannot run as set (server/burst.py needs_version): it skips the bursts that use them
const burstUses130 = (bs) => bs.sl_unit !== "pips" || bs.analysis !== false || (bs.max_hold_seconds != null && bs.max_hold_seconds > bs.hold_seconds)
  || (bs.symbols || []).some(newPair);
const kindChip = (b) => (b.kind === "rapid" ? `<span class="chip info">${t("v_rapid")}</span>` : `<span class="chip">${t("v_classic")}</span>`);
// a run's pre-news analysis as one line, in the words of bot/news_view.py describe: what it chose, how sure, the parts that
// said something (|part| >= 0.05); shadow = its switch was off, the line says what it would have done
const AN_PARTS = ["trend", "momentum", "expect", "ai"];
function anLine(a) {
  const head = t(a && a.shadow ? "an_shadow" : "an_head");
  if (!a || typeof a !== "object" || !["buy", "sell", "both", "skip"].includes(a.action)) return `${head}: ${t("an_na")}`;
  if (a.action === "skip") return `${head}: ${t(a.reason === "blow_out" ? "an_blow_out" : "an_spread")}`;
  const part = (v) => (typeof v === "number" && isFinite(v) ? Math.round(Math.max(-1, Math.min(1, v)) * 1000) / 1000 : null);
  const s = part(a.score) || 0, parts = a.parts && typeof a.parts === "object" ? a.parts : {};
  const why = AN_PARTS.map((k) => [k, part(parts[k])]).filter(([, v]) => v != null && Math.abs(v) >= 0.05)
    .map(([k, v]) => `${t("an_p_" + k)} ${v > 0 ? "+" : "-"}`).join(", ") || t("an_none");
  if (a.action === "both") return `${head}: ${t("an_both", { s: s.toFixed(2) })}: ${why}`;
  return `${head}: ${a.action.toUpperCase()} ${t("an_clear")} (${s.toFixed(2)}): ${why}`;
}
const loadBursts = () => api("/api/bursts").then((d) => { if (d && d.app) S.burstApp = d.app; return d; }).catch(() => null);
const burstSide = (s) => (s === "buy" ? t("buy") : s === "sell" ? t("sell") : s === "both" ? t("burst_both") : "—");
function burstChip(b) {
  const k = b.status === "closed" ? (b.pnl > 0 ? "up" : b.pnl < 0 ? "down" : "") : BURST_CHIP[b.status] || "";
  return `<span class="chip ${k}">${esc(t("bs_" + b.status))}</span>`;
}
function countdown(iso) {
  const s = Math.round((new Date(iso).getTime() - Date.now()) / 1000), p = (n) => String(n).padStart(2, "0");
  if (s <= 0) return t("burst_now");
  if (s >= 86400) return t("cd_dh", { d: Math.floor(s / 86400), h: Math.floor((s % 86400) / 3600) });
  const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60);
  return (h ? `${h}:${p(m)}` : m) + ":" + p(s % 60);
}
const burstLine = (b, cur) => `<div class="row between" style="align-items:flex-start"><div style="min-width:0;flex:1">
    <div class="row" style="gap:6px">${burstChip(b)}${kindChip(b)}<b>${esc(b.symbol)}</b>${b.test ? `<span class="chip">${t("burst_test_chip")}</span>` : ""}<span class="muted" style="font-size:12.5px">${esc(b.event_title)}</span></div>
    <div class="muted num" style="font-size:12.5px;margin-top:4px">${t("side")} ${burstSide(b.side)} · ${t("burst_orders")} ${Number(b.orders) || 0} · ${t("burst_fills")} ${Number(b.fills) || 0} · ${t("pnl")} <span class="${cls(b.pnl)}">${b.pnl == null ? "—" : fmtMoney(b.pnl, cur)}</span> · ${fmtDT(b.event_time)}</div>
    ${b.analysis ? `<div class="faint" style="font-size:12px;margin-top:2px">${esc(anLine(b.analysis))}</div>` : ""}
    ${b.reason ? `<div class="faint" style="font-size:12px;margin-top:2px">${esc(b.reason)}</div>` : ""}</div>
  ${BURST_FINAL.has(b.status) ? "" : `<button class="btn sm danger" data-action="burst-cancel" data-id="${esc(b.id)}">${t("burst_cancel")}</button>`}</div>`;
function burstCardHtml() {
  const d = S.burst, bs = burstSet(), app = S.burstApp, c = d.connector || {}, on = bs.enabled && app.enabled, now = Date.now();
  const next = (d.upcoming || []).find((u) => new Date(u.time).getTime() > now - 60e3);
  const list = d.bursts || [], live = list.find((b) => !BURST_FINAL.has(b.status)) || list[0];
  const warn = !app.enabled ? "" : !c.online ? t("burst_conn_off") : !verOk(c.version) ? t("burst_conn_old")
    : !verAtLeast(c.version, V130) && (burstUses130(bs) || rpSet().enabled) ? t("burst_conn_130") : "";
  const testOk = app.enabled && c.online && verOk(c.version) && c.demo !== false;
  return `<div class="card-head"><h2>${icon("bolt")}${t("burst_title")}</h2><div class="row">
      ${app.enabled ? `<span class="chip ${on ? "gold" : ""}">${on ? t("burst_on") : t("burst_off")}</span>` : `<span class="chip warn">${t("burst_admin_chip")}</span>`}
      <a href="#/settings" style="font-size:13px">${t("settings")} →</a></div></div>
    <div class="burst-grid">
      <button class="burst-toggle ${on ? "on" : ""}" data-action="burst-toggle" aria-pressed="${on}" ${app.enabled ? "" : "disabled"}>${icon("power")}
        <b>${on ? t("burst_on") : t("burst_off")}</b><small>${!app.enabled ? t("burst_admin_chip") : on ? t("burst_tap_off") : t("burst_tap_on")}</small></button>
      <div style="min-width:0">
        ${!app.enabled ? `<div class="banner danger" style="margin-bottom:14px">${icon("alert")}<div>${t("burst_admin_off")}</div></div>`
          : warn ? `<div class="banner" style="margin-bottom:14px">${icon("alert")}<div>${warn} <a href="#/connector">${t("connector")} →</a></div></div>` : ""}
        <div class="burst-sec"><div class="label">${t("burst_next")}</div>
          ${next ? `<div class="row between" style="flex-wrap:nowrap"><div style="min-width:0"><b>${esc(next.title)}</b><div class="muted" style="font-size:12.5px">${esc(next.currency)} · ${esc(next.symbol)}${next.kind === "rapid" ? ` · ${t("v_rapid")}` : ""} · ${fmtDT(next.time)}</div></div>
            <span class="burst-count num" data-countdown="${esc(next.time)}">${countdown(next.time)}</span></div>` : `<div class="muted">${t("burst_no_next")}</div>`}</div>
        <div class="burst-sec"><div class="label">${live && !BURST_FINAL.has(live.status) ? t("burst_live") : t("burst_last")}</div>
          ${live ? burstLine(live, (S.connector.account || {}).currency) : `<div class="muted">${t("burst_none")}</div>`}</div>
        <div class="burst-sec row"><button class="btn sm" data-action="burst-test" ${testOk ? "" : "disabled"}>${t("burst_test")}</button><span class="faint" style="font-size:12px">${t("burst_test_hint")}</span></div>
      </div></div>`;
}
function renderBurstCard() { const card = $("#burstCard"); if (card && S.burst) card.innerHTML = burstCardHtml(); }
function burstUpsert(b) {
  if (!b || b.id == null) return;
  if (S.burst) { const L = S.burst.bursts || (S.burst.bursts = []), i = L.findIndex((x) => x.id === b.id); if (i >= 0) L[i] = b; else L.unshift(b); }
  renderBurstCard();
}
function burstConnSync() {   // keep the card's connector view in step with live connector_state pushes
  if (!S.burst) return;
  const c = S.connector || {}, a = c.account || {};
  const nc = { online: !!c.online, demo: a.demo == null ? null : !!a.demo, version: c.version || null };
  if (JSON.stringify(nc) === JSON.stringify(S.burst.connector)) return;
  S.burst.connector = nc; renderBurstCard();
}
async function burstTest() {
  const pick = S.symbols.includes("EURUSD") ? "EURUSD" : S.symbols[0];
  // optional: an upcoming release whose forecast the test's pre-news analysis reads (each event once)
  const evs = [...new Map(((S.burst && S.burst.upcoming) || []).map((u) => [u.event_id, u])).values()];
  const box = await modal({ title: t("burst_test"), confirm: t("burst_test_go"), body: `<p class="muted">${t("burst_test_d")}</p><div class="grid g2">
    <label class="field"><span>${t("symbol")}</span><select class="input" id="bSym">${S.symbols.map((x) => `<option ${x === pick ? "selected" : ""}>${esc(x)}</option>`).join("")}</select></label>
    <label class="field"><span>${t("burst_offset")}</span><input class="input" type="number" id="bOff" value="2" min="1" max="100" step="0.1"></label></div>
    ${evs.length ? `<label class="field" style="margin-top:14px"><span>${t("burst_test_ev")}</span><select class="input" id="bEv"><option value="">${t("burst_test_ev_none")}</option>
      ${evs.map((u) => `<option value="${esc(u.event_id)}">${esc(u.currency)} · ${esc(u.title)} · ${fmtDT(u.time)}</option>`).join("")}</select><div class="hint">${t("burst_test_ev_d")}</div></label>` : ""}` });
  if (!box) return;
  const off = Math.min(100, Math.max(1, Number(box.querySelector("#bOff").value) || 2)), ev = box.querySelector("#bEv");
  const body = { symbol: box.querySelector("#bSym").value, offset_pips: off };
  if (ev && ev.value) body.event_id = ev.value;
  try { burstUpsert(await api("/api/bursts/test", { method: "POST", body })); toast(t("burst_test_sent"), "gold"); }
  catch (err) { toast(esc(errText(err)), "err"); }
}

// ── dashboard ───────────────────────────────────────────────
// tile kind -> [has a period in days, has a strategy scope]; the server keeps the same list (server/dashboard.py)
const TILE_KINDS = { equity: [0, 0], open_signals: [0, 0], open_trades: [0, 1], pnl: [1, 1], win_rate: [1, 1], trades: [1, 1],
  profit_factor: [1, 1], avg_trade: [1, 1], sig_win_rate: [1, 0], sig_pips: [1, 0] };
// a trade's strategy (server/dashboard.py SCOPES): long = trend signals, news = AI news signals, custom = custom-rules signals,
// rapid = Rapid runs (v1.3.0: at news and on short-term signals)
const TILE_SCOPES = ["all", "long", "short", "rapid", "burst", "news", "custom"], TILE_MAX = 12, TILE_ONCE = new Set(["equity", "open_signals"]);
const TILE_DEF = [{ k: "equity" }, { k: "open_signals" }, { k: "sig_win_rate", days: 30 }, { k: "sig_pips", days: 30 }, { k: "pnl", days: 30, scope: "all" }];
const dashTiles = () => (S.settings && S.settings.dashboard && Array.isArray(S.settings.dashboard.tiles) ? S.settings.dashboard.tiles : TILE_DEF);
async function loadKpiData(tiles, openSignals) {
  // one request per distinct period: two tiles on 30 days share it
  const tradeDays = [...new Set(tiles.filter((x) => TILE_KINDS[x.k] && TILE_KINDS[x.k][1]).map((x) => x.days || 30))];
  const sigDays = [...new Set(tiles.filter((x) => x.k.startsWith("sig_")).map((x) => x.days))];
  const [trades, ...sig] = await Promise.all([
    tiles.some((x) => TILE_KINDS[x.k] && TILE_KINDS[x.k][1]) ? api(`/api/trades/stats?days=${tradeDays.join(",") || 30}`) : null,
    ...sigDays.map((d) => api(`/api/signals/stats?days=${d}`))]);
  return { trades, sig: Object.fromEntries(sigDays.map((d, i) => [d, sig[i]])), openSignals };
}
const daysLabel = (d) => (d === 1 ? t("k_day1") : t("k_days", { n: d }));
function tileHtml(x, D) {
  const box = (label, value, sub, extra = "") => `<div class="card kpi ${extra}"><div class="label">${label}</div>${value}<div class="sub">${sub}</div></div>`;
  const cur = (S.connector.account || {}).currency;
  // my trades' numbers are about one account, demo or real (the server picks the one the connector is on); the label says which
  const scope = x.scope || "all", acct = D.trades ? ` · ${t(D.trades.account === "real" ? "real" : "demo")}` : "";
  const sc = t("scope_" + scope) + acct, per = x.days ? daysLabel(x.days) : "";
  if (x.k === "equity") return `<div class="card kpi kConn">${connKpiHtml()}</div>`;
  if (x.k === "open_signals") return box(t("k_open_signals"), `<div class="value"><b class="kOpen">${D.openSignals}</b></div>`, `${t("exec_title")}: ${t("exec_" + (S.settings?.execution || "notify"))}`);
  if (x.k === "open_trades") return box(t("k_open_trades"), `<div class="value"><b>${D.trades ? D.trades.open[scope] ?? "—" : "—"}</b></div>`, sc);
  if (x.k.startsWith("sig_")) {
    const all = Object.values(((D.sig[x.days] || {}).modes) || {}), closed = all.reduce((a, m) => a + m.closed, 0), wins = all.reduce((a, m) => a + m.wins, 0);
    const pips = all.reduce((a, m) => a + m.pips, 0);
    return x.k === "sig_pips" ? box(t("k_sig_pips"), `<div class="value num ${cls(pips)}"><b>${signed(pips)}</b></div>`, per)
      : box(t("k_sig_win"), `<div class="value"><b>${closed ? fmtN((100 * wins) / closed, 0) + "%" : "—"}</b></div>`, `${wins}/${closed} · ${per}`);
  }
  const n = ((D.trades && D.trades.periods[String(x.days)]) || {})[scope] || {};
  const sub = `${t("scope_" + scope)} · ${per}${acct}`;
  if (x.k === "pnl") return box(t("k_pnl"), `<div class="value num ${cls(n.pnl)}"><b>${n.closed ? fmtMoney(n.pnl, cur) : "—"}</b></div>`, sub);
  if (x.k === "win_rate") return box(t("k_win_rate"), `<div class="value"><b>${n.win_rate == null ? "—" : fmtN(n.win_rate, 0) + "%"}</b></div>`, `${n.wins || 0}/${n.closed || 0} · ${sub}`);
  if (x.k === "trades") return box(t("k_trades"), `<div class="value"><b>${n.closed ?? "—"}</b></div>`, sub);
  if (x.k === "profit_factor") return box(t("k_pf"), `<div class="value num ${n.profit_factor == null ? "" : cls(n.profit_factor - 1)}"><b>${n.profit_factor == null ? "—" : fmtN(n.profit_factor, 2)}</b></div>`, `${t("k_pf_hint")} · ${sub}`);
  if (x.k === "avg_trade") return box(t("k_avg"), `<div class="value num ${cls(n.avg)}"><b>${n.avg == null ? "—" : fmtMoney(n.avg, cur)}</b></div>`, sub);
  return "";
}
const kpisHtml = (tiles, D) => (tiles.length ? `<div class="grid kpis dyn" id="kpis">${tiles.map((x) => tileHtml(x, D)).join("")}</div>`
  : `<div class="card empty" id="kpis">${t("tile_none")}</div>`);
let kpiTimer = null;
function refreshKpis() {   // a trade opened or closed: only the numbers are fetched again, at most every 3 s
  if (kpiTimer) return;
  kpiTimer = setTimeout(async () => {
    kpiTimer = null;
    const box = $("#kpis"); if (S.page !== pageDashboard || !box) return;
    const tiles = dashTiles();
    if (!tiles.some((x) => TILE_KINDS[x.k] && TILE_KINDS[x.k][1])) return;
    try { const D = await loadKpiData(tiles, Number(($(".kOpen") || {}).textContent) || 0); const now = $("#kpis"); if (now && S.page === pageDashboard) now.outerHTML = kpisHtml(tiles, D); } catch { /* next one */ }
  }, 3000);
}
async function dashCustomize() {
  let tiles = dashTiles().map((x) => ({ ...x }));
  const rowHtml = (x, i) => { const [hd, hs] = TILE_KINDS[x.k]; return `<div class="tile-row" data-i="${i}">
      <select class="input" data-f="k" aria-label="${t("dash_custom_t")}">${Object.keys(TILE_KINDS).map((k) => `<option value="${k}" ${k === x.k ? "selected" : ""} ${k !== x.k && TILE_ONCE.has(k) && tiles.some((y) => y.k === k) ? "disabled" : ""}>${t("tile_k_" + k)}</option>`).join("")}</select>
      ${hs ? `<select class="input" data-f="scope" aria-label="${t("tile_scope")}">${TILE_SCOPES.map((s) => `<option value="${s}" ${s === (x.scope || "all") ? "selected" : ""}>${t("scope_" + s)}</option>`).join("")}</select>` : "<span></span>"}
      ${hd ? `<label class="tile-days"><input class="input" type="number" min="1" max="365" step="1" list="tileDays" data-f="days" value="${x.days || 30}" aria-label="${t("tile_days")}"><span>${t("tile_days").toLowerCase()}</span></label>` : "<span></span>"}
      <div class="tile-btns"><button class="btn sm" data-t="up" title="${t("tile_up")}" ${i ? "" : "disabled"}>↑</button><button class="btn sm" data-t="down" title="${t("tile_down")}" ${i < tiles.length - 1 ? "" : "disabled"}>↓</button><button class="btn sm" data-t="del" title="${t("tile_del")}">✕</button></div></div>`; };
  const draw = (m) => {
    $("#tileList", m).innerHTML = tiles.map(rowHtml).join("") || `<div class="empty" style="padding:12px">${t("tile_none")}</div>`;
    $("[data-t=add]", m).disabled = tiles.length >= TILE_MAX;
  };
  const box = await modal({ title: t("dash_custom_t"), confirm: t("save"), body: `<p class="muted" style="margin-top:0">${t("dash_custom_d")}</p>
    <datalist id="tileDays">${[1, 7, 14, 30, 90, 180, 365].map((d) => `<option value="${d}">`).join("")}</datalist>
    <div class="tile-list" id="tileList"></div>
    <div class="row" style="margin-top:12px"><button class="btn sm" data-t="add">+ ${t("tile_add")}</button><button class="btn sm" data-t="reset">${t("tile_reset")}</button>
      <span class="faint" style="font-size:12px">${t("tile_max", { n: TILE_MAX })}</span></div>`,
    onOpen: (m) => {
      draw(m);
      m.addEventListener("click", (e) => {
        const b = e.target.closest("[data-t]"); if (!b) return;
        const i = Number((b.closest(".tile-row") || {}).dataset?.i);
        if (b.dataset.t === "add" && tiles.length < TILE_MAX) tiles.push({ k: "pnl", days: 30, scope: "all" });
        else if (b.dataset.t === "reset") tiles = TILE_DEF.map((x) => ({ ...x }));
        else if (b.dataset.t === "del") tiles.splice(i, 1);
        else if (b.dataset.t === "up" && i > 0) [tiles[i - 1], tiles[i]] = [tiles[i], tiles[i - 1]];
        else if (b.dataset.t === "down" && i < tiles.length - 1) [tiles[i + 1], tiles[i]] = [tiles[i], tiles[i + 1]];
        draw(m);
      });
      m.addEventListener("change", (e) => {
        const f = e.target.dataset.f, row = e.target.closest(".tile-row"); if (!f || !row) return;
        const x = tiles[Number(row.dataset.i)];
        if (f === "days") { x.days = Math.min(365, Math.max(1, Math.round(Number(e.target.value) || 30))); e.target.value = x.days; return; }
        if (f === "scope") { x.scope = e.target.value; return; }
        const [hd, hs] = TILE_KINDS[e.target.value];   // another kind: keep what still applies
        tiles[Number(row.dataset.i)] = { k: e.target.value, ...(hd ? { days: x.days || 30 } : {}), ...(hs ? { scope: x.scope || "all" } : {}) };
        draw(m);
      });
    } });
  if (!box) return;
  saveSettings({ dashboard: { tiles } });
  if (S.page === pageDashboard) route();
}
async function pageDashboard(el) {
  const tiles = dashTiles();
  const [open, D, prices, an, cal, bursts] = await Promise.all([
    api("/api/signals?status=open&limit=20"), loadKpiData(tiles, 0), api("/api/market/prices"),
    api("/api/market/analysis"), api("/api/market/calendar?days_back=0&days_ahead=2&impact=High"), loadBursts()]);
  S.burst = bursts; D.openSignals = open.length;
  const soon = cal.filter((e) => new Date(e.time) > Date.now() - 3600e3 && new Date(e.time) < Date.now() + 86400e3);
  el.innerHTML = head(t("dashboard"), `${t("hello")}, <b>${esc((S.me.name || S.me.email).split(" ")[0])}</b>`, t("dash_sub"),
    `<button class="btn sm" data-action="dash-custom">${icon("dash")}${t("dash_custom")}</button>`) + `
    ${kpisHtml(tiles, D)}
    ${mktConf().widget ? `<div class="card" id="sessCard" style="margin-top:14px"></div>` : ""}
    ${bursts ?`<div class="card" id="burstCard" style="margin-top:14px">${burstCardHtml()}</div>` : ""}
    <div class="grid main-side" style="margin-top:14px">
      <div class="card"><div class="card-head"><h2>${t("live_signals")}</h2><a href="#/signals">${t("see_all")} →</a></div>
        <div class="sig-list" id="liveSignals">${open.length ? open.map((s) => sigCard(s)).join("") : `<div class="empty">${t("no_open_signals")}</div>`}</div></div>
      <div class="stack">
        <div class="card"><div class="card-head"><h2>${t("ai_view")}</h2>${an.available ? `<span class="faint" style="font-size:12px">${ago(an.created_at)}</span>` : ""}</div>
          <div id="aiSummary">${an.available ? `<p class="muted" style="margin:0">${esc(S.lang === "ro" ? an.summary_ro : an.summary_en)}</p>` : `<p class="muted" style="margin:0">${an.ai_enabled ? t("ai_waiting") : t("ai_off")}</p>`}</div></div>
        <div class="card"><div class="card-head"><h2>${t("upcoming")}</h2><a href="#/market">${t("calendar")} →</a></div>
          ${soon.length ? soon.slice(0, 8).map((e) => `<div class="ev-mini"><span class="num faint">${fmtTime(e.time)}</span><span class="cur">${esc(e.currency)}</span><span>${esc(e.title)}</span><span class="faint">${ago(e.time)}</span></div>`).join("") : `<div class="empty" style="padding:16px">${t("no_events")}</div>`}</div>
      </div>
    </div>
    <div class="card" style="margin-top:14px"><div class="card-head"><h2>${t("market_pulse")}</h2></div><div class="prices" id="prices">${pricesHtml(prices)}</div></div>
    <div class="card" style="margin-top:14px"><div class="card-head"><h2>${t("connector_log")}</h2></div><div id="connLog">${logList(S.log)}</div></div>`;
  S.prices = prices; updateMktPill();   // fills the sessions card now, not in a second
}
function connKpiHtml() {   // dashboard connector card; redrawn in place on every connector_state push
  const acct = S.connector.online ? S.connector.account || {} : null;
  return `<div class="label">${t("k_connector")}</div>${acct ? `<div class="value num">${fmtMoney(acct.equity, acct.currency)}</div><div class="sub">${t("k_balance")} ${fmtMoney(acct.balance, acct.currency)} · ${acct.demo === false ? t("real") : t("demo")}</div>`
    : `<div class="value" style="font-size:18px;margin-top:10px">${t("no_connector")}</div><div class="sub"><a href="#/connector">${t("setup_connector")} →</a></div>`}`;
}
function pricesHtml(prices) {
  return prices.map((p) => `<div class="px" data-px="${esc(p.symbol)}"><div class="row between"><span class="s">${esc(p.symbol)}</span>
    <span class="num ${cls(p.change_pips)}" style="font-size:12px">${p.last == null ? "" : signed(p.change_pips) + " p"}</span></div>
    <div class="p">${fmtP(p.symbol, p.last)}</div>${spark(p.spark)}</div>`).join("");
}

// ── signals ─────────────────────────────────────────────────
async function pageSignals(el, q) {
  const status = q.get("status") || "open", mode = q.get("mode") || "";
  const [list, stats, closedAll] = await Promise.all([
    api(`/api/signals?status=${status}&mode=${mode}&limit=200`), api("/api/signals/stats?days=30"),
    api(`/api/signals?status=closed&mode=${mode}&limit=500`)]);
  const seg = (key, val, opts) => `<div class="seg">${opts.map(([v, l]) => `<button class="${val === v ? "on" : ""}" data-action="filter" data-k="${key}" data-v="${v}">${l}</button>`).join("")}</div>`;
  const cum = []; let run = 0;
  closedAll.filter((s) => s.result_pips != null).sort((a, b) => new Date(a.closed_at) - new Date(b.closed_at)).forEach((s) => { run += s.result_pips; cum.push({ x: s.closed_at, y: run }); });
  el.innerHTML = head(t("signals"), splitTitle(t("signals")), "",
    seg("status", status, [["open", t("open")], ["closed", t("closed")], ["all", t("all")]]) +
    seg("mode", mode, [["", t("all")], ["trend", t("mode_trend")], ["custom", t("mode_custom")], ["news_ai", t("mode_news_ai")]])) + `
    <div class="grid g3">${["trend", "custom", "news_ai"].map((m) => { const s = stats.modes[m]; return `<div class="card kpi">
      <div class="label">${t("mode_" + m)}</div><div class="value"><b>${s.win_rate == null ? "—" : fmtN(s.win_rate, 0) + "%"}</b> <span style="font-size:14px" class="num ${cls(s.pips)}">${signed(s.pips)} ${t("pips")}</span></div>
      <div class="sub">${t("total")} ${s.total} · ${t("open")} ${s.open} · ${t("wins")} ${s.wins}/${s.closed}</div></div>`; }).join("")}</div>
    ${cum.length > 1 ? `<div class="card" style="margin-top:14px"><div class="card-head"><h2>${t("result")} · ${t("pips")}</h2><span class="num ${cls(run)}" style="font-weight:600">${signed(run)}</span></div>${lineChart(cum)}</div>` : ""}
    <div class="card" style="margin-top:14px"><div class="sig-list" id="sigList">${list.length ? list.map((s) => sigCard(s)).join("") : `<div class="empty">${t("no_open_signals")}</div>`}</div></div>`;
}

// ── market ──────────────────────────────────────────────────
// where the calendar comes from, as the page note names it: the furthest-ahead row's source (after a switch of
// source the old one's rows are only the earlier ones)
const calSource = (cal) => {
  const s = (cal.length && cal[cal.length - 1].source) || "forexfactory";
  return I18N.en["cal_src_" + s] ? t("cal_src_" + s) : s;
};
async function pageMarket(el, q) {
  const impact = q.get("impact") ?? "High,Medium";
  const [cal, news, an, bd] = await Promise.all([api(`/api/market/calendar?days_back=1&days_ahead=6&impact=${impact}`), api("/api/market/news?limit=80"), api("/api/market/analysis"), loadBursts()]);
  // a run trades (its chip gold) on its own kind's switch: Rapid's news runs, or the classic burst's; the admin's above both
  const now = Date.now(), bpair = new Map(), rp = rpSet();
  const kindOn = (k) => S.burstApp.enabled && (k === "rapid" ? rp.enabled && (rp.triggers || {}).news !== false : burstSet().enabled);
  for (const u of (bd && bd.upcoming) || []) {   // a release may start Rapid runs and a classic burst, each on its own pair
    const v = bpair.get(u.event_id) || { syms: [], on: false };
    v.syms.push(u.kind === "rapid" ? `${u.symbol} (${t("v_rapid")})` : u.symbol); v.on ||= kindOn(u.kind);
    bpair.set(u.event_id, v);
  }
  let lastDay = "", rows = "";
  for (const e of cal) {
    const d = new Date(e.time), day = d.toLocaleDateString(locale(), { weekday: "long", day: "numeric", month: "long" });
    if (day !== lastDay) { rows += `<tr><td colspan="7" class="cal-day">${esc(day)}</td></tr>`; lastDay = day; }
    const past = d.getTime() < now - 60e3, soon = !past && d.getTime() - now < 3600e3;
    rows += `<tr class="${past ? "past" : ""} ${soon ? "soon" : ""}"><td class="num">${fmtTime(e.time)}</td><td class="cur">${esc(e.currency)}</td><td>${impactBars(e.impact)}</td>
      <td class="wrap">${esc(e.title)}${bpair.has(e.id) ? ` <span class="chip ${bpair.get(e.id).on ? "gold" : ""}" title="${esc(t("burst_title"))}">⚡ ${t("burst_chip")} · ${esc(bpair.get(e.id).syms.join(", "))}</span>` : ""}</td><td class="r num"><b>${esc(e.actual || "")}</b></td><td class="r num muted">${esc(e.forecast || "")}</td><td class="r num faint">${esc(e.previous || "")}</td></tr>`;
  }
  const effChip = (ef) => `<span class="chip ${ef.effect === "positive" ? "up" : ef.effect === "negative" ? "down" : ""}">${esc(ef.currency)} ${ef.effect === "positive" ? "▲" : ef.effect === "negative" ? "▼" : "•"}</span>`;
  el.innerHTML = head(t("market"), splitTitle(t("market")), t("market_note", { src: esc(calSource(cal)) }),
    `<div class="seg"><button class="${impact === "High" ? "on" : ""}" data-action="filter" data-k="impact" data-v="High">${t("filter_high")}</button>
     <button class="${impact !== "High" ? "on" : ""}" data-action="filter" data-k="impact" data-v="High,Medium">${t("filter_all")}</button></div>`) + `
    <div class="grid main-side">
      <div class="card"><div class="card-head"><h2>${t("calendar")}</h2><span class="faint" style="font-size:12px">${esc(Intl.DateTimeFormat().resolvedOptions().timeZone || "")}</span></div>
        <div class="table-wrap"><table><thead><tr><th>${t("time")}</th><th>${t("currency")}</th><th>${t("impact")}</th><th>${t("event")}</th><th class="r">${t("actual")}</th><th class="r">${t("forecast")}</th><th class="r">${t("previous")}</th></tr></thead>
        <tbody>${rows || `<tr><td colspan="7" class="empty">—</td></tr>`}</tbody></table></div></div>
      <div class="stack">
        <div class="card"><div class="card-head"><h2>${t("biases")}</h2>${an.available ? `<span class="faint" style="font-size:12px">${t("updated", { t: ago(an.created_at) })}</span>` : ""}</div>
          ${an.available ? `<p class="muted">${esc(S.lang === "ro" ? an.summary_ro : an.summary_en)}</p>` + an.biases.map((b) => `<div class="bias">
            <b>${esc(b.symbol)}</b><span>${b.direction === "neutral" ? `<span class="chip">${t("neutral")}</span>` : `<span class="chip ${b.direction === "buy" ? "up" : "down"}">${t(b.direction)}</span>`} <span class="faint" style="font-size:12px">${t("horizon", { h: b.horizon_hours })}</span></span>
            <span class="conf"><span class="bar"><i style="width:${b.confidence}%"></i></span>${b.confidence}%</span>
            <div class="why">${esc(S.lang === "ro" ? b.rationale_ro : b.rationale_en)}</div></div>`).join("")
            : `<p class="muted">${an.ai_enabled ? t("ai_waiting") : t("ai_off")}</p>`}</div>
        <div class="card"><div class="card-head"><h2>${t("news")}</h2></div><div class="news">${news.map((n) => `<div class="news-item">
          <a class="t" href="${esc(safeUrl(n.link))}" target="_blank" rel="noopener noreferrer">${esc(n.title)}</a>
          <div class="src">${esc(n.source)} · ${ago(n.published_at)}</div>
          ${n.ai ? `<div class="ai">${(n.ai.effects || []).map(effChip).join("")}<span class="chip ${n.ai.impact === "high" ? "warn" : ""}">${esc(n.ai.impact)}</span><span class="n">${esc(n.ai.note)}</span></div>`
            : n.analyzed ? "" : `<div class="ai"><span class="chip">${t("not_analyzed")}</span></div>`}</div>`).join("") || `<div class="empty">—</div>`}</div></div>
      </div></div>`;
}

// ── trades ──────────────────────────────────────────────────
const TRADES_SHOWN = 200;   // newest rows of the history (the numbers on top count every trade of their period)
async function pageTrades(el, q) {
  const scope = TILE_SCOPES.includes(q.get("strategy")) ? q.get("strategy") : "all";
  const [r, st, bd] = await Promise.all([api(`/api/trades?strategy=${scope}&limit=${TRADES_SHOWN}`), api("/api/trades/stats?days=30"), loadBursts()]);
  const pos = (S.connector && S.connector.positions) || [], cur = (S.connector.account || {}).currency, bursts = bd && bd.bursts;
  const bTag = (id) => (id ? ` <span class="chip gold">⚡ #${esc(id)}</span>` : "");
  const strat = (x) => (x.strategy ? `<span class="chip">${esc(I18N.en["scope_" + x.strategy] ? t("scope_" + x.strategy) : x.strategy)}</span>` : "—");
  // P&L and win rate: the stats of the last 30 days for the strategy picked, on one account; the label says what they cover
  const n = st.periods["30"][scope] || {}, acct = (S.connector.account || null), cover = `${t("scope_" + scope)} · ${daysLabel(30)} · ${t(st.account === "real" ? "real" : "demo")}`;
  el.innerHTML = head(t("trades"), splitTitle(t("trades")), t("trades_sub"),
    `<div class="seg" data-k="seg" role="group" aria-label="${t("strategy")}">${TILE_SCOPES.map((s) => `<button class="${s === scope ? "on" : ""}" data-action="filter" data-k="strategy" data-v="${s}">${t("scope_" + s)}</button>`).join("")}</div>`) + `
    <div class="grid kpis">
      <div class="card kpi"><div class="label">${t("pnl")}</div><div class="value num ${cls(n.pnl)}"><b>${n.closed ? fmtMoney(n.pnl, cur) : "—"}</b></div><div class="sub">${cover}</div></div>
      <div class="card kpi"><div class="label">${t("win_rate")}</div><div class="value"><b>${n.win_rate == null ? "—" : fmtN(n.win_rate, 0) + "%"}</b></div><div class="sub">${n.wins || 0}/${n.closed || 0} · ${cover}</div></div>
      <div class="card kpi"><div class="label">${t("open_positions")}</div><div class="value"><b>${pos.length}</b></div><div class="sub">MT5${acct ? ` · ${t(acct.demo === false ? "real" : "demo")}` : ""}</div></div>
      <div class="card kpi"><div class="label">${t("k_connector")}</div><div class="value" style="font-size:18px;margin-top:10px">${S.connector.online ? `<span class="up">${t("online")}</span>` : `<a href="#/connector">${t("no_connector")}</a>`}</div></div>
    </div>
    <div class="card" style="margin-top:14px"><div class="card-head"><h2>${t("open_positions")}</h2></div>
      ${pos.length ? `<div class="table-wrap" data-k="pos"><table><thead><tr><th>${t("symbol")}</th><th>${t("side")}</th><th class="r">${t("lots")}</th><th class="r">${t("entry")}</th><th class="r">${t("sl")}</th><th class="r">${t("tp")}</th><th class="r">${t("price")}</th><th class="r">${t("profit")}</th><th></th></tr></thead><tbody>
        ${pos.map((p) => `<tr data-pos="${esc(p.ticket)}"><td><b>${esc(p.symbol)}</b>${p.signal_id ? ` <span class="faint">#${esc(p.signal_id)}</span>` : ""}${bTag(p.burst_id)}</td><td class="side-tag ${p.side === "buy" ? "up" : "down"}">${p.side === "buy" ? t("buy") : t("sell")}</td>
          <td class="r" data-f="lots">${esc(p.lots)}</td><td class="r" data-f="entry">${fmtP(p.symbol, p.entry_price)}</td><td class="r" data-f="sl">${fmtP(p.symbol, p.sl)}</td><td class="r" data-f="tp">${fmtP(p.symbol, p.tp)}</td><td class="r" data-f="price">${fmtP(p.symbol, p.price)}</td>
          <td class="r ${cls(p.profit)}" data-f="profit">${fmtMoney(p.profit, cur)}</td><td class="r"><button class="btn sm danger" data-action="closepos" data-ticket="${esc(p.ticket)}">${t("close")}</button></td></tr>`).join("")}</tbody></table></div>`
        : `<div class="empty">—</div>`}</div>
    ${bursts ? `<div class="card" style="margin-top:14px"><div class="card-head"><h2>${icon("bolt")}${t("bursts")}</h2><a href="#/settings">${t("settings")} →</a></div>
      ${bursts.length ? `<div class="table-wrap" data-k="bursts"><table><thead><tr><th>${t("time")}</th><th>${t("event")}</th><th>${t("symbol")}</th><th>${t("burst_kind")}</th><th>${t("status")}</th><th>${t("side")}</th><th class="r">${t("burst_of")}</th><th class="r">${t("pnl")}</th><th></th></tr></thead><tbody>
        ${bursts.map((b) => `<tr><td class="muted">${fmtDT(b.event_time)}</td><td class="wrap">${esc(b.event_title)}${b.test ? ` <span class="chip">${t("burst_test_chip")}</span>` : ""}${b.analysis ? `<div class="faint" style="font-size:12px">${esc(anLine(b.analysis))}</div>` : ""}${b.reason ? `<div class="faint" style="font-size:12px">${esc(b.reason)}</div>` : ""}</td>
          <td><b>${esc(b.symbol)}</b> <span class="faint">#${esc(b.id)}</span></td><td>${kindChip(b)}</td><td>${burstChip(b)}</td><td>${burstSide(b.side)}</td>
          <td class="r">${Number(b.orders) || 0} / ${Number(b.fills) || 0}</td><td class="r ${cls(b.pnl)}">${b.pnl == null ? "—" : fmtMoney(b.pnl, cur)}</td>
          <td class="r">${BURST_FINAL.has(b.status) ? "" : `<button class="btn sm danger" data-action="burst-cancel" data-id="${esc(b.id)}">${t("burst_cancel")}</button>`}</td></tr>`).join("")}</tbody></table></div>`
        : `<div class="empty">${t("burst_none")}</div>`}</div>` : ""}
    <div class="card" style="margin-top:14px"><div class="card-head"><h2>${t("history")}${scope === "all" ? "" : ` · ${t("scope_" + scope)}`}</h2>${r.trades.length >= TRADES_SHOWN ? `<span class="faint" style="font-size:12px">${t("trades_newest", { n: TRADES_SHOWN })}</span>` : ""}</div>
      ${r.trades.length ? `<div class="table-wrap" data-k="hist"><table><thead><tr><th>${t("opened")}</th><th>${t("symbol")}</th><th>${t("strategy")}</th><th>${t("side")}</th><th class="r">${t("lots")}</th><th class="r">${t("entry")}</th><th class="r">${t("price")}</th><th class="r">${t("pnl")}</th><th></th></tr></thead><tbody>
        ${r.trades.map((x) => `<tr><td class="muted">${fmtDT(x.opened_at)}</td><td><b>${esc(x.symbol)}</b>${x.signal_id ? ` <span class="faint">#${esc(x.signal_id)}</span>` : ""}${bTag(x.burst_id)}</td><td>${strat(x)}</td><td class="side-tag ${x.side === "buy" ? "up" : "down"}">${x.side === "buy" ? t("buy") : t("sell")}</td>
          <td class="r">${esc(x.lots)}</td><td class="r">${fmtP(x.symbol, x.entry_price)}</td><td class="r">${x.exit_price == null ? "—" : fmtP(x.symbol, x.exit_price)}</td>
          <td class="r ${cls(x.pnl)}">${x.status === "open" ? `<span class="chip gold">${t("st_open")}</span>` : fmtMoney(x.pnl, cur)}</td><td><span class="chip">${x.demo ? t("demo") : t("real")}</span></td></tr>`).join("")}</tbody></table></div>`
        : `<div class="empty">${scope === "all" ? t("no_trades") : "—"}</div>`}</div>`;
  const seg = $(".page-head .seg", el), on = seg && $(".on", seg);   // on a phone the strategy picked stays in view
  if (on) seg.scrollLeft += on.getBoundingClientRect().left - seg.getBoundingClientRect().left - (seg.clientWidth - on.offsetWidth) / 2;
}

// ── coach ───────────────────────────────────────────────────
async function pageCoach(el, q) {
  const h = await api("/api/coach/history");
  el.innerHTML = head(t("coach"), splitTitle(t("coach")), t("coach_sub"), `<button class="btn sm" data-action="clearchat">${t("clear_chat")}</button>`) + `
    ${h.ai_enabled ? "" : `<div class="banner info" style="margin-bottom:14px">${icon("info")}<div>${t("coach_off")} <a href="#/academy">${t("academy")} →</a></div></div>`}
    <div class="card chat"><div class="msgs" id="msgs">${[{ role: "assistant", content: t("coach_hello") }, ...h.messages].map((m) => `<div class="msg ${m.role}">${esc(m.content)}</div>`).join("")}</div>
      <div class="row" style="margin-bottom:10px"><span class="faint" style="font-size:12px">${t("quick_q")}</span>${["q1", "q2", "q3"].map((k) => `<button class="btn sm" data-action="quickq" data-q="${esc(t(k))}">${esc(t(k))}</button>`).join("")}</div>
      <form id="chatForm"><textarea class="input" name="text" maxlength="2000" placeholder="${esc(t("coach_placeholder"))}" ${h.ai_enabled ? "" : "disabled"}></textarea>
      <button class="btn gold" type="submit" ${h.ai_enabled ? "" : "disabled"}>${t("send")}</button></form>
      <div class="faint" id="coachUsage" style="font-size:12px;margin-top:8px"></div></div>`;
  const msgs = $("#msgs", el); msgs.scrollTop = msgs.scrollHeight;
  const form = $("#chatForm", el), ta = form.text;
  let signalId = q.get("signal") ? Number(q.get("signal")) : null, busy = false;
  if (signalId && h.ai_enabled) { ta.value = S.lang === "ro" ? `Explică-mi semnalul #${signalId}: de ce a fost dat și ce l-ar invalida?` : `Explain signal #${signalId}: why was it issued and what would invalidate it?`; }
  else if (q.get("ask") && h.ai_enabled) ta.value = (S.lang === "ro" ? "Explică-mi mai pe larg: " : "Explain in more depth: ") + q.get("ask");
  ta.addEventListener("keydown", (e) => { if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); form.requestSubmit(); } });
  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    const text = ta.value.trim(); if (busy || !text) return;   // Enter and quick questions submit too: one at a time
    busy = true; ta.value = ""; form.querySelector("button").disabled = true;
    msgs.insertAdjacentHTML("beforeend", `<div class="msg user">${esc(text)}</div><div class="msg assistant typing"><span class="spin" style="display:inline-block;vertical-align:middle"></span></div>`);
    const typing = msgs.lastElementChild; msgs.scrollTop = msgs.scrollHeight;
    try {
      const r = await api("/api/coach/message", { method: "POST", body: { text, signal_id: signalId } });
      signalId = null;
      typing.outerHTML = `<div class="msg assistant">${esc(r.reply)}</div>`;
      if (r.limit) $("#coachUsage", el).textContent = t("used_today", { n: r.used_today, m: r.limit });
    } catch (err) { typing.outerHTML = `<div class="msg assistant" style="color:var(--down)">${esc(errText(err))}</div>`; }
    busy = false; form.querySelector("button").disabled = false; msgs.scrollTop = msgs.scrollHeight; ta.focus();
  });
}
function pageAcademy(el) {
  const L = LESSONS[S.lang] || LESSONS.en;
  el.innerHTML = head(t("academy"), splitTitle(t("academy")), t("academy_sub")) + `<div class="lessons">${L.map(([title, body], i) => `
    <details class="lesson" ${i === 0 ? "open" : ""}><summary><span class="n">${String(i + 1).padStart(2, "0")}</span>${esc(title)}</summary>
    <div class="body">${body}<a class="btn sm" href="#/coach?ask=${encodeURIComponent(title)}">${t("ask_coach")} →</a></div></details>`).join("")}</div>
    <p class="disclaimer" style="text-align:left">${t("disclaimer")}</p>`;
}

// ── settings ────────────────────────────────────────────────
// saveDone: PUTs answered so far (a settings GET answered after one of them is older than that answer)
// saveEpoch: the session the save state belongs to (boot starts a new one: an answer to the one before is dropped)
let saveTimer = null, savePatch = {}, saveSent = {}, saveLoud = false, saveChain = Promise.resolve(), saveDone = 0, saveEpoch = 0;
// Object.assign one level deeper (v1.3.1): a block in a patch (burst, short_term, rapid, modes, ...) adds its keys to
// the block there instead of replacing it, so each card sends only the keys edited on it and two edits waiting in the
// same save both go out. A list or a value replaces. `dst` is changed and returned
const isBlock = (v) => !!v && typeof v === "object" && !Array.isArray(v);
function mergeSettings(dst, ...patches) {
  for (const p of patches) for (const [k, v] of Object.entries(p || {}))
    dst[k] = isBlock(v) ? { ...(isBlock(dst[k]) ? dst[k] : {}), ...v } : v;
  return dst;
}
// a money value goes with its unit, the one this page shows it in: the server reads it in that unit, never in a unit
// changed on another page since (2 % must not become 2 $)
const UNIT_OF = { sl_money: "sl_unit", risk_min: "risk_unit", risk_max: "risk_unit", daily_loss: "loss_unit" };
function withUnits(patch, set) {
  const out = { ...patch };
  for (const k of Object.keys(patch)) { const u = UNIT_OF[k]; if (u && !(u in out) && set[u] !== undefined) out[u] = set[u]; }
  return out;
}
function saveSettings(patch, quiet = false) {   // debounced 350 ms, one PUT at a time: an older reply never undoes a newer edit
  if (!S.settings) return;                       // between sessions: nothing loaded to edit
  mergeSettings(S.settings, patch); mergeSettings(savePatch, patch); saveLoud ||= !quiet;
  clearTimeout(saveTimer);
  saveTimer = setTimeout(() => { saveTimer = null; saveChain = saveChain.then(sendSettings).catch(console.error); }, 350);
}
async function sendSettings() {   // everything edited until now goes out together
  const body = savePatch, loud = saveLoud, ep = saveEpoch; savePatch = {}; saveLoud = false;
  if (!Object.keys(body).length) return;   // already sent with the PUT before
  saveSent = body;
  try {
    const r = await api("/api/me/settings", { method: "PUT", body });
    if (ep !== saveEpoch) return;           // answered after a logout or login: that account's, not this page's
    const gone = S.settings.short_term_note, rGone = S.settings.rapid_note, was = stVariant(), wasOn = stOn();
    // short-term (and Rapid) pairs not picked on this page: the server kept its own list, so a save still to go out
    // shows that one
    for (const key of ["short_term", "rapid"]) {
      const pend = savePatch[key], got = r.settings[key];
      if (pend && "symbols" in pend && !pend.symbols_picked && got) pend.symbols = got.symbols;
    }
    saveSent = {}; saveDone++; S.settings = mergeSettings(r.settings, savePatch); if (loud) toast(t("saved"));
    const c = $("#stCard"), rc = $("#rpCard");
    // pairs picked again (no banner), or the variant the server kept (Rapid and classic exclude each other)
    if (c && (gone !== S.settings.short_term_note || was !== stVariant() || wasOn !== stOn())) c.innerHTML = stCardHtml();
    else if (c) stPicksShow(c);
    if (rc) { rc.classList.toggle("hidden", stVariant() !== "rapid"); if (rGone !== S.settings.rapid_note) rc.innerHTML = rpCardHtml(); else rpPicksShow(rc); }
    burstConnShow();   // the burst's banner counts Rapid on
  } catch (e) {
    if (ep !== saveEpoch) return;
    saveSent = {}; toast(esc(errText(e)), "err");
    // not refused (no answer, a server error) with newer edits waiting: what failed goes out again with them (they win)
    if (!(e.status >= 400 && e.status < 500) && Object.keys(savePatch).length) savePatch = mergeSettings(body, savePatch);
    if (S.page === pageSettings && !saveTimer && !Object.keys(savePatch).length) route();   // refused: show what the server kept
  }
}
// short-term (scalping): the same defaults and ranges as bot/scalp.py; the server clamps again.
// [min, max, step]: each step on the grid of its min, so the defaults and the spinner land on round values
const ST_DEF = { enabled: false, risk_unit: "pct", risk_min: 0.5, risk_max: 2, hold_seconds: 30, loss_unit: "pct", daily_loss: 5, max_open: 2, max_spread_pips: 1.5, symbols: [] };
const ST_UNIT_DEF = { pct: { risk_min: 0.5, risk_max: 2, daily_loss: 5 }, usd: { risk_min: 1, risk_max: 10, daily_loss: 50 } };
const ST_RANGE = { risk: { pct: [0.1, 10, 0.1], usd: [0.1, 100000, 0.1] }, daily_loss: { pct: [0.5, 50, 0.5], usd: [1, 1000000, 1] },
  hold_seconds: [10, 300, 1], max_hold_seconds: [10, 600, 1], max_open: [1, 10, 1], max_spread_pips: [0.1, 10, 0.1] };
const stSet = () => ({ ...ST_DEF, ...(S.settings && S.settings.short_term) });
// only the keys edited, like saveBurst (v1.3.1), and the pairs whenever they are being picked: picked in this edit or
// in a save not answered yet (still to go out or on its way). Those go as the page has them, with symbols_picked:
// whether they count as picked on this page; not, and the server keeps the stored ones (main.py web_pairs_kept).
// Never an empty list on the banner (pairs gone): no choice of every pair, unless a pick is on its way (the banner is
// gone on the server then: an empty list there is every pair, as after its answer); each save decides it again, so a
// later edit re-marks a pick still to go out. No pick anywhere: no pairs sent, the server keeps its own.
// key: short_term, or rapid (its pairs are kept the same way, its banner is rapid_note)
const stPickedIn = (p, key = "short_term") => !!(p[key] && p[key].symbols_picked);
const pairsPicked = (key, patch, list) => ("symbols" in patch || stPickedIn(savePatch, key) || stPickedIn(saveSent, key))
  && !((S.settings || {})[key + "_note"] === "pairs_gone" && !list.length && !stPickedIn(saveSent, key));
const blockPatch = (key, patch, set) => {
  const p = withUnits(patch, set);
  if ("symbols" in patch || stPickedIn(savePatch, key) || stPickedIn(saveSent, key)) {
    p.symbols = "symbols" in patch ? patch.symbols : set.symbols;
    p.symbols_picked = pairsPicked(key, patch, p.symbols);
  }
  return p;
};
const saveShort = (patch) => saveSettings({ short_term: blockPatch("short_term", patch, stSet()) });
function stPicksShow(card) {   // the pair buttons as S.settings has them (a save not picked here keeps the server's)
  const on = stSet().symbols;
  $$("[data-st-sym]", card).forEach((b) => b.classList.toggle("on", on.includes(b.dataset.stSym)));
}
// Rapid (v1.3.0), the short-term variant of many stop-limit orders at once: the defaults, unit defaults and ranges of
// bot/rapid.py (DEFAULTS, UNIT_DEFAULTS, LIMITS; Y: X..MAX_HOLD_TOP), [min, max, step]; the server clamps again. The
// server always sends the whole block, max_hold_seconds included unless the user cleared it (Y follows X)
const RP_DEF = { enabled: false, orders: 10, sl_unit: "pips", risk_unit: "pct", risk_min: 0.2, risk_max: 1, sl_pips: 8, lots: 0.1, tp_min_usd: 10,
  tp_max_usd: 15, limit_pips: 0, offset_pips: 2, expire_seconds: 30, hold_seconds: 30, triggers: { signals: true, news: true }, analysis: true,
  loss_unit: "pct", daily_loss: 5, max_runs: 2, cooldown_seconds: 120, symbols: [] };
const RP_UNIT_DEF = { pct: { risk_min: 0.2, risk_max: 1, daily_loss: 5 }, usd: { risk_min: 10, risk_max: 50, daily_loss: 100 } };
const RP_RANGE = { orders: [1, 40, 1], risk: { pct: [0.05, 5, 0.05], usd: [1, 10000, 0.1] }, sl_pips: [1, 200, 0.1], lots: [0.01, 100, 0.01],
  tp_min_usd: [1, 10000, 0.1], tp_max_usd: [1, 10000, 0.1], limit_pips: [0, 5, 0.1], offset_pips: [0.5, 50, 0.1], expire_seconds: [5, 600, 1],
  hold_seconds: [5, 600, 1], max_hold_seconds: [5, 1800, 1], daily_loss: { pct: [0.5, 50, 0.5], usd: [1, 1000000, 1] }, max_runs: [1, 5, 1],
  cooldown_seconds: [0, 3600, 1] };
const rpSet = () => ({ ...RP_DEF, ...(S.settings && S.settings.rapid) });
const saveRapid = (patch) => saveSettings({ rapid: blockPatch("rapid", patch, rpSet()) });   // as saveShort
function rpPicksShow(card) {
  const on = rpSet().symbols;
  $$("[data-rp-sym]", card).forEach((b) => b.classList.toggle("on", on.includes(b.dataset.rpSym)));
}
// the variant the short-term card shows: Rapid while it is on, classic while short-term is, else the one picked last on
// this page (both off). Each draw of the card keeps it, so switching off leaves the variant that was on
const stVariant = () => (rpSet().enabled ? "rapid" : stSet().enabled ? "classic" : S.stVariant || "classic");
const stOn = () => !!(stSet().enabled || rpSet().enabled);
// a number input's value held to its min..max (whole for step 1), written back to it
const numIn = (inp) => {
  let v = Math.min(Number(inp.max), Math.max(Number(inp.min), Number(inp.value) || Number(inp.min)));
  if (inp.step === "1") v = Math.round(v);
  inp.value = v;
  return v;
};
// X (hold_seconds) set to v: Y (max_hold_seconds) is never under it, raised to it (never swapped) as the server does;
// a missing Y follows X. The Y input's minimum follows too
function holdFollow(card, sel, block, v, patch) {
  if (block.max_hold_seconds != null && block.max_hold_seconds < v) patch.max_hold_seconds = v;
  const y = $(sel, card);
  if (y) { y.min = v; if (block.max_hold_seconds == null || block.max_hold_seconds < v) y.value = v; }
}
// the value a card's number field saves (numIn), but a Y (max_hold_seconds) cleared is null: the server removes it and
// Y follows X again, as up to v1.2.0 (the field shows X). A Y left alone is sent as served: the stored one is kept
const fieldIn = (inp, key, block) => {
  if (key !== "max_hold_seconds" || inp.value !== "" || (inp.validity && inp.validity.badInput)) return numIn(inp);
  inp.value = block.hold_seconds;
  return null;
};
const holdFields = (attr, b, [xa, xb], [, yb], needY = "") => {   // X and Y, a missing Y shown as X
  const x = b.hold_seconds, y = b.max_hold_seconds ?? x;
  const f = (key, label, v, a, z, extra) => `<label class="field"><span>${t(label)}${extra}</span><input class="input" type="number" step="1" min="${a}" max="${z}" ${attr}="${key}" value="${esc(v)}"></label>`;
  return f("hold_seconds", "hold_x", x, xa, xb, "") + f("max_hold_seconds", "hold_y", y, x, yb, needY);
};
const need130 = () => (old130() ? ` <span class="chip warn">${t("need_130")}</span>` : "");
const stVerOk = (v) => { const m = /^(\d+)\.(\d+)/.exec(v || ""); return !!m && (+m[1] > 1 || (+m[1] === 1 && +m[2] >= 2)); };   // scalps need connector ≥ 1.2.0
function stMoney(st, strength) {   // what one trade risks at its stop, for the preview (bot/scalp.py risk_money)
  const a = S.connector.online && S.connector.account, bal = a ? Number(a.balance) || 0 : 0;
  if (!a) return null;
  const r = st.risk_min + (st.risk_max - st.risk_min) * strength, money = st.risk_unit === "pct" ? (bal * r) / 100 : r;
  return Math.min(money, bal * 0.1);
}
function stCardHtml() {
  const st = stSet(), c = S.connector, cur = (c.account || {}).currency;
  const [lo, hi, step] = ST_RANGE.risk[st.risk_unit], [llo, lhi, lstep] = ST_RANGE.daily_loss[st.loss_unit];
  const unitSeg = (key, val) => `<div class="seg" data-st-seg="${key}">${["pct", "usd"].map((u) => `<button class="${val === u ? "on" : ""}" data-v="${u}">${t("st_unit_" + u)}</button>`).join("")}</div>`;
  const nf = (key, label, v, [a, b, s]) => `<label class="field"><span>${t(label)}</span><input class="input" type="number" step="${s}" min="${a}" max="${b}" data-st="${key}" value="${esc(v)}"></label>`;
  const pLo = stMoney(st, 0), pHi = stMoney(st, 1), on = stOn(), variant = S.stVariant = stVariant(), rapid = variant === "rapid";
  const vSeg = `<div class="seg" data-st-variant>${["classic", "rapid"].map((v) => `<button class="${variant === v ? "on" : ""}" data-v="${v}">${t("v_" + v)}</button>`).join("")}</div>`;
  return `<div class="card-head"><h2>${icon("bolt")}${t("st_title")}</h2>${c.online && !stVerOk(c.version) ? `<span class="chip warn">${t("st_need")}</span>` : ""}</div>
    <div class="mode-row" style="padding-top:0"><label class="switch"><input type="checkbox" data-st-on ${on ? "checked" : ""}><i></i></label>
      <div class="txt"><b>${t("st_switch")}</b><div class="d" id="stSwitchD">${rapid ? t("rp_switch_d") : t("st_switch_d", { s: st.hold_seconds })}</div></div></div>
    <div class="row" style="margin:12px 0 0;gap:10px"><span class="muted" style="font-size:12.5px">${t("st_variant")}</span>${vSeg}${rapid ? need130() : ""}</div>
    <p class="hint" style="margin:6px 0 0">${t("st_variant_d")}</p>
    ${(S.settings || {}).short_term_note === "pairs_gone" ? `<div class="banner" style="margin-top:12px">${icon("alert")}<div>${t("st_pairs_gone")}</div></div>` : ""}
    <div class="row" style="margin:14px 0 8px;gap:10px"><span class="muted" style="font-size:12.5px">${t("st_risk_unit")}</span>${unitSeg("risk_unit", st.risk_unit)}</div>
    <div class="form-grid">${nf("risk_min", "st_risk_min", st.risk_min, [lo, hi, step])}${nf("risk_max", "st_risk_max", st.risk_max, [lo, hi, step])}
      ${holdFields("data-st", st, ST_RANGE.hold_seconds, ST_RANGE.max_hold_seconds, need130())}${nf("max_open", "st_max_open", st.max_open, ST_RANGE.max_open)}${nf("max_spread_pips", "st_spread", st.max_spread_pips, ST_RANGE.max_spread_pips)}</div>
    <p class="hint" style="margin:10px 0 0">${t("hold_d")}</p>
    <p class="hint" style="margin:6px 0 0">${pipScaleText()}</p>
    <p class="hint" id="stPreview" style="margin:6px 0 0">${pLo == null ? t("st_preview_off") : t("st_preview", { b: fmtMoney(c.account.balance, cur), lo: fmtMoney(pLo, cur), hi: fmtMoney(pHi, cur) })}</p>
    <div class="row" style="margin:16px 0 8px;gap:10px"><span class="muted" style="font-size:12.5px">${t("st_loss")}</span>${unitSeg("loss_unit", st.loss_unit)}</div>
    <div class="form-grid">${nf("daily_loss", st.loss_unit === "pct" ? "st_loss_pct" : "st_loss_usd", st.daily_loss, [llo, lhi, lstep])}</div>
    <div class="row" style="margin:16px 0 8px"><span class="muted" style="font-size:12.5px">${t("st_pairs")}</span><span class="faint" style="font-size:12px">${t("st_pairs_hint")}${old130() ? ` · ${t("pairs_130")}` : ""}</span></div>
    <div class="sym-picks">${S.symbols.map((x) => `<button class="${st.symbols.includes(x) ? "on" : ""}" data-st-sym="${esc(x)}">${esc(x)}</button>`).join("")}</div>
    <div class="banner" style="margin-top:16px">${icon("alert")}<div>${t("st_warn")}</div></div>
    <div class="banner info" style="margin-top:12px">${icon("info")}<div><b>${t("st_how")}</b><ul class="burst-notes">${["st_n1", "st_n2", "st_n3", "st_n4", "st_n5", "st_n6"].map((k) => `<li>${t(k)}</li>`).join("")}</ul></div></div>`;
}
function stWire(card) {   // listeners live on the card, so redrawing its insides keeps them
  card.addEventListener("change", (e) => {
    const inp = e.target;
    // the switch turns on (or off) the variant shown: the server turns the other one off
    if (inp.matches("[data-st-on]")) return stVariant() === "rapid" ? saveRapid({ enabled: inp.checked }) : saveShort({ enabled: inp.checked });
    if (!inp.dataset.st) return;
    const v = fieldIn(inp, inp.dataset.st, stSet()), patch = { [inp.dataset.st]: v };
    if (inp.dataset.st === "hold_seconds") {
      holdFollow(card, "[data-st=max_hold_seconds]", stSet(), v, patch);
      const d = $("#stSwitchD", card); if (d && stVariant() === "classic") d.innerHTML = t("st_switch_d", { s: v });
    }
    saveShort(patch);
    if (inp.dataset.st !== "daily_loss") { const p = $("#stPreview", card), st = stSet(); if (st.risk_min > st.risk_max) { saveShort({ risk_min: st.risk_max, risk_max: st.risk_min }); card.innerHTML = stCardHtml(); }
      else if (p) { const lo = stMoney(st, 0), cur = (S.connector.account || {}).currency; p.innerHTML = lo == null ? t("st_preview_off") : t("st_preview", { b: fmtMoney(S.connector.account.balance, cur), lo: fmtMoney(lo, cur), hi: fmtMoney(stMoney(st, 1), cur) }); } }
  });
  card.addEventListener("click", (e) => {
    const seg = e.target.closest("[data-st-seg] button"), sym = e.target.closest("[data-st-sym]"), vb = e.target.closest("[data-st-variant] button");
    if (vb) {
      const v = vb.dataset.v;
      if (v === stVariant()) return;
      // switched on: the flag moves to the variant picked, both in one save (the server keeps one of the two on);
      // switched off: only the page remembers it, nothing is saved (a v1.2.0 block stays as it was)
      S.stVariant = v;
      if (stOn()) { if (v === "rapid") { saveShort({ enabled: false }); saveRapid({ enabled: true }); } else { saveRapid({ enabled: false }); saveShort({ enabled: true }); } }
      card.innerHTML = stCardHtml();
      const rc = $("#rpCard"); if (rc) rc.classList.toggle("hidden", v !== "rapid");
    } else if (seg) {
      const key = seg.parentElement.dataset.stSeg, u = seg.dataset.v, st = stSet();
      if (st[key] === u) return;
      // a new unit starts from its own defaults: 50 $ must not turn into 50 %
      saveShort(key === "risk_unit" ? { risk_unit: u, risk_min: ST_UNIT_DEF[u].risk_min, risk_max: ST_UNIT_DEF[u].risk_max } : { loss_unit: u, daily_loss: ST_UNIT_DEF[u].daily_loss });
      card.innerHTML = stCardHtml();
    } else if (sym) {
      const x = sym.dataset.stSym, list = stSet().symbols, on = !list.includes(x);
      sym.classList.toggle("on", on); saveShort({ symbols: on ? [...list, x] : list.filter((y) => y !== x) });
    }
  });
}
function rpCardHtml() {   // Rapid's own settings, under the short-term card while its variant is picked
  const rp = rpSet(), R = RP_RANGE, money = rp.sl_unit === "money", trig = rp.triggers || {};
  const seg = (key, vals, val, lab) => `<div class="seg" data-rp-seg="${key}">${vals.map((u) => `<button class="${val === u ? "on" : ""}" data-v="${u}">${t(lab + u)}</button>`).join("")}</div>`;
  const nf = (key, label, v, [a, b, s]) => `<label class="field"><span>${t(label)}</span><input class="input" type="number" step="${s}" min="${a}" max="${b}" data-rp="${key}" value="${esc(v)}"></label>`;
  const chk = (k) => `<label class="check"><input type="checkbox" data-rp-trig="${k}" ${trig[k] ? "checked" : ""}><span>${t("rp_trig_" + k)}</span></label>`;
  const row = (label, rest = "", m = "16px 0 8px") => `<div class="row" style="margin:${m};gap:10px"><span class="muted" style="font-size:12.5px">${t(label)}</span>${rest}</div>`;
  return `<div class="card-head"><h2>${icon("bolt")}${t("rp_title")}</h2>${old130() ? `<span class="chip warn">${t("need_130")}</span>` : ""}</div>
    ${(S.settings || {}).rapid_note === "pairs_gone" ? `<div class="banner" style="margin-bottom:12px">${icon("alert")}<div>${t("rp_pairs_gone")}</div></div>` : ""}
    ${row("rp_trig", "", "0 0 8px")}<div class="row" style="gap:18px">${chk("signals")}${chk("news")}</div>
    ${row("rp_sl_unit", seg("sl_unit", ["pips", "money"], rp.sl_unit, "rp_sl_"))}
    ${row("rp_risk_unit", seg("risk_unit", ["pct", "usd"], rp.risk_unit, "st_unit_"), "8px 0 8px")}
    <div class="form-grid">${nf("orders", "rp_orders", rp.orders, R.orders)}${nf("risk_min", "rp_risk_min", rp.risk_min, R.risk[rp.risk_unit])}${nf("risk_max", "rp_risk_max", rp.risk_max, R.risk[rp.risk_unit])}
      ${money ? nf("lots", "rp_lots", rp.lots, R.lots) : nf("sl_pips", "burst_sl", rp.sl_pips, R.sl_pips)}
      ${nf("tp_min_usd", "rp_tp_min", rp.tp_min_usd, R.tp_min_usd)}${nf("tp_max_usd", "rp_tp_max", rp.tp_max_usd, R.tp_max_usd)}
      ${nf("offset_pips", "rp_offset", rp.offset_pips, R.offset_pips)}${nf("limit_pips", "rp_limit", rp.limit_pips, R.limit_pips)}${nf("expire_seconds", "rp_expire", rp.expire_seconds, R.expire_seconds)}
      ${holdFields("data-rp", rp, R.hold_seconds, R.max_hold_seconds)}${nf("max_runs", "rp_max_runs", rp.max_runs, R.max_runs)}${nf("cooldown_seconds", "rp_cool", rp.cooldown_seconds, R.cooldown_seconds)}</div>
    <p class="hint" style="margin:10px 0 0">${t(money ? "rp_sl_money_d" : "rp_sl_pips_d")}</p>
    <p class="hint" style="margin:6px 0 0">${t("hold_d")}</p>
    <p class="hint" style="margin:6px 0 0">${pipScaleText()}</p>
    <div class="mode-row" style="margin-top:8px"><label class="switch"><input type="checkbox" data-rp-an ${rp.analysis ? "checked" : ""}><i></i></label>
      <div class="txt"><b>${t("rp_an")}</b><div class="d">${t("burst_an_d")}</div></div></div>
    ${row("rp_loss", seg("loss_unit", ["pct", "usd"], rp.loss_unit, "st_unit_"))}
    <div class="form-grid">${nf("daily_loss", rp.loss_unit === "pct" ? "rp_loss_pct" : "rp_loss_usd", rp.daily_loss, R.daily_loss[rp.loss_unit])}</div>
    <div class="row" style="margin:16px 0 8px"><span class="muted" style="font-size:12.5px">${t("rp_pairs")}</span><span class="faint" style="font-size:12px">${t("rp_pairs_hint")}</span></div>
    <div class="sym-picks">${S.symbols.map((x) => `<button class="${rp.symbols.includes(x) ? "on" : ""}" data-rp-sym="${esc(x)}">${esc(x)}</button>`).join("")}</div>
    <div class="banner info" style="margin-top:16px">${icon("info")}<div><b>${t("rp_how")}</b><ul class="burst-notes">${["rp_n1", "rp_n2", "rp_n3", "rp_n4", "rp_n5", "rp_n6"].map((k) => `<li>${t(k)}</li>`).join("")}</ul></div></div>`;
}
function rpWire(card) {   // as stWire
  card.addEventListener("change", (e) => {
    const inp = e.target;
    if (inp.matches("[data-rp-an]")) return saveRapid({ analysis: inp.checked });
    if (inp.dataset.rpTrig) return saveRapid({ triggers: { ...RP_DEF.triggers, ...rpSet().triggers, [inp.dataset.rpTrig]: inp.checked } });
    if (!inp.dataset.rp) return;
    const v = fieldIn(inp, inp.dataset.rp, rpSet()), patch = { [inp.dataset.rp]: v };
    if (inp.dataset.rp === "hold_seconds") holdFollow(card, "[data-rp=max_hold_seconds]", rpSet(), v, patch);
    // a maximum under its minimum is raised to it (never swapped), as bot/rapid.py clamp_rapid does
    for (const [lo, hi] of [["risk_min", "risk_max"], ["tp_min_usd", "tp_max_usd"]]) {
      const r = { ...rpSet(), ...patch };
      if (r[lo] > r[hi]) { patch[hi] = r[lo]; const h = $(`[data-rp=${hi}]`, card); if (h) h.value = r[lo]; }
    }
    saveRapid(patch);
  });
  card.addEventListener("click", (e) => {
    const seg = e.target.closest("[data-rp-seg] button"), sym = e.target.closest("[data-rp-sym]");
    if (seg) {
      const key = seg.parentElement.dataset.rpSeg, u = seg.dataset.v;
      if (rpSet()[key] === u) return;
      // a new unit starts from its own defaults (bot/rapid.py UNIT_DEFAULTS): 50 $ must not turn into 50 %
      saveRapid(key === "risk_unit" ? { risk_unit: u, risk_min: RP_UNIT_DEF[u].risk_min, risk_max: RP_UNIT_DEF[u].risk_max }
        : key === "loss_unit" ? { loss_unit: u, daily_loss: RP_UNIT_DEF[u].daily_loss } : { sl_unit: u });
      card.innerHTML = rpCardHtml();
    } else if (sym) {
      const x = sym.dataset.rpSym, list = rpSet().symbols, on = !list.includes(x);
      sym.classList.toggle("on", on); saveRapid({ symbols: on ? [...list, x] : list.filter((y) => y !== x) });
    }
  });
}
// the burst's settings (Settings page; the Dashboard card is burstCardHtml). The v1.3.0 options are marked while the
// connector is older: it skips a burst that uses them (server/burst.py needs_version)
const burstPairs = () => {   // the pairs a release can trade that the engine and the user trade (their picks, none = all)
  const mine = (S.settings && S.settings.symbols) || [];
  return [...new Set(Object.values(BURST_PAIRS).flat())].filter((p) => S.symbols.includes(p) && (!mine.length || mine.includes(p)));
};
function burstMoneyText(bs) {   // what the whole burst may lose at its stops, "≈": fills can slip at news time
  const a = S.connector.online && S.connector.account, bal = a ? Number(a.balance) || 0 : 0, cur = (a || {}).currency;
  if (bs.sl_unit === "usd") return t("burst_money", { m: fmtMoney(a ? Math.min(bs.sl_money, bal * 0.1) : bs.sl_money, cur) });   // capped at 10 % of the balance
  return a ? t("burst_money", { m: fmtMoney((bal * bs.sl_money) / 100, cur) }) : t("burst_money_off", { v: fmtN(bs.sl_money, 1) });
}
// the burst's "an older connector skips it" banner, on the Dashboard card's rule: the server holds a run back from a
// connector before 1.3.0 when the burst's options need it, and Rapid's runs (all v1.3.0) while Rapid is on
const burstConnHtml = (bs) => (old130() && (burstUses130(bs) || rpSet().enabled)
  ? `<div class="banner" style="margin-top:12px">${icon("alert")}<div>${t("burst_conn_130")} <a href="#/connector">${t("connector")} →</a></div></div>` : "");
const burstConnShow = (root) => { const b = $("#burstConn", root); if (b) b.innerHTML = burstConnHtml(burstSet()); };   // redrawn in place
function burstSetHtml() {
  const bs = burstSet(), bapp = S.burstApp, R = BURST_RANGE, money = bs.sl_unit !== "pips", need = need130();
  const nf = (key, label, v, [a, b, s]) => `<label class="field"><span>${t(label)}</span><input class="input" type="number" step="${s}" min="${a}" max="${b}" data-burst="${key}" value="${esc(v)}"></label>`;
  const unitSeg = `<div class="seg" data-burst-unit>${["pips", "pct", "usd"].map((u) => `<button class="${bs.sl_unit === u ? "on" : ""}" data-v="${u}">${t("burst_u_" + u)}</button>`).join("")}</div>`;
  const pairs = burstPairs(), picks = bs.symbols || [], from = (p) => BURST_CUR.filter((c) => BURST_PAIRS[c].includes(p)).join(", ");
  // picks no longer offered (out of the user's pairs, or the admin's): shown picked and marked, so they can be taken
  // off; a release trades only picked pairs, so with no other pick the burst trades nothing (server/burst.py pick_pairs)
  const gone = picks.filter((p) => !pairs.includes(p));
  return `<div class="card-head"><h2>${icon("bolt")}${t("burst_title")}</h2>${bapp.enabled ? "" : `<span class="chip warn">${t("burst_admin_chip")}</span>`}</div>
    <div class="mode-row" style="padding-top:0"><label class="switch"><input type="checkbox" data-burst-on ${bs.enabled ? "checked" : ""} ${bapp.enabled ? "" : "disabled"}><i></i></label>
      <div class="txt"><b>${t("burst_switch")}</b><div class="d">${t("burst_switch_d")}</div></div></div>
    <div id="burstConn">${burstConnHtml(bs)}</div>
    <div class="row" style="margin:14px 0 8px;gap:10px"><span class="muted" style="font-size:12.5px">${t("burst_sl_unit")}</span>${unitSeg}${need}</div>
    <div class="form-grid">
      ${nf("trades", "burst_trades", bs.trades, [1, bapp.max_trades, 1])}${nf("lead_seconds", "burst_lead", bs.lead_seconds, R.lead_seconds)}${nf("offset_pips", "burst_offset", bs.offset_pips, R.offset_pips)}
      ${money ? nf("sl_money", "burst_sl_" + bs.sl_unit, bs.sl_money, R.sl_money[bs.sl_unit]) + nf("lots", "lot_order", bs.lots, R.lots) : nf("sl_pips", "burst_sl", bs.sl_pips, R.sl_pips)}
      ${nf("tp_start_pips", "burst_tp_start", bs.tp_start_pips, R.tp_start_pips)}${nf("tp_step_pips", "burst_tp_step", bs.tp_step_pips, R.tp_step_pips)}
      ${holdFields("data-burst", bs, R.hold_seconds, R.max_hold_seconds, need)}${nf("expire_seconds", "burst_expire", bs.expire_seconds, R.expire_seconds)}</div>
    ${money ? `<p class="hint" id="burstMoney" style="margin:10px 0 0">${burstMoneyText(bs)} ${t("burst_lots_d")}</p>` : ""}
    <p class="hint" style="margin:6px 0 0">${t("hold_d")}</p>
    <p class="hint" style="margin:6px 0 0">${pipScaleText()}</p>
    <div class="mode-row" style="margin-top:8px"><label class="switch"><input type="checkbox" data-burst-an ${bs.analysis ? "checked" : ""}><i></i></label>
      <div class="txt"><b>${t("burst_an")}</b>${need}<div class="d">${t("burst_an_d")}</div></div></div>
    <div class="row" style="margin:16px 0 8px"><span class="muted" style="font-size:12.5px">${t("burst_currencies")}</span><span class="faint" style="font-size:12px">${t("burst_cur_hint")}</span></div>
    <div class="sym-picks">${BURST_CUR.map((x) => `<button class="${(bs.currencies || []).includes(x) ? "on" : ""}" data-burst-cur="${x}">${x}</button>`).join("")}</div>
    <div class="row" style="margin:16px 0 8px"><span class="muted" style="font-size:12.5px">${t("burst_pairs")}</span><span class="faint" style="font-size:12px">${t("burst_pairs_hint")}${old130() ? ` · ${t("pairs_130")}` : ""}</span></div>
    ${gone.length && gone.length === picks.length ? `<div class="banner" style="margin-bottom:8px">${icon("alert")}<div>${t("burst_pairs_gone")}</div></div>` : ""}
    <div class="sym-picks">${pairs.map((x) => `<button class="${picks.includes(x) ? "on" : ""}" data-burst-sym="${esc(x)}" title="${esc(t("burst_pair_of", { c: from(x) }))}">${esc(x)}</button>`).join("")
      + gone.map((x) => `<button class="on gone" data-burst-sym="${esc(x)}" title="${esc(t("burst_pair_gone"))}">${esc(x)}</button>`).join("") || `<span class="faint">—</span>`}</div>
    <div class="banner info" style="margin-top:16px">${icon("info")}<div><b>${t("burst_how")}</b><ul class="burst-notes">${["burst_n1", "burst_n2", "burst_n3", "burst_n4", "burst_n5", "burst_n6", "burst_n7"].map((k) => `<li>${t(k)}</li>`).join("")}</ul></div></div>`;
}
function burstWire(card) {   // as stWire: the listeners live on the card, so redrawing its insides keeps them
  card.addEventListener("change", (e) => {
    const inp = e.target;
    if (inp.matches("[data-burst-on]")) return saveBurst({ enabled: inp.checked });
    // the analysis, a Y over X (and a pair below) may need connector 1.3.0: the banner follows at once
    if (inp.matches("[data-burst-an]")) { saveBurst({ analysis: inp.checked }); return burstConnShow(card); }
    if (!inp.dataset.burst) return;
    const v = fieldIn(inp, inp.dataset.burst, burstSet()), patch = { [inp.dataset.burst]: v };
    if (inp.dataset.burst === "hold_seconds") holdFollow(card, "[data-burst=max_hold_seconds]", burstSet(), v, patch);
    saveBurst(patch); burstConnShow(card);
    const m = $("#burstMoney", card); if (m && inp.dataset.burst === "sl_money") m.innerHTML = `${burstMoneyText(burstSet())} ${t("burst_lots_d")}`;
  });
  card.addEventListener("click", (e) => {
    const seg = e.target.closest("[data-burst-unit] button"), cur = e.target.closest("[data-burst-cur]"), sym = e.target.closest("[data-burst-sym]");
    if (seg) {
      const u = seg.dataset.v;
      if (burstSet().sl_unit === u) return;
      saveBurst({ sl_unit: u, sl_money: BURST_MONEY_DEF[u] });   // a new unit starts from its own default: 2 % must not turn into 2 $
      card.innerHTML = burstSetHtml();
    } else if (cur) {
      const x = cur.dataset.burstCur, list = burstSet().currencies || [], on = !list.includes(x);
      cur.classList.toggle("on", on); saveBurst({ currencies: on ? [...list, x] : list.filter((y) => y !== x) });
    } else if (sym) {
      const x = sym.dataset.burstSym, list = burstSet().symbols || [], on = !list.includes(x), pairs = burstPairs();
      saveBurst({ symbols: on ? [...list, x] : list.filter((y) => y !== x) });
      // with a pick no longer offered the card is drawn again: one taken off goes, the banner follows the picks
      if (list.some((p) => !pairs.includes(p))) card.innerHTML = burstSetHtml(); else { sym.classList.toggle("on", on); burstConnShow(card); }
    }
  });
}
const execWarn = (m) => (m === "notify" ? "" :`<div class="banner" style="margin-top:12px">${icon("alert")}<div>${t("auto_warn")} <a href="#/connector">${t("connector")} →</a></div></div>`);
// the Settings page's market clock card (v1.3.2): the pill, the Dashboard card, and each session's hours in its city's
// time; the sessions go out whole (a save merges one level deep), so the server never mixes two pages' hours
const hh = (h) => String(h).padStart(2, "0") + ":00";
function mkSetHtml() {
  const cfg = mktConf(), now = Date.now();
  const sw = (k) => `<div class="mode-row"><label class="switch"><input type="checkbox" data-mk="${k}" ${cfg[k] ? "checked" : ""}><i></i></label>
    <div class="txt"><b>${t("mk_" + k)}</b><div class="d">${t("mk_" + k + "_d")}</div></div></div>`;
  const sel = (id, k, lo, hi) => `<select class="input" data-mk-h="${k}" data-id="${id}" aria-label="${t("sess_" + id)} · ${t(k === "from" ? "mk_from" : "mk_to")}">${Array.from({ length: hi - lo + 1 }, (_, i) => lo + i)
    .map((h) => `<option value="${h}" ${h === cfg.sessions[id][k] ? "selected" : ""}>${hh(h)}</option>`).join("")}</select>`;
  return `<div class="card-head"><h2>${t("mk_title")}</h2></div>${sw("pill")}${sw("widget")}
    <div class="mk-h">${t("mk_sessions")}</div><div class="mk-sess">${SESSIONS.map(([id]) => { const c = cfg.sessions[id];
      return `<div class="mk-row"><label class="check"><input type="checkbox" data-mk-on="${id}" ${c.on ? "checked" : ""}><span><b>${t("sess_" + id)}</b></span></label>
        <label class="field"><span>${t("mk_from")}</span>${sel(id, "from", 0, 23)}</label><label class="field"><span>${t("mk_to")}</span>${sel(id, "to", 1, 24)}</label>
        <span class="faint mk-you">${c.on ? sessHours(id, now, cfg) : ""}</span></div>`; }).join("")}</div>
    <div class="hint">${t("mk_hint")}</div>
    <div class="row" style="margin-top:12px"><button class="btn sm" type="button" data-mk-reset>${t("mk_reset")}</button></div>`;
}
function mkWire(box) {
  const put = (sessions) => { saveSettings({ market: { sessions } }); box.innerHTML = mkSetHtml(); updateMktPill(); };
  box.addEventListener("change", (e) => {
    const el = e.target, cfg = mktConf();
    if (el.dataset.mk) { saveSettings({ market: { [el.dataset.mk]: el.checked } }); updateMktPill(); return; }
    const id = el.dataset.mkOn || el.dataset.id; if (!cfg.sessions[id]) return;
    const ses = JSON.parse(JSON.stringify(cfg.sessions)), c = ses[id];
    if (el.dataset.mkOn) c.on = el.checked; else c[el.dataset.mkH] = Number(el.value);
    if (c.to <= c.from) { if (el.dataset.mkH === "to") c.from = c.to - 1; else c.to = c.from + 1; }   // the server's rule too
    put(ses);
  });
  box.addEventListener("click", (e) => {
    if (!e.target.closest("[data-mk-reset]")) return;
    const cfg = mktConf();
    put(Object.fromEntries(SESSIONS.map(([id]) => [id, { ...MARKET_DEFAULT.sessions[id], on: cfg.sessions[id].on }])));
  });
}
async function pageSettings(el) {
  await loadSettings();
  const s = S.settings, me = S.me;
  const num = (key, label, step, min, max) => `<label class="field"><span>${t(label)}</span><input class="input" type="number" step="${step}" min="${min}" max="${max}" data-set="${key}" value="${s[key]}"></label>`;
  el.innerHTML = head(t("settings"), splitTitle(t("settings")), t("settings_sub")) + `<div class="stack">
    <div class="card"><div class="card-head"><h2>${t("exec_title")}</h2></div><div class="opt-cards">
      ${["notify", "confirm", "auto"].map((m) => `<button class="opt ${s.execution === m ? "on" : ""}" data-action="exec-mode" data-v="${m}"><div class="t">${t("exec_" + m)}</div><div class="d">${t("exec_" + m + "_d")}</div></button>`).join("")}</div>
      <div id="execWarn">${execWarn(s.execution)}</div></div>
    <div class="card"><div class="card-head"><h2>${t("modes_title")}</h2></div>
      ${["trend", "custom", "news_ai"].map((m) => { const on = S.modesEnabled[m]; return `<div class="mode-row"><label class="switch"><input type="checkbox" data-mode="${m}" ${s.modes[m] ? "checked" : ""} ${on ? "" : "disabled"}><i></i></label>
        <div class="txt"><b>${t("mode_" + m)}</b> ${on ? "" : `<span class="chip">${t("mode_disabled")}</span>`}<div class="d">${t("mode_" + m + "_d")}</div>
        ${m === "news_ai" ? `<div style="max-width:220px;margin-top:10px">${num("min_confidence", "min_conf", 1, 50, 100)}</div>` : ""}</div></div>`; }).join("")}</div>
    <div class="card" id="stCard">${stCardHtml()}</div>
    <div class="card ${stVariant() === "rapid" ? "" : "hidden"}" id="rpCard">${rpCardHtml()}</div>
    <div class="card"><div class="card-head"><h2>${t("pairs_title")}</h2><span class="faint" style="font-size:12px">${t("pairs_hint")}</span></div>
      <div class="sym-picks">${S.symbols.map((x) => `<button class="${s.symbols.includes(x) ? "on" : ""}" data-action="sym" data-v="${esc(x)}">${esc(x)}</button>`).join("")}</div></div>
    <div class="card"><div class="card-head"><h2>${t("risk_title")}</h2></div><div class="form-grid">
      ${num("risk_per_trade_pct", "risk_pct", 0.1, 0.1, 5)}${num("max_open_trades", "max_open", 1, 1, 20)}${num("daily_loss_limit_pct", "daily_loss", 0.5, 0.5, 20)}
      ${num("max_spread_pips", "max_spread", 0.1, 0.2, 20)}${num("max_price_drift_pips", "max_drift", 0.5, 0.5, 50)}</div>
      <label class="check" style="margin-top:14px"><input type="checkbox" data-bool="follow_exits" ${s.follow_exits ? "checked" : ""}><span>${t("follow_exits")}</span></label></div>
    <div class="card" id="burstSetCard">${burstSetHtml()}</div>
    <div class="card" id="mkSetCard">${mkSetHtml()}</div>
    <div class="card"><div class="card-head"><h2>${t("notif_title")}</h2></div>
      <label class="check"><input type="checkbox" data-bool="notify_browser" ${s.notify_browser ? "checked" : ""}><span>${t("notif_browser")}</span></label>
      <label class="check" style="margin-top:10px"><input type="checkbox" data-bool="notify_telegram" ${s.notify_telegram ? "checked" : ""} ${S.info.telegram_enabled ? "" : "disabled"}><span>${t("notif_tg")} ${S.info.telegram_enabled ? "" : `<span class="faint">· ${t("tg_off")}</span>`}</span></label>
      ${S.info.telegram_enabled ? `<div class="row" style="margin-top:12px;align-items:flex-end"><label class="field" style="flex:1;min-width:200px"><span>${t("tg_id")}</span><input class="input" id="tgId" value="${esc(me.telegram_chat_id || "")}" inputmode="numeric"></label>
        <button class="btn" data-action="tgsave">${t("save")}</button><button class="btn" data-action="tgtest">${t("tg_test")}</button></div><div class="hint">${t("tg_hint")}</div>` : ""}</div>
    <div class="grid g2">
      <div class="card"><div class="card-head"><h2>${t("profile_title")}</h2></div><form id="profForm" class="stack">
        <label class="field"><span>${t("name")}</span><input class="input" name="name" value="${esc(me.name || "")}" maxlength="120"></label>
        <label class="field"><span>${t("email")}</span><input class="input" value="${esc(me.email)}" disabled></label>
        <button class="btn gold" type="submit">${t("save")}</button></form></div>
      <div class="card"><div class="card-head"><h2>${t("pw_title")}</h2></div><form id="pwForm" class="stack">
        <label class="field"><span>${t("pw_current")}</span><input class="input" type="password" name="current" autocomplete="current-password" required></label>
        <label class="field"><span>${t("pw_new")}</span><input class="input" type="password" name="new" autocomplete="new-password" minlength="8" required></label>
        <div class="err" id="pwErr"></div><button class="btn gold" type="submit">${t("save")}</button></form></div>
    </div></div>`;
  $$("[data-set]", el).forEach((inp) => inp.addEventListener("change", () => saveSettings({ [inp.dataset.set]: Number(inp.value) })));
  $$("[data-mode]", el).forEach((inp) => inp.addEventListener("change", () => saveSettings({ modes: { [inp.dataset.mode]: inp.checked } })));   // the server keeps the others
  stWire($("#stCard", el)); rpWire($("#rpCard", el)); burstWire($("#burstSetCard", el)); mkWire($("#mkSetCard", el));
  $$("[data-bool]", el).forEach((inp) => inp.addEventListener("change", async () => {
    const ep = saveEpoch;   // the prompt can outlive the session: an answer after a logout or login is not this account's edit
    if (inp.dataset.bool === "notify_browser" && inp.checked && "Notification" in window && Notification.permission === "default") await Notification.requestPermission();
    if (ep === saveEpoch) saveSettings({ [inp.dataset.bool]: inp.checked });
  }));
  $("#profForm", el).addEventListener("submit", async (e) => {
    e.preventDefault();
    try { S.me = await api("/api/me/profile", { method: "PUT", body: { name: e.target.name.value } }); toast(t("saved")); renderShell(); route(); } catch (err) { toast(esc(errText(err)), "err"); }
  });
  $("#pwForm", el).addEventListener("submit", async (e) => {
    e.preventDefault(); $("#pwErr", el).textContent = "";
    try { await api("/api/me/password", { method: "POST", body: { current: e.target.current.value, new: e.target.new.value } }); e.target.reset(); toast(t("pw_changed")); }
    catch (err) { $("#pwErr", el).textContent = errText(err); }
  });
}

// ── connector ───────────────────────────────────────────────
function connStatusHtml() {   // the connector page's status card; redrawn in place on every connector_state push
  const c = S.connector || {}, a = c.account || {};
  return `<div class="card-head"><h2>${t("status")}</h2><span class="pill"><span class="dot ${c.online ? "on" : ""}"></span>${c.online ? t("online") : t("no_connector")}</span></div>
        ${c.account ? `<div class="grid g2"><div class="kpi"><div class="label">${t("account")}</div><div class="value" style="font-size:18px">${esc(a.login)} <span class="chip ${a.demo === false ? "warn" : ""}">${a.demo === false ? t("real") : t("demo")}</span></div><div class="sub">${esc(a.server || "")}</div></div>
          <div class="kpi"><div class="label">${t("k_equity")}</div><div class="value num" style="font-size:18px">${fmtMoney(a.equity, a.currency)}</div><div class="sub">${t("k_balance")} ${fmtMoney(a.balance, a.currency)}</div></div></div>
          <p class="faint" style="margin:12px 0 0;font-size:12px">v${esc(c.version || "?")} · ${t("updated", { t: ago(c.updated) })} · ${(c.positions || []).length} ${t("open_positions").toLowerCase()}</p>`
          : `<ol class="steps"><li><span>${t("step1")}</span></li><li><span>${t("step2")}</span></li><li><span>${t("step3")}</span></li><li><span>${t("step4")}</span></li></ol>`}`;
}
async function pageConnector(el) {
  const tokens = await api("/api/connector/tokens");
  const c = S.connector || {};
  el.innerHTML = head(t("connector"), splitTitle(t("connector")), t("connector_sub"),
    `<a class="btn gold" href="/api/connector/download" download>${icon("download")}${t("download")}</a>`) + `
    <div class="grid g2">
      <div class="card" id="connStatus">${connStatusHtml()}</div>
      <div class="card"><div class="card-head"><h2>${t("tokens")}</h2><button class="btn sm gold" data-action="newtoken">${t("new_token")}</button></div>
        ${tokens.length ? `<div class="table-wrap" data-k="tokens"><table><tbody>${tokens.map((x) => `<tr><td><b>${esc(x.label)}</b><div class="faint" style="font-size:12px">${fmtDate(x.created_at)} · ${t("last_used")}: ${x.last_used_at ? ago(x.last_used_at) : t("never")}</div></td>
          <td class="r"><button class="btn sm danger" data-action="revoke" data-id="${x.id}">${t("revoke")}</button></td></tr>`).join("")}</tbody></table></div>` : `<div class="empty">${t("no_tokens")}</div>`}</div>
    </div>
    <div class="card" style="margin-top:14px" id="ttCard">${testCardHtml()}</div>
    ${c.account ? `<div class="card" style="margin-top:14px"><div class="card-head"><h2>${t("how_to")}</h2></div><ol class="steps"><li><span>${t("step1")}</span></li><li><span>${t("step2")}</span></li><li><span>${t("step3")}</span></li><li><span>${t("step4")}</span></li></ol></div>` : ""}
    <div class="card" style="margin-top:14px"><div class="card-head"><h2>${t("connector_log")}</h2></div><div id="connLog">${logList(S.log)}</div></div>`;
}

// ── auto-trading test: a signal for this user's own connector (demo accounts) ──
const verAtLeast = (v, min) => {
  const n = (String(v || "").match(/\d+/g) || []).slice(0, 3).map(Number);
  if (!n.length) return false;
  while (n.length < 3) n.push(0);
  for (let i = 0; i < 3; i++) if (n[i] !== min[i]) return n[i] > min[i];
  return true;
};
function testChecks() {   // the same checks the server makes before it sends a test signal
  const c = S.connector || {}, a = c.account || {}, st = S.settings || {};
  const modes = ["trend", "custom", "news_ai"].filter((m) => st.modes && st.modes[m] && S.modesEnabled[m]);
  return [[!!c.online, t("tt_c_online")], [!!c.online && a.demo === true, t("tt_c_demo")],
    [!!c.online && verAtLeast(c.version, [1, 1, 2]), t("tt_c_version")],
    [st.execution === "auto", t("tt_c_auto"), "#/settings"], [modes.length > 0, t("tt_c_mode"), "#/settings"]];
}
const checkList = (rows) => `<ul class="checks">${rows.map(([ok, label, href]) => `<li class="${ok ? "ok" : "no"}"><i>${ok ? "✓" : "✗"}</i><span>${label}${!ok && href ? ` · <a href="${href}">${t("settings")} →</a>` : ""}</span></li>`).join("")}</ul>`;
const TT_LIVE = ["sent", "opened", "closing", "close_failed"];   // states that wait for the connector
function testPos() {   // the test trade in the connector's last snapshot; null: not there, undefined: connector offline
  const T = S.testSig, c = S.connector || {};
  if (!T || !c.online) return undefined;
  return (c.positions || []).find((p) => p.signal_id === T.id || (T.ticket != null && p.ticket === T.ticket)) || null;
}
function testBusy() {   // a test in flight; a stuck one frees the buttons again after a while, or once its trade is gone
  const T = S.testSig, age = T ? Date.now() - (T.closingAt || T.at) : 0;
  return !!T && (((T.state === "opened" || T.state === "close_failed") && testPos() !== null)
    || (T.state === "sent" && age < 60e3) || (T.state === "closing" && age < 30e3));
}
const testCanClose = (T) => !!T && (["sent", "opened", "close_failed"].includes(T.state) || (T.state === "closing" && Date.now() - T.closingAt > 30e3));
function testResultHtml() {
  const T = S.testSig; if (!T) return "";
  const spin = `<span class="spin" style="display:inline-block;vertical-align:middle;margin-right:6px"></span>`;
  const line = {
    sent: Date.now() - T.at > 20e3 ? `<span class="warn">${t("tt_r_silent")}</span>` : spin + t("tt_r_sent"),
    opened: `<span class="up">✓ ${t("tt_r_opened", { lots: esc(T.lots), sym: esc(T.symbol), p: fmtP(T.symbol, T.price), tk: esc(T.ticket) })}</span>`,
    skipped: `<span class="down">✗ ${t("tt_r_skipped")}: ${esc(T.reason)}</span>`,
    closing: T.retrying ? `<span class="warn">${spin}${t("tt_r_close_retry")}: ${esc(T.retrying)}</span>`
      : Date.now() - T.closingAt > 30e3 ? `<span class="warn">${t("tt_r_close_slow")}</span>` : spin + t("tt_r_closing"),
    close_failed: `<span class="down">✗ ${t("tt_r_close_failed")}: ${esc(T.reason)}</span>`,
    closed: `<span class="${cls(T.pnl)}">✓ ${t("tt_r_closed", { pnl: fmtMoney(T.pnl, (S.connector.account || {}).currency) })}</span>`,
    gone: `<span class="muted">${t("tt_r_gone")}</span>`,
    ended: `<span class="muted">${t("tt_r_ended")}</span>`,
  }[T.state] || "";
  return `<div class="banner info" style="margin-top:14px">${icon("info")}<div><b>${t("tt_signal")} #${esc(T.id)}</b> · ${T.side === "buy" ? t("buy") : t("sell")} ${esc(T.symbol)}<div style="margin-top:4px">${line}</div></div></div>`;
}
function testWatch() {   // on every connector snapshot: catch up with "trade" pushes this page missed
  const T = S.testSig, p = testPos();
  if (!T) return;
  if (p === undefined) { T.absent = 0; return; }   // connector offline: absences count again from its next snapshot
  if (p && T.state === "gone") {   // the snapshots that missed it were stale: it is open after all
    Object.assign(T, { state: "opened", ticket: p.ticket, lots: p.lots, price: p.entry_price, absent: 0 }); renderTest(); return;
  }
  if (!TT_LIVE.includes(T.state)) return;
  if (p && T.state === "sent") { Object.assign(T, { state: "opened", ticket: p.ticket, lots: p.lots, price: p.entry_price }); renderTest(); }
  // gone twice in a row: a snapshot read just before the order went in can arrive after the "trade" push
  T.absent = p || T.state === "sent" ? 0 : (T.absent || 0) + 1;
  if (T.absent >= 2) testSync(true);
}
async function testSync(absent = false) {   // read the test trade back from My trades (a "trade" push was missed)
  const T = S.testSig;
  if (!T || !TT_LIVE.includes(T.state) || Date.now() - (T.syncAt || 0) < 10e3) return;
  T.syncAt = Date.now();
  let rows;
  try { rows = (await api("/api/trades?limit=50")).trades.filter((x) => x.signal_id === T.id); } catch { return; }
  if (S.testSig !== T || !TT_LIVE.includes(T.state)) return;
  const open = rows.find((x) => x.status === "open"), done = rows.find((x) => x.status === "closed");
  if (done) Object.assign(T, { state: "closed", pnl: done.pnl });
  else if (absent) T.state = open ? "gone" : "ended";
  else if (open && T.state === "sent") Object.assign(T, { state: "opened", ticket: open.ticket, lots: open.lots, price: open.entry_price });
  renderTest();
}
function testCardHtml() {
  const st = S.settings || {}, syms = S.symbols.filter((x) => !(st.symbols || []).length || st.symbols.includes(x));
  const ready = testChecks().every((x) => x[0]), T = S.testSig;
  return `<div class="card-head"><h2>${icon("bolt")}${t("tt_title")}</h2><span class="chip">${t("demo")}</span></div>
    <p class="muted" style="margin:0 0 12px">${t("tt_d")}</p>
    <div id="ttChecks">${checkList(testChecks())}</div>
    <div class="row" style="margin-top:14px">
      <select class="input" id="ttSym" style="width:auto;min-width:120px">${syms.map((x) => `<option>${esc(x)}</option>`).join("")}</select>
      <div class="seg" id="ttSide"><button class="on" data-action="tt-side" data-v="buy">${t("buy")}</button><button data-action="tt-side" data-v="sell">${t("sell")}</button></div>
      <button class="btn gold" data-action="tt-send" ${ready && !testBusy() ? "" : "disabled"}>${t("tt_send")}</button>
      <button class="btn danger" data-action="tt-close" ${testCanClose(T) ? "" : "disabled"}>${t("tt_close")}</button></div>
    <div id="ttResult">${testResultHtml()}</div><p class="hint">${t("tt_hint")}</p>`;
}
function renderTest() {   // in place: keeps the chosen pair and side
  const ch = $("#ttChecks"); if (!ch) return;
  ch.innerHTML = checkList(testChecks());
  const T = S.testSig, send = $("[data-action=tt-send]"), close = $("[data-action=tt-close]");
  if (send) send.disabled = !testChecks().every((x) => x[0]) || testBusy();
  if (close) close.disabled = !testCanClose(T);
  $("#ttResult").innerHTML = testResultHtml();
}
async function testSend(btn) {
  btn.disabled = true;
  const side = ($("#ttSide .on") || { dataset: { v: "buy" } }).dataset.v, sym = $("#ttSym") && $("#ttSym").value;
  try {
    const r = await api("/api/test-signal", { method: "POST", body: { symbol: sym || null, side } });
    S.testSig = { id: r.signal.id, symbol: r.signal.symbol, side: r.signal.side, state: "sent", at: Date.now() };
    toast(t("tt_sent_toast"), "gold");
    setTimeout(renderTest, 20500);   // still no answer by then: say so
    setTimeout(renderTest, 60500);
  } catch (err) { toast(esc(errText(err)), "err"); }
  renderTest();
}
async function testClose(btn) {
  btn.disabled = true;
  try {
    const r = await api("/api/test-signal/close", { method: "POST" }), T = S.testSig;
    // an opened one whose trade is already gone: the snapshots tell, then My trades has the result (testWatch)
    if (T) Object.assign(T, { state: r.tickets.length || T.ticket != null ? "closing" : "ended", closingAt: Date.now(), tickets: r.tickets, absent: 0, retrying: T.state === "closing" ? T.retrying : "" });
    setTimeout(renderTest, 30500);
  } catch (err) {   // the buttons come back (renderTest): the server keeps the test signal for another try
    toast(esc(errText(err)), "err");
    if (err.code === "no_test_signal" && S.testSig) S.testSig.state = "ended";
  }
  renderTest();
}
function testCloseFailed(msg) {   // "Could not close #123: Market closed (signal closed). Retrying." (warn), then gave up (error)
  const T = S.testSig, m = /^Could not close #(\d+)\b[^:]*:\s*(.*)$/.exec(String(msg.message || ""));
  if (!T || !m || T.state !== "closing" || !["warn", "error"].includes(msg.level)) return;
  if (!(T.ticket != null ? [T.ticket] : T.tickets || []).map(String).includes(m[1])) return;
  if (msg.level === "warn") T.retrying = m[2].replace(/\s*Retrying\.$/, "");   // the connector keeps trying for a while
  else Object.assign(T, { state: "close_failed", reason: m[2], retrying: "" });
  renderTest();
}

// ── subscription ────────────────────────────────────────────
function pageSubscription(el) {
  const me = S.me, staff = me.role !== "client";
  el.innerHTML = head(t("subscription"), splitTitle(t("subscription"))) + `<div class="card" style="max-width:640px">
    <div class="row"><span class="dot ${me.subscription_active ? "on" : "off"}"></span><h2>${me.subscription_active ? t("sub_active") : t("sub_inactive")}</h2></div>
    <div class="grid g2" style="margin-top:16px"><div class="kpi"><div class="label">${t("plan")}</div><div class="value" style="font-size:20px;text-transform:capitalize">${esc(me.plan)}</div></div>
      <div class="kpi"><div class="label">${t("sub")}</div><div class="value" style="font-size:20px">${staff ? "∞" : me.subscription_until ? fmtDate(me.subscription_until) : "—"}</div>
      <div class="sub">${staff ? t("sub_staff") : me.subscription_until ? ago(me.subscription_until) : ""}</div></div></div>
    ${staff ? "" : `<p class="muted" style="margin-top:16px">${t("sub_renew")}</p>`}</div>`;
}

// ── admin: users ────────────────────────────────────────────
async function pageAdminUsers(el, q) {
  const [ov, users] = await Promise.all([api("/api/admin/overview"), api("/api/admin/users?q=" + encodeURIComponent(q.get("q") || ""))]);
  const subCell = (u) => u.role !== "client" ? `<span class="chip gold">∞</span>` : u.is_banned ? `<span class="chip down">${t("banned")}</span>`
    : `<span class="chip ${u.subscription_active ? "up" : "warn"}">${u.subscription_active ? t("active") : t("expired")}</span> <span class="faint" style="font-size:12px">${u.subscription_until ? fmtDate(u.subscription_until) : "—"}</span>`;
  el.innerHTML = head(t("nav_admin"), splitTitle(t("admin_users")), t("admin_sub"), `<button class="btn gold" data-action="adduser">${t("add_user")}</button>`) + `
    <div class="grid kpis">
      <div class="card kpi"><div class="label">${t("ov_users")}</div><div class="value"><b>${ov.users}</b></div><div class="sub">${t("banned")}: ${ov.banned}</div></div>
      <div class="card kpi"><div class="label">${t("ov_active")}</div><div class="value"><b>${ov.active_clients}</b></div><div class="sub">/ ${ov.clients}</div></div>
      <div class="card kpi"><div class="label">${t("ov_online")}</div><div class="value"><b>${ov.online}</b></div><div class="sub">${t("ov_connectors")}: ${ov.connectors}</div></div>
      <div class="card kpi"><div class="label">${t("ov_signals")}</div><div class="value"><b>${ov.signals_today}</b></div><div class="sub">${t("ov_ai")}: ${ov.ai_runs_today}</div></div>
    </div>
    <div class="card" style="margin-top:14px"><div class="card-head"><form id="userSearch" style="flex:1;max-width:360px"><input class="input" name="q" placeholder="${esc(t("search"))}" value="${esc(q.get("q") || "")}"></form></div>
      <div class="table-wrap"><table><thead><tr><th>${t("name")}</th><th>${t("role")}</th><th>${t("sub")}</th><th>${t("last_login")}</th><th>${t("status")}</th><th class="r">${t("actions")}</th></tr></thead><tbody>
      ${users.map((u) => `<tr><td><b>${esc(u.name || "—")}</b><div class="faint" style="font-size:12px">${esc(u.email)}</div></td>
        <td><span class="chip ${u.role === "client" ? "" : "gold"}">${t("r_" + u.role)}</span></td><td>${subCell(u)}</td>
        <td class="muted">${u.last_login_at ? ago(u.last_login_at) : "—"}</td>
        <td><span class="row" style="gap:10px"><span title="Web" class="dot ${u.online ? "on" : ""}"></span><span title="MT5" class="dot ${u.connector ? "on" : ""}"></span></span></td>
        <td class="r">${u.manageable ? `<button class="btn sm" data-action="manage" data-id="${u.id}">${t("actions")}</button>` : `<span class="faint" style="font-size:12px">${u.role === "founder" ? t("founder_protected") : ""}</span>`}</td></tr>`).join("")}
      </tbody></table></div></div>`;
  S._users = users;
  $("#userSearch", el).addEventListener("submit", (e) => { e.preventDefault(); location.hash = "#/admin?q=" + encodeURIComponent(e.target.q.value); });
}
async function manageUser(id) {
  const u = (S._users || []).find((x) => x.id === id); if (!u) return;
  const founder = S.me.role === "founder", self = u.id === S.me.id;
  const m = await modal({ title: u.name || u.email, confirm: null, cancel: t("cancel"), body: `<p class="muted" style="margin-top:-4px">${esc(u.email)} · ${t("r_" + u.role)}</p>
    <div class="stack" style="margin-top:14px">
      ${u.role === "client" ? `<div class="row"><button class="btn sm gold" data-u="extend">${t("extend30")}</button><button class="btn sm" data-u="date">${t("set_date")}</button><button class="btn sm danger" data-u="cut">${t("cut")}</button></div>` : ""}
      ${u.role !== "founder" && !self ? `<div class="row">${u.is_banned ? `<button class="btn sm" data-u="unban">${t("unban")}</button>` : `<button class="btn sm danger" data-u="ban">${t("ban")}</button>`}</div>` : ""}
      <div class="row"><button class="btn sm" data-u="reset">${t("reset_pw")}</button><button class="btn sm" data-u="logout">${t("force_logout")}</button></div>
      ${founder && !self && u.role !== "founder" ? `<div class="row"><button class="btn sm" data-u="role">${u.role === "admin" ? t("make_client") : t("make_admin")}</button></div>` : ""}
      ${!self || u.role === "founder" ? `<div class="row"><button class="btn sm danger" data-u="delete">${t("delete")}</button></div>` : ""}
    </div>`, onOpen: (box) => box.addEventListener("click", async (e) => {
      const act = e.target.dataset.u; if (!act) return;
      box.closest(".modal-back").remove();
      await userAction(u, act);
    }) });
  return m;
}
async function userAction(u, act) {
  const patch = (body) => api(`/api/admin/users/${u.id}`, { method: "PATCH", body });
  try {
    if (act === "extend") await patch({ extend_days: 30 });
    else if (act === "cut") { if (!await modal({ title: t("cut"), body: `<p class="muted">${esc(u.email)}</p>`, danger: true })) return; await patch({ subscription_until: "" }); }
    else if (act === "date") {
      const box = await modal({ title: t("set_date"), body: `<input class="input" type="date" id="mDate" value="${(u.subscription_until || "").slice(0, 10)}">` });
      if (!box) return; const v = box.querySelector("#mDate").value; if (!v) return; await patch({ subscription_until: v + "T23:59:59" });
    } else if (act === "ban") {
      const box = await modal({ title: t("ban") + " · " + u.email, danger: true, confirm: t("ban"), body: `<label class="field"><span>${t("ban_reason")}</span><input class="input" id="mReason" maxlength="500"></label>` });
      if (!box) return; await patch({ ban: true, ban_reason: box.querySelector("#mReason").value });
    } else if (act === "unban") await patch({ ban: false });
    else if (act === "role") await patch({ role: u.role === "admin" ? "client" : "admin" });
    else if (act === "reset") {
      const r = await api(`/api/admin/users/${u.id}/reset-password`, { method: "POST" });
      await modal({ title: t("reset_pw"), confirm: null, cancel: "OK", body: `<p>${t("temp_pw", { e: esc(u.email) })}</p><div class="token-box"><code>${esc(r.temp_password)}</code></div><p class="hint">${t("temp_pw_note")}</p>` });
    } else if (act === "logout") await api(`/api/admin/users/${u.id}/logout`, { method: "POST" });
    else if (act === "delete") {
      const founderDel = u.role === "founder";
      const box = await modal({ title: t("delete"), danger: true, confirm: t("delete"), body: founderDel
        ? `<p>${t("confirm_delete_founder")}</p><input class="input" id="mConfirm" autocomplete="off">` : `<p>${t("confirm_delete", { e: esc(u.email) })}</p>` });
      if (!box) return;
      await api(`/api/admin/users/${u.id}`, { method: "DELETE", headers: founderDel ? { "X-Confirm": box.querySelector("#mConfirm").value } : {} });
      if (founderDel) { S.me = null; closeWS(); return boot(); }
    }
    toast(t("saved")); route();
  } catch (e) { toast(esc(errText(e)), "err"); }
}
async function addUser() {
  const founder = S.me.role === "founder";
  const box = await modal({ title: t("add_user"), confirm: t("add_user"), body: `<div class="stack">
    <label class="field"><span>${t("email")}</span><input class="input" id="nEmail" type="email"></label>
    <label class="field"><span>${t("name")}</span><input class="input" id="nName"></label>
    <div class="grid g2"><label class="field"><span>${t("role")}</span><select class="input" id="nRole"><option value="client">${t("r_client")}</option>${founder ? `<option value="admin">${t("r_admin")}</option>` : ""}</select></label>
    <label class="field"><span>${t("days")}</span><input class="input" id="nDays" type="number" value="30" min="0"></label></div>
    <label class="field"><span>${t("plan")}</span><input class="input" id="nPlan" value="monthly"></label></div>` });
  if (!box) return;
  try {
    const r = await api("/api/admin/users", { method: "POST", body: { email: box.querySelector("#nEmail").value, name: box.querySelector("#nName").value,
      role: box.querySelector("#nRole").value, days: Number(box.querySelector("#nDays").value || 0), plan: box.querySelector("#nPlan").value || "monthly" } });
    await modal({ title: t("add_user"), confirm: null, cancel: "OK", body: `<p>${t("temp_pw", { e: esc(r.user.email) })}</p><div class="token-box"><code>${esc(r.temp_password)}</code></div><p class="hint">${t("temp_pw_note")}</p>` });
    route();
  } catch (e) { toast(esc(errText(e)), "err"); }
}

// ── admin: engine ───────────────────────────────────────────
async function pageAdminEngine(el) {
  const r = await api("/api/admin/engine"), s = r.settings, st = r.status;
  const sel = (id, opts, val) => `<select class="input" id="${id}">${opts.map((o) => `<option ${o === val ? "selected" : ""}>${esc(o)}</option>`).join("")}</select>`;
  const n = (id, label, val, step = 1) => `<label class="field"><span>${t(label)}</span><input class="input" type="number" step="${step}" id="${id}" value="${esc(val)}"></label>`;
  // a mode's own timeframe counts only when longer than the engine's (engine.mode_timeframe), so only those are offered;
  // a stored one that is not longer stays listed, marked ignored, until another is picked
  const secs = r.timeframe_seconds || Object.fromEntries(r.timeframes.map((o, i) => [o, i]));
  const tfOpts = (cur, eng) => `<option value="">${esc(t("tf_engine", { tf: eng }))}</option>${r.timeframes.filter((o) => secs[o] > secs[eng] || o === cur)
    .map((o) => `<option value="${esc(o)}" ${o === cur ? "selected" : ""}>${esc(secs[o] > secs[eng] ? o : t("tf_ignored", { tf: o }))}</option>`).join("")}`;
  const tfSel = (m) => `<select class="input" id="${m}_tf">${tfOpts(s.modes[m].timeframe || "", s.timeframe)}</select>`;
  const stratBlock = (m) => `<div class="card"><div class="card-head"><h2>${t("mode_" + m)}</h2><label class="switch"><input type="checkbox" id="${m}_on" ${s.modes[m].enabled ? "checked" : ""}><i></i></label></div>
    <div class="stack"><label class="field"><span>${t("strategy")}</span>${sel(m + "_strat", r.strategies, s.modes[m].strategy)}</label>
    <label class="field"><span>${t("mode_tf")}</span>${tfSel(m)}</label><p class="hint" style="margin:0">${t("mode_tf_hint")}</p>
    <label class="field"><span>${t("params")}</span><textarea class="input" id="${m}_params" spellcheck="false" style="font-family:ui-monospace,Consolas,monospace;font-size:12.5px">${esc(JSON.stringify(s.modes[m].params || {}, null, 1))}</textarea></label></div></div>`;
  const ai = s.modes.news_ai, bu = { enabled: true, max_trades: 40, ...s.burst };
  el.innerHTML = head(t("nav_admin"), splitTitle(t("admin_engine")), t("engine_sub"), `<button class="btn" data-action="runai" ${st.ai_enabled ? "" : "disabled"}>${icon("refresh")}${t("run_ai")}</button><button class="btn gold" data-action="saveengine">${t("save")}</button>`) + `
    <div class="card"><div class="card-head"><h2>${t("engine_status")}</h2></div><div class="grid kpis">
      <div class="kpi"><div class="label">${t("feed")}</div><div class="value" style="font-size:18px">${esc(st.feed)}</div><div class="sub">${t("started")} ${st.started ? ago(st.started) : "—"}</div></div>
      <div class="kpi"><div class="label">AI</div><div class="value" style="font-size:18px">${st.ai_enabled ? `<span class="up">${t("online")}</span>` : `<span class="muted">off</span>`}</div><div class="sub">${esc(st.model)}</div></div>
      <div class="kpi"><div class="label">${t("last_ai")}</div><div class="value" style="font-size:18px">${st.last_ai_run ? ago(st.last_ai_run) : "—"}</div><div class="sub ${st.last_ai_error ? "down" : ""}">${esc(st.last_ai_error || "")}</div></div>
      <div class="kpi"><div class="label">Bars</div><div class="sub" style="font-size:12px">${Object.entries(st.last_bar || {}).map(([k, v]) => `${esc(k)} ${esc(String(v).slice(5, 16))}`).join("<br>") || "—"}</div></div></div>
      ${st.errors && st.errors.length ? `<div style="margin-top:12px"><div class="label faint" style="font-size:12px">${t("errors")}</div>${logList(st.errors.map((e) => ({ time: e.time, level: "error", message: e.message })))}</div>` : ""}</div>
    <div class="card" style="margin-top:14px"><div class="form-grid">
      <label class="field" style="grid-column:1/-1"><span>${t("symbols")}</span><input class="input" id="e_symbols" value="${esc(s.symbols.join(", "))}"></label>
      <label class="field"><span>${t("timeframe")}</span>${sel("e_tf", r.timeframes, s.timeframe)}</label>
      ${n("e_guard", "guard", s.event_guard_minutes)}${n("e_poll", "news_poll", s.news_poll_minutes)}${n("e_trial", "trial_days", s.trial_days)}${n("e_coach", "coach_limit", s.coach_daily_limit)}
      <label class="check" style="align-self:end;padding-bottom:10px"><input type="checkbox" id="e_signup" ${s.signup_open ? "checked" : ""}><span>${t("signup_open")}</span></label></div></div>
    <div class="grid g3" style="margin-top:14px">${stratBlock("trend")}${stratBlock("custom")}
      <div class="card"><div class="card-head"><h2>${t("mode_news_ai")}</h2><label class="switch"><input type="checkbox" id="ai_on" ${ai.enabled ? "checked" : ""}><i></i></label></div>
        <div class="grid g2">${n("ai_conf", "min_conf", ai.min_confidence)}${n("ai_int", "ai_min_interval", ai.min_interval_minutes)}${n("ai_sl", "ai_sl", ai.sl_atr, 0.1)}${n("ai_tp", "ai_tp", ai.tp_atr, 0.1)}${n("ai_exp", "ai_expire", ai.expire_hours)}</div></div></div>
    <div class="card" style="margin-top:14px"><div class="card-head"><h2>${icon("bolt")}${t("burst_title")}</h2><label class="switch"><input type="checkbox" id="burst_on" ${bu.enabled ? "checked" : ""}><i></i></label></div>
      <div class="form-grid"><label class="field"><span>${t("burst_max")}</span><input class="input" type="number" step="1" min="1" max="40" id="burst_max" value="${esc(bu.max_trades)}"></label></div>
      <p class="hint" style="margin:10px 0 0">${t("burst_admin_d")}</p></div>
    <div class="card" style="margin-top:14px"><label class="field"><span>${t("rss")}</span><textarea class="input" id="e_rss" spellcheck="false">${esc(s.rss_feeds.join("\n"))}</textarea></label></div>`;
  $("#e_tf", el).addEventListener("change", (e) => {   // another engine timeframe: what counts as longer changes too
    for (const m of ["trend", "custom"]) { const x = $(`#${m}_tf`, el); x.innerHTML = tfOpts(x.value, e.target.value); }
  });
}
async function saveEngine() {
  const v = (id) => $("#" + id).value, num = (id) => Number(v(id));
  let params = {};
  try { params = { trend: JSON.parse(v("trend_params") || "{}"), custom: JSON.parse(v("custom_params") || "{}") }; } catch { return toast(t("e_invalid_params"), "err"); }
  const body = { symbols: v("e_symbols").split(/[,\s]+/).filter(Boolean), timeframe: v("e_tf"), event_guard_minutes: num("e_guard"), news_poll_minutes: num("e_poll"),
    trial_days: num("e_trial"), coach_daily_limit: num("e_coach"), signup_open: $("#e_signup").checked, rss_feeds: v("e_rss").split("\n").map((x) => x.trim()).filter(Boolean),
    modes: { trend: { enabled: $("#trend_on").checked, strategy: v("trend_strat"), timeframe: v("trend_tf"), params: params.trend }, custom: { enabled: $("#custom_on").checked, strategy: v("custom_strat"), timeframe: v("custom_tf"), params: params.custom },
      news_ai: { enabled: $("#ai_on").checked, min_confidence: num("ai_conf"), min_interval_minutes: num("ai_int"), sl_atr: num("ai_sl"), tp_atr: num("ai_tp"), expire_hours: num("ai_exp") } },
    burst: { enabled: $("#burst_on").checked, max_trades: Math.min(40, Math.max(1, Math.round(num("burst_max")) || 40)) } };
  try { await api("/api/admin/engine", { method: "PUT", body }); toast(t("saved")); await loadSettings(); route(); } catch (e) { toast(esc(errText(e)), "err"); }
}

// ── admin: self-learning (docs/SELF_LEARNING.md) ───────────
const LEARN_LABELS = ["gave_back", "stop_too_tight", "exit_rule", "wrong_way", "chop"];
const DECISION_CHIP = { improved: "up", no_change: "", rejected: "warn", error: "down", running: "gold" };
const RUN_CHIP = { proposed: "gold", applied: "up", reverted: "down", superseded: "" };
const WHY_NOT = [["suggest mode", "wn_suggest"], ["runs started by hand", "wn_manual"], ["nothing to apply", "wn_nothing"],
  ["new AI lessons", "wn_lessons"], ["learned from synthetic", "wn_demo"], ["stop/target scale", "wn_scale"], ["the active model has been", "wn_dwell"],
  ["the active model changed", "wn_changed"]];
const fmtR = (v, d = 2) => (v == null ? "—" : signed(v, d) + "R");
const fmtT = (v) => (v == null ? "—" : fmtN(v, 2));
const fmtX = (v) => "×" + Number(v).toLocaleString(S.lang === "ro" ? "ro-RO" : "en-US", { maximumFractionDigits: 2 });
const utcIso = (s) => (s && !/(Z|[+-]\d\d:?\d\d)$/.test(s) ? s + "Z" : s);   // replay times are UTC without a zone
const weekdayName = (i) => new Date(Date.UTC(2024, 0, 1 + i)).toLocaleDateString(locale(), { weekday: "long", timeZone: "UTC" });   // 1 Jan 2024: a Monday
const decisionChip = (d) => `<span class="chip ${DECISION_CHIP[d] || ""}">${esc(t("dec_" + d))}</span>`;
const runChip = (r) => (r.status && r.status !== "none" ? `<span class="chip ${RUN_CHIP[r.status] || ""}">${esc(t("lrs_" + r.status))}</span>` : "");
const whyNot = (w) => { const k = WHY_NOT.find(([p]) => (w || "").startsWith(p)); return k ? t(k[1]) : w || ""; };
function filterText(x) {   // learning.filter_text, in the page's language
  const where = x.scope === "*" ? t("lf_all") : x.scope, v = t("lv_" + x.value);
  return x.dim === "trend" ? t("lf_trend", { where, v }) : t("lf_dim", { where, dim: t("ld_" + x.dim), v });
}
function learnStep(s) {
  const m = /^loading (\w+)/.exec(s || "");
  if (m) return t("lst_loading", { s: m[1] });
  return { starting: t("lst_start"), "analysing the history": t("lst_history"), "AI signals": t("lst_ai") }[s] || s || "";
}
let learnPoll = null;
async function pollLearning() {   // while a run is going: show its step, then reload the page with the result
  if (S.page !== pageAdminLearning) return;
  let d = null;
  try { d = await api("/api/admin/learning"); } catch { /* try again */ }
  if (S.page !== pageAdminLearning) return;
  if (!d || d.running || Date.now() < S.learnKick) {
    const st = $("#learnStep"); if (st && d) st.textContent = learnStep(d.step);
    learnPoll = setTimeout(pollLearning, 3000); return;
  }
  S.learnKick = 0;
  const r = d.runs[0];
  if (r && r.decision !== "running") toast(`<b>${t("learn_done")} #${r.id}</b><br>${esc(t("dec_" + r.decision))}`, r.decision === "error" ? "err" : "gold");
  if (location.hash !== "#/admin/learning") location.hash = "#/admin/learning"; else route();
}
async function saveLearning(patch) {
  try { await api("/api/admin/engine", { method: "PUT", body: { learning: patch } }); toast(t("saved")); }
  catch (e) { toast(esc(errText(e)), "err"); route(); }
}
const learnLock = (busy) => (busy ? `disabled title="${esc(t("learn_running"))}"` : "");   // apply/revert wait for a run to end
function learnModelHtml(m, busy, off) {
  if (!m) return `<p class="muted" style="margin:0">${t("learn_plain")}</p>`;
  const f = m.filters || [], L = m.lessons || [];
  return `<div class="grid g2">
      <div class="kpi"><div class="label">${t("learn_model")}</div><div class="value" style="font-size:18px">#${esc(m.id)} <span class="chip ${m.applies ? "up" : "warn"}">${m.applies ? t("learn_in_use") : t("learn_not_in_use")}</span></div><div class="sub">${t("learn_since")} ${fmtDT(m.applied_at)}</div></div>
      <div class="kpi"><div class="label">${t("learn_levels")}</div><div class="value num" style="font-size:18px">SL ${fmtX(m.sl_scale)} · TP ${fmtX(m.tp_scale)}</div></div></div>
    ${m.applies ? "" : `<p class="hint">${t(off && !m.stale ? "learn_off_d" : "learn_not_in_use_d")}</p>`}
    <div class="label faint" style="font-size:12px;margin-top:14px">${t("learn_filters")}</div>
    ${f.length ? `<ul class="burst-notes">${f.map((x) => `<li>${esc(filterText(x))}</li>`).join("")}</ul>` : `<div class="muted" style="font-size:13px">${t("learn_none")}</div>`}
    ${L.length ? `<div class="label faint" style="font-size:12px;margin-top:12px">${t("learn_lessons")}</div><ul class="burst-notes">${L.map((x) => `<li>${esc(x)}</li>`).join("")}</ul>` : ""}
    <div class="row" style="margin-top:14px"><button class="btn sm" data-action="learn-revert" data-base="0" ${learnLock(busy)}>${t("learn_revert_prev")}</button>
      <button class="btn sm danger" data-action="learn-revert" data-base="1" ${learnLock(busy)}>${t("learn_revert_base")}</button></div>`;
}
function learnReportHtml(run, busy) {
  const r = run.report || {}, g = r.gate || {}, sm = r.summary;
  const canApply = ["proposed", "superseded"].includes(run.status) && run.model && !run.active;
  let h = `<div class="card-head"><h2>${t("learn_run_t")} #${esc(run.id)}</h2><div class="row">
    <span class="chip">${esc(t("trig_" + run.trigger))}</span>${decisionChip(run.decision)}${runChip(run)}
    <span class="faint" style="font-size:12px">${fmtDT(run.created_at)}</span>
    ${canApply ? `<button class="btn sm gold" data-action="learn-apply" data-id="${esc(run.id)}" ${learnLock(busy)}>${t("learn_apply")}</button>` : ""}</div></div>`;
  if (r.error) return h + `<div class="banner danger">${icon("alert")}<div>${esc(r.error)}</div></div>`;
  if (!sm) return h + `<div class="empty">${run.decision === "running" ? t("learn_running") : "—"}</div>`;
  if (r.warning) h += `<div class="banner" style="margin-bottom:12px">${icon("alert")}<div>${t("learn_demo_warn")}</div></div>`;
  if (r.edge && r.edge.alert) h += `<div class="banner danger" style="margin-bottom:12px">${icon("alert")}<div><b>${t("learn_edge_t")}</b> ${t("learn_edge_d", { m: fmtR(r.edge.test.mean), t: fmtT(r.edge.test.t) })}</div></div>`;
  const ex = r.explain || {};
  h += `<ul class="burst-notes learn-lines">${(ex[S.lang] || ex.en || []).map((l) => `<li>${esc(l)}</li>`).join("")}</ul>`;
  if (r.auto) h += `<p class="hint">${r.auto.applied ? t("learn_auto_applied") : t("learn_auto_not", { why: esc(whyNot(r.auto.why_not)) })}</p>`;

  const per = { train: [sm.period[0], sm.cuts[0]], valid: [sm.cuts[0], sm.cuts[1]], test: [sm.cuts[1], sm.period[1]], all: [sm.period[0], sm.period[1]] };
  const cells = (x) => `<td class="r num">${x.n}</td><td class="r num ${cls(x.mean)}">${fmtR(x.mean)}</td><td class="r num">${x.win_rate == null ? "—" : fmtN(x.win_rate, 0) + "%"}</td>`;
  const cols = `<th class="r">${t("learn_trades")}</th><th class="r">${t("learn_r_trade")}</th><th class="r">${t("win_rate")}</th>`;
  h += `<div class="learn-sec"><h3>${t("learn_results")}</h3><div class="table-wrap" data-k="lres"><table><thead>
      <tr><th></th><th class="r" colspan="3">${t("learn_active_col")}</th><th class="r" colspan="3">${t("learn_cand_col")}</th></tr>
      <tr><th>${t("learn_period")}</th>${cols}${cols}</tr></thead><tbody>
      ${Object.entries(per).map(([p, [a, b]]) => `<tr><td>${t("lp_" + p)}<div class="faint" style="font-size:11.5px">${fmtDate(utcIso(a))} → ${fmtDate(utcIso(b))}</div></td>${cells(sm.active[p])}${cells(sm.candidate[p])}</tr>`).join("")}
      </tbody></table></div><p class="hint">${t("learn_results_d", { c: fmtN(sm.cost_pips, 1), w: sm.weeks.test })}</p></div>`;

  const mk = r.mistakes;
  h += `<div class="learn-sec"><h3>${t("learn_mistakes")} <span class="faint" style="font-weight:400">· ${mk.losses} ${t("learn_losses")}</span></h3>
    ${LEARN_LABELS.map((k) => `<div class="lbar"><span>${t("ml_" + k)}</span><span class="bar"><i style="width:${Math.min(100, mk.share[k] || 0)}%"></i></span><span class="num">${fmtN(mk.share[k] || 0, 0)}% · ${mk.counts[k] || 0}</span></div>`).join("")}
    <p class="hint">${t("learn_mistakes_d")}</p></div>`;

  const G = (r.scales || {}).grid || [], pick = (r.scales || {}).chosen || {};
  if (G.length) {
    const sls = [...new Set(G.map((x) => x.sl_scale))].sort((a, b) => a - b), tps = [...new Set(G.map((x) => x.tp_scale))].sort((a, b) => a - b);
    h += `<div class="learn-sec"><h3>${t("learn_scales")}</h3><div class="table-wrap" data-k="lgrid"><table class="lgrid"><thead><tr><th>SL ↓ · TP →</th>${tps.map((x) => `<th class="r">${fmtX(x)}</th>`).join("")}</tr></thead><tbody>
      ${sls.map((sl) => `<tr><th>${fmtX(sl)}</th>${tps.map((tp) => {
        const c = G.find((x) => x.sl_scale === sl && x.tp_scale === tp);
        return c ? `<td class="r num ${pick.sl_scale === sl && pick.tp_scale === tp ? "pick" : ""}"><span class="${cls(c.train_gain.mean)}">${fmtR(c.train_gain.mean)}</span> / <span class="${cls(c.valid_gain.mean)}">${fmtR(c.valid_gain.mean)}</span></td>` : `<td class="r faint">—</td>`;
      }).join("")}</tr>`).join("")}</tbody></table></div><p class="hint">${t("learn_scales_d")}</p></div>`;
  }

  const F = (r.filters || {}).candidates || [];
  const fWhy = (c) => (c.kept ? "" : c.why ? t("learn_filter_limit") : c.blocked_share > 0.25 ? t("learn_too_many_blocked") : t("learn_no_valid_gain"));
  h += `<div class="learn-sec"><h3>${t("learn_filters_tried")}</h3>${F.length ? checkList(F.map((c) => [c.kept,
      `${esc(filterText(c))} — ${t("lp_train")}: ${c.train.n} · ${fmtR(c.train.mean)} (t ${fmtT(c.train.t)}), ${t("lp_valid")}: ${c.valid.n} · ${fmtR(c.valid.mean)}${c.kept ? "" : ` · <span class="faint">${fWhy(c)}</span>`}`]))
    : `<p class="muted" style="margin:0;font-size:12.5px">${t("learn_no_filters")}</p>`}</div>`;

  const okMark = { drawdown: "≤", double_cost: ">" };
  h += `<div class="learn-sec"><h3>${t("learn_gate")}</h3><div class="grid g2">
      ${[["learn_valid_gain", g.valid_gain, `${t("learn_need")} ≥ ${fmtR(g.margin)}, t ≥ ${fmtN(1.5, 1)}`], ["learn_test_gain", g.test_gain, `${t("learn_need")} > 0, t ≥ ${fmtN(1.28, 2)}`]].map(([k, x, need]) => `<div class="kpi">
        <div class="label">${t(k)}</div><div class="value num ${cls((x || {}).mean)}" style="font-size:18px">${fmtR((x || {}).mean)}<span class="faint" style="font-size:12px"> / ${t("week")}</span></div>
        <div class="sub">t = ${fmtT((x || {}).t)} · ${(x || {}).weeks ?? "—"} ${t("weeks")} · ${need}</div></div>`).join("")}</div>
    ${(g.checks || []).length ? `<div style="margin-top:12px">${checkList(g.checks.map((c) => [c.ok, `${t("gc_" + c.id)} — ${fmtN(c.value, c.id.endsWith("trades") ? 0 : 2)} (${t("learn_need")} ${okMark[c.id] || "≥"} ${fmtN(c.need, c.id.endsWith("trades") ? 0 : 2)})`]))}</div>`
      : `<p class="hint">${t("learn_no_checks")}</p>`}</div>`;

  const E = r.examples || [];
  if (E.length) h += `<div class="learn-sec"><h3>${t("learn_examples")}</h3><div class="table-wrap" data-k="lex"><table><thead><tr><th>${t("time")}</th><th>${t("symbol")}</th><th>${t("side")}</th><th class="r">R</th><th class="r">${t("learn_best")}</th><th>${t("learn_why")}</th></tr></thead><tbody>
    ${E.map((x) => `<tr><td class="muted">${fmtDT(utcIso(x.time))}</td><td><b>${esc(x.symbol)}</b></td><td class="side-tag ${x.side === "buy" ? "up" : "down"}">${x.side === "buy" ? t("buy") : t("sell")}</td>
      <td class="r num down">${fmtR(x.r)}</td><td class="r num">${fmtR(x.mfe_r)}</td><td>${t("ml_" + x.label)}</td></tr>`).join("")}</tbody></table></div></div>`;

  const N = r.news, PL = (run.model || {}).lessons || [];
  if (N) h += `<div class="learn-sec"><h3>${t("learn_news")}</h3>${N.closed ? `<div class="table-wrap" data-k="lnews"><table><thead><tr><th>${t("confidence")}</th><th class="r">${t("learn_trades")}</th><th class="r">${t("learn_r_trade")}</th><th class="r">${t("win_rate")}</th></tr></thead><tbody>
      ${N.bands.map((b) => `<tr><td>${b.from}–${b.to}%</td><td class="r num">${b.n}</td><td class="r num ${cls(b.mean)}">${fmtR(b.mean)}</td><td class="r num">${b.win_rate == null ? "—" : fmtN(b.win_rate, 0) + "%"}</td></tr>`).join("")}</tbody></table></div>
      <p class="hint">${N.suggested_min_confidence ? t("learn_news_suggest", { a: N.min_confidence, b: N.suggested_min_confidence }) : t("learn_news_ok", { n: N.closed })}</p>`
      : `<p class="muted" style="margin:0;font-size:12.5px">${t("learn_news_none")}</p>`}
    ${(N.new_lessons || []).length ? `<div class="label faint" style="font-size:12px;margin-top:10px">${t("learn_new_lessons")}</div><ul class="burst-notes">${N.new_lessons.map((l) => `<li>${esc(l)}</li>`).join("")}</ul>` : ""}
    ${PL.length ? `<div class="label faint" style="font-size:12px;margin-top:10px">${t("learn_proposal_lessons")}</div><ul class="burst-notes">${PL.map((l) => `<li>${esc(l)}</li>`).join("")}</ul>` : ""}
    ${N.lessons_error ? `<p class="hint down">${esc(N.lessons_error)}</p>` : ""}</div>`;
  return h;
}
async function pageAdminLearning(el, q) {
  const d = await api("/api/admin/learning"), s = d.settings, rg = d.regret;
  const pickId = Number(q.get("run")) || (d.runs.find((x) => x.decision !== "running") || {}).id;
  const run = pickId ? await api(`/api/admin/learning/runs/${pickId}`).catch(() => null) : null;
  const busy = d.running || Date.now() < S.learnKick;
  const lnum = (key, label, step, min, max) => `<label class="field"><span>${t(label)}</span><input class="input" type="number" step="${step}" min="${min}" max="${max}" data-learn="${key}" value="${esc(s[key])}"></label>`;
  el.innerHTML = head(t("nav_admin"), splitTitle(t("admin_learning")), t("learn_sub"),
    `<button class="btn gold" data-action="learn-run" ${busy ? "disabled" : ""}>${busy ? `<span class="spin"></span>${t("running")}` : `${icon("refresh")}${t("learn_run")}`}</button>`) + `
    ${busy ? `<div class="banner info" style="margin-bottom:14px"><span class="spin" style="flex:none"></span><div><b>${t("learn_running")}</b> · <span id="learnStep">${esc(learnStep(d.step))}</span></div></div>` : ""}
    <details class="card learn-how" style="margin-bottom:14px"><summary>${icon("info")}${t("learn_how")}</summary>
      <ul class="burst-notes">${["lh1", "lh2", "lh3", "lh4", "lh5"].map((k) => `<li>${t(k)}</li>`).join("")}</ul></details>
    <div class="card"><div class="card-head"><h2>${t("learn_settings")}</h2></div>
      <div class="opt-cards">${["off", "suggest", "auto"].map((v) => `<button class="opt ${s.mode === v ? "on" : ""}" data-action="learn-mode" data-v="${v}"><div class="t">${t("lm_" + v)}</div><div class="d">${t("lm_" + v + "_d")}</div></button>`).join("")}</div>
      <div class="form-grid" style="margin-top:14px">
        <label class="field"><span>${t("learn_day")}</span><select class="input" data-learn="weekday">${[0, 1, 2, 3, 4, 5, 6].map((i) => `<option value="${i}" ${s.weekday === i ? "selected" : ""}>${esc(weekdayName(i))}</option>`).join("")}</select></label>
        ${lnum("hour_utc", "learn_hour", 1, 0, 23)}${lnum("history_bars", "learn_bars", 1000, 3000, 44000)}${lnum("cost_pips", "learn_cost", 0.1, 0, 10)}</div>
      <label class="check" style="margin-top:14px"><input type="checkbox" data-learn="ai_lessons" ${s.ai_lessons ? "checked" : ""}><span>${t("learn_ai_lessons")}</span></label>
      <p class="hint">${s.mode === "off" ? t("learn_off_note") : t("learn_next", { d: weekdayName(s.weekday), h: String(s.hour_utc).padStart(2, "0") })} · ${t("feed")}: ${esc(d.feed)}</p></div>
    <div class="grid g2" style="margin-top:14px">
      <div class="card"><div class="card-head"><h2>${t("learn_active")}</h2></div>${learnModelHtml(d.model, busy, s.mode === "off")}</div>
      <div class="card"><div class="card-head"><h2>${t("learn_regret")}</h2></div>
        ${rg ? `<p class="muted" style="margin:0;font-size:13px">${t("learn_regret_d", { w: rg.weeks, g: fmtR((rg.weekly_gain || {}).mean), t: fmtT((rg.weekly_gain || {}).t) })}</p>
          <p class="hint">${t("learn_checked")} ${ago(rg.checked_at)}${rg.fidelity ? ` · ${t("learn_fidelity", { m: rg.fidelity.matched, n: rg.fidelity.live })}` : ""}${rg.revert ? ` · <span class="down">${t("learn_reverted")}</span>` : ""}</p>`
          : `<p class="muted" style="margin:0;font-size:13px">${t("learn_regret_none")}</p>`}
        <div class="label faint" style="font-size:12px;margin-top:14px">${t("learn_blocked")}</div>
        ${d.blocked.length ? `<ul class="log">${d.blocked.map((b) => `<li><time>${fmtTime(b.time)}</time><span>${esc(b.symbol)} ${b.side === "buy" ? t("buy") : t("sell")} · ${esc(S.lang === "ro" && b.rule_ro ? b.rule_ro : b.rule)}</span></li>`).join("")}</ul>`
          : `<div class="muted" style="font-size:13px">${t("learn_none")}</div>`}</div></div>
    ${run ? `<div class="card" style="margin-top:14px">${learnReportHtml(run, busy)}</div>` : ""}
    <div class="card" style="margin-top:14px"><div class="card-head"><h2>${t("learn_history")}</h2></div>
      ${d.runs.length ? `<div class="table-wrap" data-k="runs"><table><thead><tr><th>#</th><th>${t("time")}</th><th>${t("learn_trigger")}</th><th>${t("learn_decision")}</th><th>${t("status")}</th><th class="r">${t("learn_trades")}</th><th class="r">${t("learn_r_trade")}</th><th></th></tr></thead><tbody>
        ${d.runs.map((x) => { const a = x.report && x.report.summary ? x.report.summary.active.all : null; return `<tr class="${x.id === pickId ? "on" : ""}"><td>${esc(x.id)}${x.active ? ` <span class="chip up">${t("learn_in_use")}</span>` : ""}</td>
          <td class="muted">${fmtDT(x.created_at)}</td><td>${esc(t("trig_" + x.trigger))}</td><td>${decisionChip(x.decision)}</td><td>${runChip(x) || "—"}</td>
          <td class="r num">${a ? a.n : "—"}</td><td class="r num ${a ? cls(a.mean) : ""}">${a ? fmtR(a.mean) : "—"}</td>
          <td class="r"><a class="btn sm" href="#/admin/learning?run=${esc(x.id)}">${t("learn_view")}</a></td></tr>`; }).join("")}</tbody></table></div>`
        : `<div class="empty">${t("learn_none_runs")}</div>`}</div>`;
  $$("[data-learn]", el).forEach((inp) => inp.addEventListener("change", () => {
    let v = inp.type === "checkbox" ? inp.checked : Number(inp.value);
    if (inp.type === "number") { v = Math.min(Number(inp.max), Math.max(Number(inp.min), v || 0)); inp.value = v; }
    saveLearning({ [inp.dataset.learn]: v });
  }));
  clearTimeout(learnPoll);
  if (busy) learnPoll = setTimeout(pollLearning, 3000);
  S.pageCleanup = () => clearTimeout(learnPoll);
}

// ── admin: audit ────────────────────────────────────────────
async function pageAdminAudit(el) {
  const rows = await api("/api/admin/audit?limit=300");
  el.innerHTML = head(t("nav_admin"), splitTitle(t("admin_audit")), t("audit_sub")) + `<div class="card"><div class="table-wrap"><table>
    <thead><tr><th>${t("time")}</th><th>${t("actor")}</th><th>${t("action")}</th><th>${t("target")}</th><th>${t("details")}</th></tr></thead><tbody>
    ${rows.map((r) => `<tr><td class="muted">${fmtDT(r.time)}</td><td>${esc(r.actor)}</td><td><span class="chip">${esc(r.action)}</span></td><td>${esc(r.target || "")}</td><td class="wrap muted">${esc(r.details || "")}</td></tr>`).join("") || `<tr><td colspan="5" class="empty">—</td></tr>`}
    </tbody></table></div></div>`;
}

// ── WebSocket (live updates) ────────────────────────────────
let pingTimer = null;
function openWS() {
  if (!S.me || !S.me.subscription_active) { S.wsState = "off"; updateLivePill(); return; }
  closeWS(true);
  const ws = new WebSocket((location.protocol === "https:" ? "wss://" : "ws://") + location.host + "/ws");
  S.ws = ws;
  ws.onopen = () => { S.wsState = "on"; S.wsRetry = 0; updateLivePill(); clearInterval(pingTimer); pingTimer = setInterval(() => ws.readyState === 1 && ws.send('{"type":"ping"}'), 25000); };
  ws.onmessage = (ev) => { try { onWS(JSON.parse(ev.data)); } catch (e) { console.error(e); } };
  ws.onclose = (ev) => {
    clearInterval(pingTimer);
    if (S.ws !== ws) return;
    S.ws = null;
    if (!S.me) return;
    if (ev.code === 4401 || ev.code === 4402) { S.wsState = "off"; updateLivePill(); api("/api/auth/me").then((m) => { S.me = m; updateSubBanner(); }).catch(() => {}); return; }
    S.wsState = "retry"; S.wsMissed = true; updateLivePill();
    const delay = Math.min(30000, 1000 * 2 ** S.wsRetry++);
    setTimeout(() => { if (S.me && !S.ws) openWS(); }, delay);
  };
}
function closeWS(silent) { if (S.ws) { const w = S.ws; S.ws = null; try { w.close(); } catch {} } clearInterval(pingTimer); if (!silent) { S.wsState = "off"; updateLivePill(); } }
function notify(title, body) {
  if (!S.settings || !S.settings.notify_browser || !("Notification" in window) || Notification.permission !== "granted" || !document.hidden) return;
  try { new Notification(title, { body, tag: "fx-" + title, icon: $("link[rel=icon]").href }); } catch { /* unsupported */ }
}
function onWS(msg) {
  switch (msg.type) {
    case "hello": {   // usually lands after the first render: redraw pages that read S.connector once
      const prev = S.connector;
      S.connector = msg.connector || { online: false }; updateConnPill(); burstConnSync();
      if (S.wsMissed) { S.wsMissed = false; refreshSoft(); testSync(); }   // back after a drop (phone asleep): pushes were missed, reload the page
      else connRedraw(prev);
      testWatch();
      break;
    }
    case "signal": {
      const s = { ...msg.signal, _fresh: true };
      const st = S.settings || {}, mine = st.modes && st.modes[s.mode] && (!st.symbols.length || st.symbols.includes(s.symbol)) && (s.mode !== "news_ai" || (s.confidence || 0) >= st.min_confidence);
      s.for_me = !!mine;
      const title = `${s.side.toUpperCase()} ${s.symbol} · ${t("mode_" + s.mode)}`;
      if (mine) { toast(`<b>${t("new_signal")}</b><br>${esc(title)} @ ${fmtP(s.symbol, s.price)}`, "gold"); notify(t("new_signal") + ": " + title, `${t("sl")} ${fmtP(s.symbol, s.sl)}${s.tp ? " · " + t("tp") + " " + fmtP(s.symbol, s.tp) : ""}`); }
      const hq = parseHash().q, hm = hq.get("mode");
      const list = $("#liveSignals") || ($("#sigList") && hq.get("status") !== "closed" && (!hm || hm === s.mode) ? $("#sigList") : null);
      if (list) { const empty = list.querySelector(".empty"); if (empty) empty.remove(); list.insertAdjacentHTML("afterbegin", sigCard(s)); }
      $$(".kOpen").forEach((k) => { k.textContent = Number(k.textContent || 0) + 1; });
      break;
    }
    case "signal_closed": {
      const s = msg.signal, card = $(`[data-sig="${s.id}"]`);
      if (card) {
        const live = $("#liveSignals");
        if (live) {
          card.remove(); $$(".kOpen").forEach((k) => { k.textContent = Math.max(0, Number(k.textContent) - 1); });
          if (!live.querySelector(".sig")) live.innerHTML = `<div class="empty">${t("no_open_signals")}</div>`;
        }
        else card.outerHTML = sigCard(s);
      }
      if (S.settings && S.settings.modes[s.mode]) toast(`<b>${t("signal_closed_t")}</b><br>${esc(s.symbol)} · <span class="${cls(s.result_pips)}">${signed(s.result_pips)} ${t("pips")}</span> · ${esc(s.close_reason)}`);
      break;
    }
    case "price": {
      const box = $(`[data-px="${CSS.escape(msg.symbol)}"] .p`); if (box) box.textContent = fmtP(msg.symbol, msg.close);
      break;
    }
    case "connector_state": {   // pushed with every 5 s snapshot
      const prev = S.connector;
      S.connector = msg.state || { online: false }; updateConnPill(); burstConnSync(); connRedraw(prev); testWatch();
      break;
    }
    case "burst": {
      const b = msg.burst; if (!b || b.id == null) break;
      const fin = BURST_FINAL.has(b.status);
      burstUpsert(b);
      if (S.page === pageTrades || (S.page === pageDashboard && fin)) refreshSoft();
      if (S.burstSeen[b.id] === b.status || !(fin || b.status === "triggered")) break;   // toast each state once
      S.burstSeen[b.id] = b.status;
      const line = `${esc(b.symbol)}${b.kind === "rapid" ? ` (${t("v_rapid")})` : ""} · ${esc(b.event_title)}`, cur = (S.connector.account || {}).currency;
      if (!fin) { toast(`<b>${t("burst_t_triggered")}</b><br>${line} · ${burstSide(b.side)}`, "gold"); notify(t("burst_t_triggered"), `${b.symbol} · ${b.event_title}`); break; }
      toast(`<b>⚡ ${t("burst_title")}: ${esc(t("bs_" + b.status))}</b><br>${line}${b.status === "closed" ? ` · <span class="${cls(b.pnl)}">${fmtMoney(b.pnl, cur)}</span>`
        : b.reason ? ` · ${esc(b.reason)}` : ""}`, b.status === "error" ? "err" : b.status === "closed" ? "gold" : "");
      break;
    }
    case "connector_log": S.log.unshift(msg); S.log = S.log.slice(0, 200); { const box = $("#connLog"); if (box) box.innerHTML = logList(S.log); }
      if (msg.level === "error") toast(esc(msg.message), "err");
      testCloseFailed(msg); break;
    case "trade": {
      const tr = msg.trade || {}, T = S.testSig;
      if (T && tr.signal_id === T.id) {   // the auto-trading test: opened, then closed
        if (tr.status === "open") { Object.assign(T, { state: "opened", ticket: tr.ticket, lots: tr.lots, price: tr.entry_price, absent: 0 }); toast(`<b>✓ ${t("tt_title")}</b><br>${esc(tr.symbol)} #${esc(tr.ticket)}`, "gold"); }
        else Object.assign(T, { state: "closed", pnl: tr.pnl });
        renderTest();
      }
      if (S.page === pageTrades) refreshSoft();
      else if (S.page === pageDashboard) refreshKpis();
      break;
    }
    case "signal_skip": {
      const T = S.testSig;
      if (T && msg.signal_id === T.id) { Object.assign(T, { state: "skipped", reason: msg.reason }); renderTest(); }
      if (S.page === pageSignals || S.page === pageDashboard) refreshSoft();
      break;
    }
    case "analysis": case "news_updated": case "calendar_updated": if (S.page === pageMarket || (S.page === pageDashboard && msg.type === "analysis")) refreshSoft(); break;
    case "kicked": {
      const key = "kicked_" + msg.reason; toast(esc(I18N.en[key] ? t(key) : t("kicked_other")), "err");
      if (msg.reason === "subscription_inactive") { api("/api/auth/me").then((m) => { S.me = m; updateSubBanner(); route(); }).catch(() => {}); }
      else { S.me = null; closeWS(); api("/api/auth/logout", { method: "POST" }).catch(() => {}).finally(() => { location.hash = "#/login"; boot(); }); }
      break;
    }
  }
}
function connRedraw(prev) {   // after S.connector changed (a push every 5 s): patch in place; rebuild only what changed shape
  const c = S.connector, flip = !!prev.online !== !!c.online;
  if (S.page === pageTrades) {   // compare with the rows on screen, so a refresh that failed is tried again on the next push
    if ($("#page .spin")) return;   // first render still loading; it reads S.connector itself
    const shown = $$("#page [data-pos]").map((r) => r.dataset.pos).sort().join(), live = (c.positions || []).map((p) => String(p.ticket)).sort().join();
    if (flip || shown !== live || $("#page [data-action=reload]")) refreshSoft(); else patchPositions();
  }
  else if (S.page === pageConnector) { const k = $("#connStatus"); if (flip || !prev.account !== !c.account || !k) refreshSoft(); else { k.innerHTML = connStatusHtml(); renderTest(); } }
  else if (S.page === pageDashboard) { $$(".kConn").forEach((k) => { k.innerHTML = connKpiHtml(); }); if (flip) refreshSoft(); }
  else if (S.page === pageSignals && flip) refreshSoft();   // the execute buttons depend on online
  else if (S.page === pageSettings) {   // the cards show what the connector can run: drawn again once it changed
    if (flip || prev.version !== c.version || !prev.account !== !c.account) S.setConnStale = true;
    if (S.setConnStale) S.setConnStale = !settingsConnRedraw();
  }
}
// settings page: the short-term, Rapid and burst cards drawn again for a new connector state (the 1.3.0 marks and
// banners, the money previews). A card being typed in is left as it is: false, the next push tries it again
function settingsConnRedraw() {
  const a = document.activeElement;
  let done = true;
  for (const [sel, html] of [["#stCard", stCardHtml], ["#rpCard", rpCardHtml], ["#burstSetCard", burstSetHtml]]) {
    const card = $(sel);
    if (!card) continue;
    if (a && card.contains(a) && a.matches("input:not([type=checkbox]), select, textarea")) done = false;
    else card.innerHTML = html();
  }
  return done;
}
function patchPositions() {   // trades page: the live cells of each open position (keeps scroll, focus and the Close buttons)
  const cur = (S.connector.account || {}).currency;
  for (const p of S.connector.positions || []) {
    const row = $(`#page [data-pos="${CSS.escape(String(p.ticket))}"]`); if (!row) continue;
    const vals = { lots: p.lots, entry: fmtP(p.symbol, p.entry_price), sl: fmtP(p.symbol, p.sl), tp: fmtP(p.symbol, p.tp), price: fmtP(p.symbol, p.price), profit: fmtMoney(p.profit, cur) };
    for (const [f, v] of Object.entries(vals)) { const td = $(`[data-f="${f}"]`, row); if (td) td.textContent = v; }
    const pc = $('[data-f="profit"]', row); if (pc) pc.className = "r " + cls(p.profit);
  }
}
let softTimer = null, softGen = 0;
function refreshSoft(retry = true) {   // re-render the current page at most every 2s, keeping scroll position
  clearTimeout(softTimer);
  const want = S.page, seq = S.routeSeq, gen = ++softGen;
  softTimer = setTimeout(async () => {
    const el = $("#page");
    if (!want || S.page !== want || S.routeSeq !== seq || !el || el.contains(document.activeElement) && document.activeElement.matches("input,textarea,select")) return;
    // render off-screen and swap in only if the user is still on this page (the pages refreshed here only set innerHTML)
    const tmp = el.cloneNode(false);
    try { await want(tmp, parseHash().q); } catch {   // keep the old view; try once more in 10 s unless something newer is queued
      if (retry && gen === softGen && S.page === want && S.routeSeq === seq) softTimer = setTimeout(() => refreshSoft(false), 8000);
      return;
    }
    if (S.page !== want || S.routeSeq !== seq || $("#page") !== el) return;
    // keep where the user is: page scroll, each table's and the strategy picker's sideways scroll (phones), and the
    // focused button
    const y = window.scrollY, xs = {}, f = document.activeElement, sideways = ".table-wrap[data-k], .seg[data-k]";
    $$(sideways, el).forEach((w) => (xs[w.dataset.k] = w.scrollLeft));
    const attr = (k) => (f.dataset[k] != null ? `[data-${k}="${CSS.escape(f.dataset[k])}"]` : "");   // e.g. filter buttons differ only by data-k/data-v
    const fsel = f && el.contains(f) && f.dataset.action ? ["action", "ticket", "id", "k", "v"].map(attr).join("") : null;
    el.replaceChildren(...tmp.childNodes);
    $$(sideways, el).forEach((w) => { if (xs[w.dataset.k]) w.scrollLeft = xs[w.dataset.k]; });
    if (fsel) { const ns = $$(fsel, el); if (ns.length === 1) ns[0].focus({ preventScroll: true }); }
    window.scrollTo({ top: y });
  }, 2000);
}

// ── global events ───────────────────────────────────────────
document.addEventListener("click", async (e) => {
  const langBtn = e.target.closest("[data-lang]");
  if (langBtn) return setLang(langBtn.dataset.lang);
  const a = e.target.closest("[data-action]"); if (!a) return;
  const act = a.dataset.action;
  if (act === "logout") { closeWS(); await api("/api/auth/logout", { method: "POST" }).catch(() => {}); S.me = null; location.hash = "#/login"; boot(); }
  else if (act === "menu") $("#side").classList.toggle("open");
  else if (act === "reload") route();
  else if (act === "filter") { const { path, q } = parseHash(); q.set(a.dataset.k, a.dataset.v); location.hash = "#" + path + "?" + q.toString(); }
  else if (act === "exec") {
    a.disabled = true;
    try { await api(`/api/signals/${a.dataset.id}/execute`, { method: "POST" }); toast(t("executing"), "gold"); } catch (err) { toast(esc(errText(err)), "err"); a.disabled = false; }
  }
  else if (act === "dash-custom") dashCustomize();
  else if (act === "exec-mode") {   // in place: a re-render would wipe edits still on their way to the server
    $$("[data-action=exec-mode]").forEach((b) => b.classList.toggle("on", b === a));
    const w = $("#execWarn"); if (w) w.innerHTML = execWarn(a.dataset.v);
    saveSettings({ execution: a.dataset.v });
  }
  else if (act === "sym") {
    a.classList.toggle("on"); saveSettings({ symbols: $$("[data-action=sym].on").map((b) => b.dataset.v) });
    const bc = $("#burstSetCard"); if (bc) bc.innerHTML = burstSetHtml();   // the burst offers only the pairs traded
  }
  else if (act === "burst-toggle") { saveBurst({ enabled: !burstSet().enabled }); renderBurstCard(); }
  else if (act === "burst-test") burstTest();
  else if (act === "burst-cancel") {
    if (!await modal({ title: t("burst_cancel"), body: `<p>${t("burst_cancel_q")}</p>`, danger: true, confirm: t("burst_cancel"), cancel: t("burst_keep") })) return;
    try { const r = await api(`/api/bursts/${encodeURIComponent(a.dataset.id)}/cancel`, { method: "POST" }); if (r && r.status) burstUpsert(r); toast(t("burst_cancel_sent")); if (S.page === pageTrades) refreshSoft(); }
    catch (err) { toast(esc(errText(err)), "err"); }
  }
  else if (act === "tgsave") { try { S.me = await api("/api/me/profile", { method: "PUT", body: { telegram_chat_id: $("#tgId").value } }); toast(t("saved")); } catch (err) { toast(esc(errText(err)), "err"); } }
  else if (act === "tgtest") { try { await api("/api/me/telegram-test", { method: "POST" }); toast("✓ Telegram"); } catch (err) { toast(esc(errText(err)), "err"); } }
  else if (act === "newtoken") {
    const box = await modal({ title: t("new_token"), confirm: t("new_token"), body: `<label class="field"><span>${t("token_label")}</span><input class="input" id="tLabel" value="My PC" maxlength="80"></label>` });
    if (!box) return;
    try {
      const r = await api("/api/connector/tokens", { method: "POST", body: { label: box.querySelector("#tLabel").value } });
      await modal({ title: t("new_token"), confirm: null, cancel: "OK", body: `<p class="muted">${t("token_once")}</p><div class="token-box"><code id="tokVal">${esc(r.token)}</code><button class="btn sm" data-action="copytok">${t("copy")}</button></div>` });
      route();
    } catch (err) { toast(esc(errText(err)), "err"); }
  }
  else if (act === "copytok") { try { await navigator.clipboard.writeText($("#tokVal").textContent); a.textContent = t("copied"); } catch { /* select manually */ } }
  else if (act === "revoke") { if (await modal({ title: t("revoke"), body: `<p>${t("confirm_revoke")}</p>`, danger: true, confirm: t("revoke") })) { try { await api(`/api/connector/tokens/${a.dataset.id}`, { method: "DELETE" }); route(); } catch (err) { toast(esc(errText(err)), "err"); } } }
  else if (act === "closepos") { if (await modal({ title: t("close"), body: `<p>${t("confirm_close", { t: esc(a.dataset.ticket) })}</p>`, danger: true, confirm: t("close") })) { try { await api(`/api/trades/${a.dataset.ticket}/close`, { method: "POST" }); toast(t("executing")); } catch (err) { toast(esc(errText(err)), "err"); } } }
  else if (act === "clearchat") { await api("/api/coach/history", { method: "DELETE" }).catch(() => {}); route(); }
  else if (act === "quickq") { const ta = $("#chatForm textarea"); if (ta && !ta.disabled) { ta.value = a.dataset.q; $("#chatForm").requestSubmit(); } }
  else if (act === "manage") manageUser(Number(a.dataset.id));
  else if (act === "adduser") addUser();
  else if (act === "saveengine") saveEngine();
  else if (act === "tt-side") $$("#ttSide button").forEach((b) => b.classList.toggle("on", b === a));
  else if (act === "tt-send") testSend(a);
  else if (act === "tt-close") testClose(a);
  else if (act === "learn-run") {
    a.disabled = true;
    try { await api("/api/admin/learning/run", { method: "POST" }); S.learnKick = Date.now() + 8000; toast(t("learn_started"), "gold"); }
    catch (err) { toast(esc(errText(err)), "err"); }
    route();
  }
  else if (act === "learn-mode") {
    if (a.classList.contains("on")) return;
    if (a.dataset.v === "auto" && !await modal({ title: t("lm_auto"), body: `<p>${t("learn_auto_confirm")}</p>`, confirm: t("lm_auto") })) return;
    $$("[data-action=learn-mode]").forEach((b) => b.classList.toggle("on", b === a));
    saveLearning({ mode: a.dataset.v });
  }
  else if (act === "learn-apply") {
    if (!await modal({ title: `${t("learn_apply")} #${a.dataset.id}`, body: `<p>${t("learn_apply_q")}</p>`, confirm: t("learn_apply") })) return;
    try { await api(`/api/admin/learning/runs/${encodeURIComponent(a.dataset.id)}/apply`, { method: "POST" }); toast(t("learn_applied"), "gold"); }
    catch (err) { toast(esc(errText(err)), "err"); }
    route();
  }
  else if (act === "learn-revert") {
    const base = a.dataset.base === "1";
    if (!await modal({ title: base ? t("learn_revert_base") : t("learn_revert_prev"), body: `<p>${base ? t("learn_revert_base_q") : t("learn_revert_prev_q")}</p>`, danger: true, confirm: t("learn_revert") })) return;
    try { await api("/api/admin/learning/revert", { method: "POST", body: { to_base: base } }); toast(t("saved")); }
    catch (err) { toast(esc(errText(err)), "err"); }
    route();
  }
  else if (act === "runai") {
    a.disabled = true; const old = a.innerHTML; a.innerHTML = `<span class="spin"></span>${t("running")}`;
    try { const r = await api("/api/admin/engine/run-ai", { method: "POST" }); toast(r.error ? esc(r.error) : `AI ✓ ${r.biases} biases · ${r.headlines} headlines`, r.error ? "err" : "gold"); route(); }
    catch (err) { toast(esc(errText(err)), "err"); a.disabled = false; a.innerHTML = old; }
  }
});
window.addEventListener("hashchange", () => {
  const h = location.hash;
  if (!S.me) { if (h.startsWith("#/register")) renderAuth("register"); else if (h.startsWith("#/login")) renderAuth("login"); return; }
  route();
});
setInterval(() => { if (S.page === pageDashboard || S.page === pageSignals) $$(".sig .meta").length && refreshSoft(); }, 60000);   // keep "x min ago" fresh
setInterval(updateMktPill, 1000);   // the market's clock in the top bar
setInterval(() => $$("[data-countdown]").forEach((el) => {
  el.textContent = countdown(el.dataset.countdown);
  if (Date.now() - new Date(el.dataset.countdown).getTime() > 60e3 && el.closest("#burstCard")) renderBurstCard();   // roll on to the next event
}), 1000);
boot();
})();
