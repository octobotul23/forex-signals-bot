"""The web app (server/web: index.html, app.js, i18n.js, app.css) against the server it talks to: every text it looks
up exists in English and Romanian (same placeholders, no key in one language only), every settings / burst error code
has its text, the assets load with one cache version, the JS parses (node --check), the burst / short-term / Rapid
fields it saves are ones the server keeps (server/burst.py clamp_burst, bot/rapid.py clamp_rapid, bot/scalp.py
clamp_short), its copies of the server's defaults, ranges and pairs are the server's, a v1.2.0 user's burst saved
from the page still runs on an old connector, and its pre-news analysis line, price digits and version gates say what
the Python ones say (run in node). The header's version label, the Settings page's handlers (the variant switch,
the short-term and Rapid pairs kept apart, Rapid's maxima, a unit picked starting from its own amounts, the analysis
and trigger switches, burst picks no longer offered, the burst's banner following each edit, the currency and pair
buttons, a pair under Pairs drawing the burst card again), its redraw on a connector push and after a save, the
burst test's release, a new session's reset and Rapid's name on the Market page, Dashboard and toasts run in node too.

    .venv\\Scripts\\python tests\\test_web.py

Reads the files as text and imports the pure modules only (no database, no app, no network); node runs in a
throw-away folder. Nothing is written inside the project. Exit code 0 = every check passed.
"""
import ast
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import warnings
from pathlib import Path

sys.dont_write_bytecode = True                                  # no __pycache__ left in the project
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
WEB = ROOT / "server" / "web"
TMP = Path(tempfile.mkdtemp(prefix="fx-web-"))
# server.settings is read when server.burst is imported: configure first (.env never overrides these), so no secret
# key file and no database is made in the project
os.environ.update(DATABASE_URL=f"sqlite:///{(TMP / 'platform.db').as_posix()}", SECRET_KEY="web-test-" + "k" * 40,
                  PRICE_FEED="demo", ANTHROPIC_API_KEY="", ANTHROPIC_AUTH_TOKEN="", TELEGRAM_BOT_TOKEN="")
_KEPT = ("data", "logs")
_before = {d: sorted(p.name for p in (ROOT / d).glob("*")) for d in _KEPT}

import bot.news_view as V                                       # noqa: E402
import bot.rapid as R                                           # noqa: E402
import bot.scalp as SC                                          # noqa: E402
import bot.symbols as SY                                        # noqa: E402
import server.burst as B                                        # noqa: E402
import server.dashboard as D                                    # noqa: E402

FAILED: list[str] = []
PASSED: list[str] = []
JS = (WEB / "app.js").read_text(encoding="utf-8")
I18N_SRC = (WEB / "i18n.js").read_text(encoding="utf-8")
HTML = (WEB / "index.html").read_text(encoding="utf-8")
CSS = (WEB / "app.css").read_text(encoding="utf-8")
NODE = shutil.which("node")


def check(ok, label: str, detail=""):
    got = f"\n       got: {detail!r}"[:900] if not ok and detail != "" else ""
    print(f"{'ok  ' if ok else 'FAIL'} {label}{got}")
    (PASSED if ok else FAILED).append(label)


# ── reading JavaScript ─────────────────────────────────────────
def _skip_string(src: str, i: int) -> int:
    """The index after the '...' or "..." string that starts at src[i]."""
    q, i = src[i], i + 1
    while src[i] != q:
        i += 2 if src[i] == "\\" else 1
    return i + 1


def _skip_template(src: str, i: int) -> int:
    """The index after the `...` template that starts at src[i], ${...} parts (and templates in them) included."""
    i += 1
    while src[i] != "`":
        if src[i] == "\\":
            i += 2
        elif src.startswith("${", i):
            i, depth = i + 2, 1
            while depth:
                c = src[i]
                if c in "\"'":
                    i = _skip_string(src, i)
                elif c == "`":
                    i = _skip_template(src, i)
                else:
                    depth += (c == "{") - (c == "}")
                    i += 1
        else:
            i += 1
    return i + 1


def _skip_regex(src: str, i: int) -> int:
    """The index after the /.../flags regex literal that starts at src[i] ([...] classes and escapes included)."""
    i, cls = i + 1, False
    while cls or src[i] != "/":
        if src[i] == "\\":
            i += 1
        elif src[i] in "[]":
            cls = src[i] == "["
        i += 1
    i += 1
    while i < len(src) and src[i].isalpha():
        i += 1
    return i


def _scan(src: str, i: int, stop):
    """Walk src from i over code at bracket depth (strings, templates, regex literals and comments skipped); returns
    the index where stop(char, depth) is first true, after the depth change of that char. A / starts a regex where
    a value is expected: after an operator or an opening bracket (the only places app.js writes one)."""
    depth, last = 0, "("
    while i < len(src):
        c = src[i]
        if c in "\"'":
            i, last = _skip_string(src, i), '"'
            continue
        if c == "`":
            i, last = _skip_template(src, i), '"'
            continue
        if src.startswith("//", i):
            i = src.index("\n", i)
            continue
        if src.startswith("/*", i):
            i = src.index("*/", i) + 2
            continue
        if c == "/" and last in "(,=:[!&|?{};+-*%<>~^":
            i, last = _skip_regex(src, i), '"'
            continue
        if not c.isspace():
            last = c
        depth += (c in "([{") - (c in ")]}")
        if stop(c, depth):
            return i
        i += 1
    raise ValueError("unbalanced source")


def statement(name: str, src: str = JS) -> str:
    """The top-level `const name = ...;` or `[async] function name(...) {...}` of src, as written."""
    m = re.search(rf"^(?:const|(?:async )?function) {re.escape(name)}\b", src, re.M)
    if not m:
        raise KeyError(name)
    fn = not src.startswith("const", m.start())
    end = _scan(src, m.start(), lambda c, d: d == 0 and (c == ";" or (fn and c == "}")))
    return src[m.start():end + 1]


def call_args(name: str, src: str = JS) -> list[str]:
    """The argument text of every call name(...) in src."""
    out = []
    for m in re.finditer(rf"(?<![\w.$]){re.escape(name)}\(", src):
        end = _scan(src, m.end() - 1, lambda c, d: d == 0)
        out.append(src[m.end():end])
    return out


def strip_strings(src: str) -> str:
    """src with every string and template replaced by "" (their insides cannot be mistaken for code)."""
    out, i = [], 0
    while i < len(src):
        c = src[i]
        if c in "\"'`":
            j = _skip_string(src, i) if c != "`" else _skip_template(src, i)
            out.append('""')
            i = j
        else:
            out.append(c)
            i += 1
    return "".join(out)


_TOK = re.compile(r"""(?P<sp>\s+|//[^\n]*|/\*.*?\*/)|(?P<str>"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*')"""
                  r"""|(?P<num>-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)|(?P<name>[A-Za-z_$][\w$]*)|(?P<p>[{}\[\]:,+])""", re.S)


def literal(src: str, at: int = 0):
    """The JS literal at src[at] as Python data: objects (bare or quoted keys), arrays, strings, numbers, true /
    false / null and "a" + "b" joins; anything else (a name, a call) raises ValueError. Repeated keys of one object
    are collected in literal.dupes."""
    pos = [at]

    def tok():
        while True:
            m = _TOK.match(src, pos[0])
            if not m:
                raise ValueError(f"cannot read {src[pos[0]:pos[0] + 40]!r}")
            pos[0] = m.end()
            if m.lastgroup != "sp":
                return m.lastgroup, m.group()

    def peek():
        save = pos[0]
        try:
            return tok()
        finally:
            pos[0] = save

    def value():
        kind, text = tok()
        if kind == "str":
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                v = ast.literal_eval(text)
            while peek() == ("p", "+"):
                tok()
                k2, t2 = tok()
                if k2 != "str":
                    raise ValueError(f"not a string after +: {t2!r}")
                v += ast.literal_eval(t2)
            return v
        if kind == "num":
            return float(text) if re.search(r"[.eE]", text) else int(text)
        if kind == "name" and text in ("true", "false", "null"):
            return {"true": True, "false": False, "null": None}[text]
        if text == "[":
            out = []
            while peek() != ("p", "]"):
                out.append(value())
                if peek() == ("p", ","):
                    tok()
            tok()
            return out
        if text == "{":
            out = {}
            while peek() != ("p", "}"):
                k, key = tok()
                key = ast.literal_eval(key) if k == "str" else key
                if tok() != ("p", ":"):
                    raise ValueError(f"no ':' after {key!r}")
                if key in out:
                    literal.dupes.append(key)
                out[key] = value()
                if peek() == ("p", ","):
                    tok()
            tok()
            return out
        raise ValueError(f"not a literal: {text!r}")

    return value()


literal.dupes = []


def const(name: str):
    """The value of app.js's `const NAME = <literal>`."""
    m = re.search(rf"^const {re.escape(name)} = ", JS, re.M)
    if not m:
        raise KeyError(name)
    return literal(JS, m.end())


def i18n() -> dict:
    m = re.search(r"window\.I18N = ", I18N_SRC)
    return literal(I18N_SRC, m.end())


LANG = i18n()
EN, RO = LANG["en"], LANG["ro"]


# ── the texts ─────────────────────────────────────────────────
def used_keys() -> set:
    """The text keys app.js looks up by name: t("k") / t('k'), both sides of t(c ? "a" : "b"), the labels its field
    helpers pass on (nf, holdFields' f, row), the note lists drawn with .map((k) => ... t(k)) and seg's prefix +
    values."""
    keys = {m[1] for m in re.findall(r"""\bt\(\s*(["'])([a-z][a-z0-9_]*)\1\s*[,)]""", JS)}
    for a, b in re.findall(r"""\bt\((?:[^()"'`]|"[^"]*")*\?\s*"([a-z][a-z0-9_]*)"\s*:\s*"([a-z][a-z0-9_]*)"\s*\)""", JS):
        keys |= {a, b}
    keys |= set(re.findall(r"""\b(?:nf|f)\(\s*"[a-z_]+"\s*,\s*"([a-z][a-z0-9_]*)"\s*,""", JS))
    for a, b in re.findall(r"""\bnf\(\s*"[a-z_]+"\s*,\s*[^,()]*\?\s*"([a-z][a-z0-9_]*)"\s*:\s*"([a-z][a-z0-9_]*)"\s*,""", JS):
        keys |= {a, b}
    keys |= set(re.findall(r"""\brow\(\s*"([a-z][a-z0-9_]*)"\s*[,)]""", JS))
    for lst, _ in re.findall(r"""\[((?:\s*"[a-z0-9_]+"\s*,?)+)\]\.map\(\((\w+)\)\s*=>\s*`[^`]*?\bt\(\2\)""", JS):
        keys |= set(re.findall(r'"([a-z0-9_]+)"', lst))
    for vals, prefix in re.findall(r"""\bseg\(\s*"\w+"\s*,\s*\[([^\]]*)\]\s*,[^,]+,\s*"([a-z0-9_]+_)"\s*\)""", JS):
        keys |= {prefix + v for v in re.findall(r'"(\w+)"', vals)}
    return keys


def dynamic_prefixes() -> set:
    """The prefixes app.js joins a value to: t("prefix" + x), and nf labels written "prefix" + x."""
    found = set(re.findall(r"""\bt\(\s*"([a-z0-9_]+_)"\s*\+""", JS))
    found |= set(re.findall(r"""\bnf\(\s*"\w+"\s*,\s*"([a-z0-9_]+_)"\s*\+""", JS))
    return found


def test_texts():
    check(not literal.dupes, "no text key is written twice in one language", literal.dupes)
    check(set(EN) == set(RO), "every text has both languages (no key in one language only)",
          sorted(set(EN) ^ set(RO)))
    holes = re.compile(r"\{(\w+)\}")
    odd = [k for k in set(EN) & set(RO) if set(holes.findall(EN[k])) != set(holes.findall(RO[k]))]
    check(not odd, "both languages fill the same {placeholders}", sorted(odd))
    keys = used_keys()
    check(len(keys) > 300, f"the texts app.js looks up are found ({len(keys)})", len(keys))
    check(not keys - set(EN), "every text app.js looks up exists in English", sorted(keys - set(EN)))
    check(not keys - set(RO), "every text app.js looks up exists in Romanian", sorted(keys - set(RO)))
    # the families app.js builds from a value: each value has its text
    tiles = const("TILE_SCOPES")
    families = {"burst_u_": B.SL_UNITS, "burst_sl_": [u for u in B.SL_UNITS if u != "pips"], "rp_sl_": R.SL_UNITS,
                "rp_trig_": R.TRIGGERS, "an_p_": V.PARTS, "scope_": set(tiles) | set(D.SCOPES),
                "v_": ("classic", "rapid"), "st_unit_": SC.UNITS, "bs_": B.RANK}
    for prefix, values in families.items():
        miss = [prefix + v for v in values if prefix + v not in EN or prefix + v not in RO]
        check(not miss, f"{prefix}<value>: a text in both languages for {sorted(values)}", miss)
    lone = [p for p in dynamic_prefixes() if not any(k.startswith(p) for k in EN)]
    check(not lone, "every prefix app.js joins a value to has texts", lone)
    # the user's own words for the new settings (RO), and the units of the burst's stop
    want = {"hold_x": "Închide dacă e pe profit după (s)", "hold_y": "Ține cel mult (s)", "lot_order": "Lot per ordin",
            "st_variant": "Variantă", "v_classic": "Clasic", "v_rapid": "Rapid", "burst_u_pips": "pips",
            "burst_u_pct": "% sold", "burst_u_usd": "$"}
    got = {k: RO.get(k) for k in want}
    check(got == want, "the Romanian labels are the requested ones", got)
    got = [EN.get(k) for k in ("hold_x", "hold_y", "lot_order", "st_variant", "v_classic", "v_rapid")]
    check(got == ["Close if in profit after (s)", "Hold at most (s)", "Lot per order", "Variant", "Classic", "Rapid"],
          "and the English ones", got)
    # what needs connector 1.3.0 says so, in both languages
    need = ("need_130", "pairs_130", "burst_conn_130", "rp_n6", "st_variant_d", "burst_an_d", "hold_d", "pip_scale",
            "e_connector_outdated")
    miss = [f"{lang}.{k}" for lang, L in (("en", EN), ("ro", RO)) for k in need if "1.3.0" not in L.get(k, "")]
    check(not miss, "the texts of the v1.3.0 options name connector 1.3.0", miss)
    notes = [k for k in ("rp_n1", "rp_n2", "rp_n3") if k not in EN]
    check(not notes and "demo" in EN["rp_n1"] and "0.5" in EN["rp_n2"] and "0,5" in RO["rp_n2"]
          and "slippage" in EN["rp_n2"] and "expires" in EN["rp_n2"] and "burst" in EN["rp_n3"],
          "Rapid's notes: signal runs on demo only, stop-limit entry (0.5 pip, no slippage, expiry), news pairs", notes)


def test_analysis_texts():
    """The analysis line is bot/news_view.py describe() in app.js: the same words."""
    keys = {"head": "an_head", "shadow": "an_shadow", "clear": "an_clear", "both": "an_both", "spread": "an_spread",
            "blow_out": "an_blow_out", "none": "an_none", "na": "an_na"}
    for lang, L in (("en", EN), ("ro", RO)):
        tx = V._TEXT[lang]
        bad = {k: (L.get(js), tx[k]) for k, js in keys.items() if L.get(js) != tx[k]}
        bad |= {p: (L.get("an_p_" + p), tx["parts"][p]) for p in V.PARTS if L.get("an_p_" + p) != tx["parts"][p]}
        check(not bad, f"{lang}: the analysis words are describe()'s", bad)
    check(const("AN_PARTS") == list(V.PARTS), "the analysis parts in describe()'s order", const("AN_PARTS"))


def test_error_codes():
    """Each code the settings and burst endpoints answer with (their own, and the clamps' ValueError codes they pass
    on) has a text in both languages; so has invalid_input (a request FastAPI refuses, app.js api())."""
    def consts(path: Path, call: str, funcs=None) -> set:
        out = set()
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and (funcs is None or node.name in funcs):
                for c in ast.walk(node):
                    if isinstance(c, ast.Call) and getattr(c.func, "id", None) == call:
                        out |= {a.value for a in c.args if isinstance(a, ast.Constant) and isinstance(a.value, str)
                                and re.fullmatch(r"[a-z_]+", a.value)}
        return out
    codes = consts(ROOT / "server" / "main.py", "HTTPException", {"put_my_settings", "burst_test", "burst_cancel"})
    check({"invalid_burst", "rapid_excludes_short", "invalid_event", "not_found"} <= codes,
          "the endpoints' own codes are found", sorted(codes))
    for path, funcs in ((ROOT / "server" / "burst.py", {"clamp_burst", "_number"}),
                        (ROOT / "bot" / "rapid.py", {"clamp_rapid", "_num"}),
                        (ROOT / "bot" / "scalp.py", {"clamp_short", "_num"}),
                        (ROOT / "server" / "dashboard.py", {"clamp_dashboard"})):
        codes |= consts(path, "ValueError", funcs)
    check({"invalid_triggers", "invalid_analysis", "invalid_currencies", "invalid_dashboard"} <= codes,
          "and the clamps' codes", sorted(codes))
    codes.add("invalid_input")
    miss = sorted(c for c in codes if "e_" + c not in EN or "e_" + c not in RO)
    check(not miss, f"every settings / burst error code has its text in both languages ({len(codes)})", miss)


def test_assets():
    refs = re.findall(r"""(?:href|src)="/static/([\w.]+)\?v=(\d+)\"""", HTML)
    check(sorted(f for f, _ in refs) == ["app.css", "app.js", "i18n.js"] and {v for _, v in refs} == {"17"},
          "index.html loads app.css, i18n.js and app.js with ?v=17", refs)
    check(re.search(r"\.ver\s*\{", CSS) is not None and re.search(r"\.hidden\s*\{", CSS) is not None,
          "app.css has the version label (.ver) and .hidden")
    gone = re.search(r"\.sym-picks button\.gone\s*\{([^}]*)\}", CSS)
    check(gone is not None and "dashed" in gone[1] and "line-through" in gone[1],
          "app.css marks a burst pick no longer offered (.sym-picks button.gone: dashed, struck through)",
          gone and gone[0])
    tag = statement("verTag")
    check("S.info.version" in tag and "V${" in tag, "the header label is V + /api/public/info's version", tag)
    check(len(re.findall(r"\$\{verTag\(\)\}", JS)) >= 2, "the label is in the login page's header and the app's")


def test_node():
    if not NODE:
        print("note: node is not installed: the JavaScript syntax checks are skipped")
        return
    for name in ("app.js", "i18n.js"):
        r = subprocess.run([NODE, "--check", str(WEB / name)], capture_output=True, text=True, timeout=60)
        check(r.returncode == 0, f"node --check {name}", r.stderr.strip())


def test_mirrors():
    """app.js's copies of the server's constants are the server's."""
    check(const("BURST_PAIRS") == B.BURST_PAIRS, "BURST_PAIRS is server/burst.py's")
    check(const("SYMBOL_SCALE") == SY.SYMBOL_SCALE, "SYMBOL_SCALE is bot/symbols.py's", const("SYMBOL_SCALE"))
    check(tuple(const("V130")) == B.V130, "V130 is server/burst.py's", const("V130"))
    rng = const("BURST_RANGE")
    bad = {k: (rng.get(k, [None, None])[:2], [lo, hi]) for k, (lo, hi, _) in B.LIMITS.items()
           if rng.get(k, [None, None])[:2] != [lo, hi]}
    bad |= {f"sl_money.{u}": (rng["sl_money"][u][:2], list(B.SL_MONEY[u][:2])) for u in B.SL_MONEY
            if rng["sl_money"][u][:2] != list(B.SL_MONEY[u][:2])}
    if rng["max_hold_seconds"][:2] != [B.LIMITS["hold_seconds"][0], B.MAX_HOLD]:
        bad["max_hold_seconds"] = rng["max_hold_seconds"]
    check(not bad, "BURST_RANGE is server/burst.py's LIMITS, SL_MONEY and MAX_HOLD", bad)
    money = const("BURST_MONEY_DEF")
    check(money == {"pips": B.SL_MONEY["pct"][2], **{u: B.SL_MONEY[u][2] for u in B.SL_MONEY}},
          "a new stop unit starts from SL_MONEY's default (pips reads pct's)", money)
    check(const("RP_DEF") == R.DEFAULTS, "RP_DEF is bot/rapid.py DEFAULTS")
    check(const("RP_UNIT_DEF") == R.UNIT_DEFAULTS, "RP_UNIT_DEF is bot/rapid.py UNIT_DEFAULTS")
    rng, bad = const("RP_RANGE"), {}
    for key, lim in R.LIMITS.items():
        js = rng["risk"] if key in ("risk_min", "risk_max") else rng[key]
        if isinstance(lim, dict):
            bad |= {f"{key}.{u}": js[u][:2] for u in lim if js[u][:2] != list(lim[u])}
        elif js[:2] != list(lim):
            bad[key] = js[:2]
    if rng["max_hold_seconds"][:2] != [R.LIMITS["hold_seconds"][0], R.MAX_HOLD_TOP]:
        bad["max_hold_seconds"] = rng["max_hold_seconds"]
    check(not bad, "RP_RANGE is bot/rapid.py LIMITS and MAX_HOLD_TOP", bad)
    check(const("ST_DEF") == SC.DEFAULTS, "ST_DEF is bot/scalp.py DEFAULTS")
    check(const("ST_UNIT_DEF") == SC.UNIT_DEFAULTS, "ST_UNIT_DEF is bot/scalp.py UNIT_DEFAULTS")
    rng, bad = const("ST_RANGE"), {}
    for key, lim in SC.RANGES.items():
        js = rng["risk"] if key in ("risk_min", "risk_max") else rng[key]
        if isinstance(lim, dict):
            bad |= {f"{key}.{u}": js[u][:2] for u in lim if js[u][:2] != list(lim[u])}
        elif js[:2] != list(lim):
            bad[key] = js[:2]
    if rng["max_hold_seconds"][:2] != [SC.RANGES["hold_seconds"][0], SC.MAX_HOLD_TOP]:
        bad["max_hold_seconds"] = rng["max_hold_seconds"]
    check(not bad, "ST_RANGE is bot/scalp.py RANGES and MAX_HOLD_TOP", bad)
    tiles = const("TILE_SCOPES")
    check("rapid" in tiles and set(tiles) <= set(D.SCOPES), "the strategy filters include Rapid, all known to "
          "server/dashboard.py", tiles)


def saved_keys(block: str, saver: str, card: str) -> set:
    """The keys app.js can send in a settings block: its defaults object (the save sends it whole), the keys written
    in the saver's calls, and the fields its card draws (nf("key"...), holdFields' X and Y, risk_max and tp_max_usd
    raised in rpWire)."""
    keys = set(const(block))
    for arg in call_args(saver):
        keys |= set(re.findall(r"[{,]\s*([a-z_][a-z0-9_]*)\s*:", strip_strings(arg)))
    body = statement(card)
    keys |= set(re.findall(r"""\bnf\(\s*"([a-z_]+)\"""", body))
    if "holdFields(" in body:
        keys |= {"hold_seconds", "max_hold_seconds"}
    keys.discard("symbols_picked")                              # the page's marker: main.py drops it (web_pairs_kept)
    return keys


def test_saved_fields():
    """Every field the page saves is one the server keeps: its clamp gives it back for some value."""
    tries = (60, 2, 0.5, True, False, "pct", "money", "pips", ["USD"], ["EURUSD"], {"signals": True})

    def kept(clamp, key) -> bool:
        for v in tries:
            try:
                if key in clamp({key: v}):
                    return True
            except (ValueError, TypeError):
                pass
        return False
    cur = B.legacy_burst({})
    burst = saved_keys("BURST_DEF", "saveBurst", "burstSetHtml")
    check({"sl_unit", "sl_money", "lots", "analysis", "symbols", "max_hold_seconds", "currencies"} <= burst,
          "the burst's v1.3.0 fields are found", sorted(burst))
    lost = [k for k in burst if not kept(lambda b: B.clamp_burst(b, 40, cur), k)]
    check(not lost, f"server/burst.py clamp_burst keeps every burst field the page saves ({len(burst)})", lost)
    rapid = saved_keys("RP_DEF", "saveRapid", "rpCardHtml")
    lost = [k for k in rapid if not kept(R.clamp_rapid, k)]
    check(not lost and "max_hold_seconds" in rapid, f"bot/rapid.py clamp_rapid keeps every Rapid field ({len(rapid)})",
          lost)
    short = saved_keys("ST_DEF", "saveShort", "stCardHtml")
    lost = [k for k in short if not kept(SC.clamp_short, k)]
    check(not lost and "max_hold_seconds" in short, f"bot/scalp.py clamp_short keeps every short-term field "
          f"({len(short)})", lost)


def test_legacy():
    """New versions never break the old one: a v1.2.0 user who edits an old field on the page sends back what the
    server served (the page's defaults only fill gaps), and the burst still runs on a v1.2.0 connector."""
    js_def = const("BURST_DEF")
    check("max_hold_seconds" not in js_def, "the page has no Y of its own for the burst (a missing Y follows X)")
    legacy = B.legacy_burst({})
    bad = {k: (v, legacy.get(k)) for k, v in js_def.items() if legacy.get(k) != v}
    check(not bad, "the page's burst defaults are what a v1.2.0 block reads as (legacy_burst)", bad)
    v120 = {"enabled": True, "trades": 12, "lead_seconds": 60, "offset_pips": 7.0, "sl_pips": 10.0,
            "tp_start_pips": 4.0, "tp_step_pips": 1.5, "hold_seconds": 30, "expire_seconds": 90, "currencies": ["USD"]}
    served = B.legacy_burst(v120)
    for edit in ({"trades": 15}, {"hold_seconds": 45}, {"currencies": ["USD", "EUR"]}, {"enabled": False}):
        # a v1.3.0 page still open sends the whole block ({ ...burstSet(), ...patch }); v1.3.1's only the edit
        for page, sent in (("v1.3.0", {**js_def, **served, **edit}), ("v1.3.1", dict(edit))):
            stored = B.raise_max_hold({**served, **B.clamp_burst(sent, 40, served)})
            p = B.params_for({"burst": stored, "risk_per_trade_pct": 1.0, "max_spread_pips": 3.0}, 40)
            check(B.needs_version(p) == B.MIN_CONNECTOR and "max_hold_seconds" not in stored
                  and stored["sl_unit"] == "pips" and stored["analysis"] is False,
                  f"a v1.2.0 burst edited on a {page} page ({edit}) still runs on connector 1.1.0", stored)
    st_old = {k: v for k, v in SC.DEFAULTS.items()}             # a short-term block saved by v1.2.0 (no Y)
    served = SC.clamp_short(None, st_old)
    for page, sent in (("v1.3.0", {**const("ST_DEF"), **served, "risk_min": 1.0}),
                       ("v1.3.1", {"risk_min": 1.0, "risk_unit": served["risk_unit"]})):
        stored = SC.clamp_short(sent, served)
        check("max_hold_seconds" not in stored and stored["risk_min"] == 1.0,
              f"a v1.2.0 short-term block edited on a {page} page gets no Y", stored)
    # the variant switch: on, it moves the flag in one save; off, it saves nothing (stWire)
    wire = statement("stWire")
    check(re.search(r"if \(stOn\(\)\) \{", wire) is not None, "the variant picked while switched off is not saved")


def test_node_logic():
    """anLine, priceDigits, newPair and verAtLeast run in node against describe(), price_digits, new_pair and
    version_ok."""
    if not NODE:
        print("note: node is not installed: the JavaScript logic checks are skipped")
        return
    harness = "\n".join(["const window = {};", I18N_SRC, "const I18N = window.I18N;", "const S = { lang: 'en' };"]
                        + [statement(n) for n in ("t", "priceDigits", "SYMBOL_SCALE", "V130", "newPair", "verAtLeast",
                                                  "AN_PARTS", "anLine")] + ["""
const input = JSON.parse(require("fs").readFileSync(process.argv[2], "utf8"));
const out = { an: [], digits: [], pair: [], ver: [] };
for (const [lang, a] of input.an) { S.lang = lang; out.an.push(anLine(a)); }
for (const s of input.symbols) { out.digits.push(priceDigits(s)); out.pair.push(newPair(s)); }
for (const v of input.versions) out.ver.push(verAtLeast(v, V130));
process.stdout.write(JSON.stringify(out));
"""])
    decisions = [
        {"action": "buy", "score": 0.62, "parts": {"trend": 0.4, "momentum": 0.3, "expect": 0.0, "ai": None}},
        {"action": "sell", "score": -0.71, "parts": {"trend": -0.52, "momentum": -0.6, "expect": 0.02, "ai": -0.2}},
        {"action": "both", "score": 0.21, "parts": {"trend": 0.3, "momentum": -0.2}},
        {"action": "both", "score": 0.1, "parts": {}},
        {"action": "buy", "score": "x", "parts": "bad"},
        {"action": "sell", "score": -0.55, "parts": {"trend": True, "momentum": -2.5, "ai": 3}},
        {"action": "skip", "reason": "blow_out"}, {"action": "skip", "reason": "spread"}, {"action": "nope"},
    ]
    cases, want = [], []
    for lang in ("en", "ro"):
        cases.append([lang, None])
        want.append(V.describe(None, lang))
        for d in decisions:
            for shadow in (False, True):
                cases.append([lang, {**d, "shadow": shadow}])
                want.append(V.describe(d, lang, shadow))
    symbols = ["EURUSD", "USDJPY", "GBPJPY", "XAUUSD", "XAUUSD.m", "gbpjpy", "GBPUSD", "AUDUSD"]
    versions = ["1.2.0", "1.3.0", "1.3.1", "1.10.0", "2.0", "1.2.9", "", None, "v1.3.0-beta"]
    src, inp = TMP / "harness.js", TMP / "input.json"
    src.write_text(harness, encoding="utf-8")
    inp.write_text(json.dumps({"an": cases, "symbols": symbols, "versions": versions}), encoding="utf-8")
    r = subprocess.run([NODE, str(src), str(inp)], capture_output=True, text=True, encoding="utf-8", timeout=60,
                       cwd=TMP)
    check(r.returncode == 0, "the node harness runs", r.stderr.strip()[-600:])
    if r.returncode:
        return
    out = json.loads(r.stdout)
    bad = [(c, g, w) for c, g, w in zip(cases, out["an"], want) if g != w]
    check(not bad, f"anLine is describe(), word for word, in both languages ({len(cases)} cases)", bad[:3])
    want = [SY.price_digits(s) for s in symbols]
    check(out["digits"] == want, "fmtP's decimals are bot/symbols.py price_digits (gold 2, JPY 3, else 5)",
          (out["digits"], want))
    want = [B.new_pair(s) for s in symbols]
    check(out["pair"] == want, "newPair is server/burst.py new_pair", (out["pair"], want))
    want = [B.version_ok(v, B.V130) for v in versions]
    check(out["ver"] == want, "verAtLeast(v, V130) is server/burst.py version_ok(v, V130)", (out["ver"], want))


def node_run(names: list[str], body: str, data) -> tuple[object, str]:
    """Run app.js's statements `names` and what they use (added, in app.js's order, as node reports each one
    missing) with i18n.js, a stand-in S and `body`, which reads `input` (data) and writes its answer as JSON.
    Returns (answer or None, error)."""
    names = list(names)
    for _ in range(80):
        parts = sorted({n: statement(n) for n in names}.items(), key=lambda kv: JS.index(kv[1]))
        src = "\n".join(["const window = {};", I18N_SRC, "const I18N = window.I18N;",
                         "const S = { lang: 'en', info: { version: '1.3.0' }, settings: null, symbols: [], "
                         "connector: { online: false }, burstApp: { enabled: true, max_trades: 40 }, stVariant: null };",
                         *[p for _, p in parts],
                         'const input = JSON.parse(require("fs").readFileSync(process.argv[2], "utf8"));', body])
        (TMP / "cards.js").write_text(src, encoding="utf-8")
        (TMP / "cards.json").write_text(json.dumps(data), encoding="utf-8")
        r = subprocess.run([NODE, str(TMP / "cards.js"), str(TMP / "cards.json")], capture_output=True, text=True,
                           encoding="utf-8", timeout=60, cwd=TMP)
        if r.returncode == 0:
            return json.loads(r.stdout), ""
        m = re.search(r"ReferenceError: (\w+) is not defined", r.stderr)
        if not m or m[1] in names:
            return None, r.stderr.strip()[-800:]
        try:
            statement(m[1])
        except KeyError:
            return None, f"{m[1]} is used but is no top-level const / function of app.js"
        names.append(m[1])
    return None, "too many statements"


def test_cards():
    """The Settings page's short-term, Rapid and burst cards, the Dashboard's burst card and a history line draw for a
    v1.2.0 user, a new user, Rapid on and a connector that is offline: no error, no undefined / NaN, the v1.3.0 marks
    where the connector is older, its banners where the server would hold a run back (needs_version, Rapid on), the
    pip-scale note on each card with pip distances. A run's kind chip (history line, the Trades page's bursts table) is
    Rapid's on a Rapid run only, and Rapid's stop field follows its unit."""
    if not NODE:
        print("note: node is not installed: the card checks are skipped")
        return
    from server.settings import DEFAULT_USER_SETTINGS
    acct = {"balance": 10000, "currency": "USD", "demo": True}
    pairs = ["EURUSD", "GBPUSD", "USDJPY", "XAUUSD", "GBPJPY", "AUDUSD"]
    v120 = {"enabled": True, "trades": 12, "lead_seconds": 60, "offset_pips": 7.0, "sl_pips": 10.0,
            "tp_start_pips": 4.0, "tp_step_pips": 1.5, "hold_seconds": 30, "expire_seconds": 90, "currencies": []}

    def user(burst, st=None, rp=None):
        return {"symbols": [], "burst": B.legacy_burst(burst), "short_term": SC.clamp_short(None, st),
                "rapid": R.clamp_rapid(None, rp if rp is not None else DEFAULT_USER_SETTINGS["rapid"])}
    old = {"online": True, "version": "1.2.0", "account": acct}
    new = {"online": True, "version": "1.3.0", "account": acct}
    scenarios = {
        "a v1.2.0 user, connector 1.2.0": {"lang": "en", "settings": user(v120), "connector": old},
        "a new user, connector 1.2.0": {"lang": "en", "settings": user(DEFAULT_USER_SETTINGS["burst"]),
                                        "connector": old},
        "Rapid on (money stop), connector 1.3.0, in Romanian": {
            "lang": "ro", "connector": new,
            "settings": user(v120, rp=R.clamp_rapid({"enabled": True, "sl_unit": "money"}))},
        "a $ stop, connector offline": {"lang": "en", "connector": {"online": False},
                                        "settings": user({**v120, "sl_unit": "usd", "sl_money": 150.0})},
        "Rapid on, a v1.2.0 burst, connector 1.2.0": {
            "lang": "en", "connector": old, "settings": user(v120, rp=R.clamp_rapid({"enabled": True}))},
        "a $ stop over 10% of a small balance, connector 1.3.0": {
            "lang": "en", "connector": {**new, "account": {**acct, "balance": 1000}},
            "settings": user({**v120, "sl_unit": "usd", "sl_money": 150.0})},
        "Rapid on, its pairs gone (rapid_note), connector 1.2.0, in Romanian": {
            "lang": "ro", "connector": old,
            "settings": {**user(v120, rp=R.clamp_rapid({"enabled": True})), "rapid_note": "pairs_gone"}},
        "short-term on, its pairs gone (short_term_note), connector 1.3.0": {
            "lang": "en", "connector": new,
            "settings": {**user(v120, st={**SC.DEFAULTS, "enabled": True, "hold_seconds": 45}),
                         "short_term_note": "pairs_gone"}},
        "short-term on, connector 1.2.0": {
            "lang": "en", "connector": old, "settings": user(v120, st={**SC.DEFAULTS, "enabled": True})},
    }
    decision = {"action": "buy", "score": 0.62, "parts": {"trend": 0.4, "momentum": 0.3}}
    run = {"id": 7, "kind": "rapid", "status": "closed", "pnl": 5.0, "symbol": "XAUUSD", "event_title": "CPI",
           "side": "buy", "orders": 10, "fills": 3, "event_time": "2026-11-03T13:30:00Z", "test": False,
           "analysis": {**decision, "shadow": True}}
    data = [{"S": {**sc, "symbols": pairs}, "run": run} for sc in scenarios.values()]
    body = """const out = [];
for (const sc of input) {
  Object.assign(S, sc.S, { stVariant: null });
  // the Dashboard card's connector view, as burstConnSync keeps it
  S.burst = { connector: { online: !!S.connector.online, version: S.connector.version || null, demo: true }, bursts: [], upcoming: [] };
  out.push({ st: stCardHtml(), rp: rpCardHtml(), burst: burstSetHtml(), line: burstLine(sc.run, "USD"), variant: stVariant(),
    dash: burstCardHtml() });
}
process.stdout.write(JSON.stringify(out));"""
    got, err = node_run(["stCardHtml", "rpCardHtml", "burstSetHtml", "burstLine", "burstCardHtml"], body, data)
    check(got is not None, "the cards draw in node", err)
    if got is None:
        return
    word = lambda on: "on" if on else "off"     # noqa: E731
    for (name, sc), card in zip(scenarios.items(), got):
        html = card["st"] + card["rp"] + card["burst"] + card["line"] + card["dash"]
        L = LANG[sc["lang"]]
        bad = [w for w in ("undefined", "NaN", "[object Object]") if w in html]
        check(not bad, f"{name}: nothing undefined in the cards", bad)
        need = f'<span class="chip warn">{L["need_130"]}</span>' in html   # the notes say it too, always
        check(need == (sc["connector"].get("version") == "1.2.0"), f"{name}: the 'needs connector 1.3.0' chip only "
              "when the connector is older", need)
        bs, older = sc["settings"]["burst"], sc["connector"].get("version") == "1.2.0"
        # what the server holds back from an older connector: a burst needs_version gives 1.3.0 (its pair may be a
        # new one), and Rapid (its runs are all v1.3.0)
        uses = (B.needs_version(B.params_for({"burst": bs, "risk_per_trade_pct": 1.0, "max_spread_pips": 3.0}, 40))
                == B.V130 or any(B.new_pair(s) for s in bs.get("symbols") or []))
        rapid_on = sc["settings"]["rapid"]["enabled"]
        banner = L["burst_conn_130"] in card["burst"]
        check(banner == (older and (uses or rapid_on)), f"{name}: the burst's 'older connector skips it' banner only "
              "when its options need 1.3.0, or Rapid is on", banner)
        banner = L["burst_conn_130"] in card["dash"]
        check(banner == (older and (uses or rapid_on)), f"{name}: the Dashboard's burst card says so on the same rule",
              banner)
        # every card with pip distances (the short-term card's max spread too) says they are scaled per pair, as the
        # connector scales them (spec.scale)
        scale = L["pip_scale"].replace("{list}", ", ".join(f"{s} ×{k}" for s, k in const("SYMBOL_SCALE").items()))
        check(all(scale in card[k] for k in ("st", "rp", "burst")), f"{name}: the pip-scale note on the short-term, "
              "Rapid and burst cards", [k for k in ("st", "rp", "burst") if scale not in card[k]])
        money = 'data-burst="sl_money"' in card["burst"] and 'data-burst="lots"' in card["burst"]
        check(money == (bs["sl_unit"] != "pips") and ('data-burst="sl_pips"' in card["burst"]) == (not money),
              f"{name}: the stop field follows its unit (pips, or money + lot per order)", bs["sl_unit"])
        y = re.search(r'data-burst="max_hold_seconds" value="(\d+)"', card["burst"])
        want = str(bs.get("max_hold_seconds", bs["hold_seconds"]))
        check(y is not None and y[1] == want, f"{name}: the burst's Y shows {want} (a missing Y shows X)",
              y and y[1])
        check(card["variant"] == ("rapid" if sc["settings"]["rapid"]["enabled"] else "classic")
              and L["st_variant"] in card["st"], f"{name}: the short-term card shows the variant on", card["variant"])
        # each v1.3.0 option is marked where it is, for a connector before 1.3.0 only (need130): the short-term card's
        # Y (and its variant row while Rapid is the variant shown), the burst's stop unit, Y and analysis, and the
        # pairs' hints; the Rapid card once, in its head
        chip, rapid = f'<span class="chip warn">{L["need_130"]}</span>', card["variant"] == "rapid"
        after_seg = lambda attr: r'<div class="seg" ' + attr + r'>(?:(?!</div>).)*</div> ' + re.escape(chip)  # noqa: E731
        marks = {"short-term Y": f'{L["hold_y"]} {chip}' in card["st"],
                 "short-term variant": re.search(after_seg("data-st-variant"), card["st"]) is not None,
                 "burst stop unit": re.search(after_seg("data-burst-unit"), card["burst"]) is not None,
                 "burst Y": f'{L["hold_y"]} {chip}' in card["burst"],
                 "burst analysis": f'<b>{L["burst_an"]}</b> {chip}' in card["burst"],
                 "short-term pairs": f'{L["st_pairs_hint"]} · {L["pairs_130"]}' in card["st"],
                 "burst pairs": f'{L["burst_pairs_hint"]} · {L["pairs_130"]}' in card["burst"]}
        want = {k: older and (k != "short-term variant" or rapid) for k in marks}
        counts = {k: card[k].count(chip) for k in ("st", "rp", "burst")}
        check(marks == want and counts == ({"st": 1 + rapid, "rp": 1, "burst": 3} if older else dict.fromkeys(counts, 0))
              and (older or L["pairs_130"] not in card["st"] + card["burst"]),
              f"{name}: the 'needs connector 1.3.0' chips and pair hints at each v1.3.0 option "
              f"{'(connector older)' if older else 'none (connector 1.3.0 or offline)'}", (marks, counts))
        # each card's Y field has X as its minimum (a Y under X cannot be typed in)
        blocks = (("st", "short_term"), ("rp", "rapid"), ("burst", "burst"))
        mins = {k: re.search(rf'min="([^"]*)" max="[^"]*" data-{k}="max_hold_seconds"', card[k]) for k, _ in blocks}
        mins = {k: m and m[1] for k, m in mins.items()}
        want = {k: str(sc["settings"][b]["hold_seconds"]) for k, b in blocks}
        check(mins == want, f"{name}: each card's Y field has its X as minimum {want}", mins)
        # the switch says what the variant shown does: Rapid's own words, or the classic one's with its X
        classic = L["st_switch_d"].replace("{s}", str(sc["settings"]["short_term"]["hold_seconds"]))
        said = (L["rp_switch_d"] in card["st"], classic in card["st"])
        check(said == (rapid, not rapid), f"{name}: the short-term switch says what the {card['variant']} variant does",
              said)
        # a block's pairs gone (main.py's short_term_note / rapid_note): its own card's banner, never the other's
        notes = (L["st_pairs_gone"] in card["st"], L["rp_pairs_gone"] in card["rp"])
        want = (sc["settings"].get("short_term_note") == "pairs_gone", sc["settings"].get("rapid_note") == "pairs_gone")
        check(notes == want, f"{name}: the 'pairs gone' banner on the short-term card {word(want[0])}, on Rapid's "
              f"{word(want[1])} (each block's own note)", notes)
        line = html_escape(V.describe(decision, sc["lang"], True))
        check(line in card["line"] and L["v_rapid"] in card["line"], f"{name}: a history line has the kind and the "
              "analysis", card["line"][-300:])
    money = got[3]["burst"]
    check("≈ $150.00" in money, "a $ stop shows what the whole burst may lose, ≈", re.findall(r"≈[^<]*", money))
    money = got[1]["burst"]
    check("≈ $200.00" in money, "a % stop shows its amount for the balance (2% of 10,000)", re.findall(r"≈[^<]*", money))
    money, cap = got[5]["burst"], f"≈ ${SC.MAX_RISK_PCT / 100 * 1000:,.2f}"   # bot/risk.py money_for's cap
    check(cap in money, f"a $ stop over {SC.MAX_RISK_PCT:g}% of the balance shows the capped amount ({cap})",
          re.findall(r"≈[^<]*", money))
    # a run's kind: the Rapid chip on a Rapid run only; a classic one (kind "burst", or none: a row from before v1.3.0)
    # says Classic, in a history line and in the Trades page's bursts table (pageTrades, its GETs stood in)
    runs = [run, {**run, "id": 8, "kind": "burst", "symbol": "GBPUSD"},
            {**{k: v for k, v in run.items() if k != "kind"}, "id": 9, "symbol": "EURUSD"}]
    # Rapid's stop field follows its unit: pips asks for the stop in pips, money for the lot per order (any risk unit)
    stops = [(u, r) for u in R.SL_UNITS for r in ("pct", "usd")]
    rapid = [R.clamp_rapid({"sl_unit": u, "risk_unit": r}) for u, r in stops]
    body = """async function api(path) {
  if (path.startsWith("/api/trades?")) return { trades: [] };
  if (path.startsWith("/api/trades/stats")) return { periods: { "30": {} }, account: "demo" };
  if (path === "/api/bursts") return { bursts: input.runs, app: S.burstApp };
  throw new Error("no GET " + path + " here");
}
function $() { return null; }
const out = { lines: [], trades: [], rp: [] };
(async () => {
  for (const lang of ["en", "ro"]) {
    S.lang = lang;
    out.lines.push(input.runs.map((b) => burstLine(b, "USD")));
    const el = { innerHTML: "" };
    await pageTrades(el, new URLSearchParams(""));
    out.trades.push(el.innerHTML);
  }
  S.lang = "en";
  for (const rp of input.rapid) { S.settings = { rapid: rp }; out.rp.push(rpCardHtml()); }
  process.stdout.write(JSON.stringify(out));
})();"""
    got, err = node_run(["burstLine", "pageTrades", "rpCardHtml"], body, {"runs": runs, "rapid": rapid})
    check(got is not None, "the history lines, the Trades page and the Rapid card draw in node", err)
    if got is None:
        return
    for lang, lines, page in zip(("en", "ro"), got["lines"], got["trades"]):
        L = LANG[lang]
        chips = {"rapid": f'<span class="chip info">{L["v_rapid"]}</span>',
                 "burst": f'<span class="chip">{L["v_classic"]}</span>'}
        rows = {}
        for row in re.findall(r"<tr>(.*?)</tr>", page[page.find('data-k="bursts"'):], re.S):
            m = re.search(r'<span class="faint">#(\d+)</span></td><td>(.*?)</td>', row)
            if m:
                rows[int(m[1])] = (m[2], row)
        for b, line in zip(runs, lines):
            kind = b.get("kind") or "burst"
            other = L["v_classic"] if kind == "rapid" else L["v_rapid"]
            cell, row = rows.get(b["id"], (None, ""))
            check(chips[kind] in line and other not in line and cell == chips[kind] and other not in row,
                  f"{lang}: a run of kind {b.get('kind', '(none)')} shows the "
                  f"{'Rapid' if kind == 'rapid' else 'Classic'} chip only, in its history line and its Trades row",
                  (line[:300], cell, row[:300]))
        # the bursts table: a header over each cell of a run's row (the Kind column too), a run's analysis under its
        # event, as its history line has it
        table = page[page.find('data-k="bursts"'):]
        table = table[:table.find("</table>")]
        heads = len(re.findall(r"<th[\s>]", table))
        cells = [len(re.findall(r"<td[\s>]", r)) for r in re.findall(r"<tr>(.*?)</tr>", table, re.S) if "<td" in r]
        an = html_escape(V.describe(decision, lang, True))
        check(f"<th>{L['burst_kind']}</th>" in table and len(cells) == len(runs) and set(cells) == {heads}
              and an in rows.get(7, (None, ""))[1], f"{lang}: the Trades page's bursts table has a Kind header, one "
              "header per cell of each run's row, and a run's analysis under its event", (heads, cells))
    for (unit, risk), html in zip(stops, got["rp"]):
        fields = ('data-rp="sl_pips"' in html, 'data-rp="lots"' in html)
        check(fields == (unit == "pips", unit == "money"), f"Rapid, a stop in {unit} (risk in {risk}): "
              + ("the stop in pips, no lot field" if unit == "pips" else "the lot per order, no pips field"), fields)


def test_hold_clear():
    """A Y field cleared on a card (burst, short-term, Rapid) is saved as max_hold_seconds null, which the server
    removes (Y follows X again, as up to v1.2.0), and the field shows X; a Y typed in is sent, one left alone is not
    sent (v1.3.1: the server keeps it), and a block served without Y (a v1.2.0 one) is sent without it, its X changed
    too. An X typed over Y
    raises Y to it (sent, and shown in the Y field, whose minimum follows X), on each card; an X of 45.6 goes out and
    shows as 46 (a whole number field). The cards' real change handlers run in node (saveSettings and $ stand in, $
    with the card's Y field)."""
    if not NODE:
        print("note: node is not installed: the hold field checks are skipped")
        return
    from server.settings import DEFAULT_USER_SETTINGS
    v120 = {"enabled": True, "trades": 12, "lead_seconds": 60, "offset_pips": 7.0, "sl_pips": 10.0,
            "tp_start_pips": 4.0, "tp_step_pips": 1.5, "hold_seconds": 30, "expire_seconds": 90, "currencies": ["USD"]}
    burst_y = B.legacy_burst({**v120, "max_hold_seconds": 200})
    burst_old = B.legacy_burst(v120)
    st_y = SC.clamp_short({"hold_seconds": 40, "max_hold_seconds": 120})
    st_old = SC.clamp_short(None, dict(SC.DEFAULTS))
    rp_y = R.clamp_rapid(None, DEFAULT_USER_SETTINGS["rapid"])
    rp_y60 = R.clamp_rapid({"hold_seconds": 30, "max_hold_seconds": 60})
    block = {"burst": "burst", "st": "short_term", "rp": "rapid"}
    cases = {   # name: (card, served block, field, value typed, a value the browser cannot read)
        "burst: Y cleared": ("burst", burst_y, "max_hold_seconds", "", False),
        "burst: Y typed in": ("burst", burst_y, "max_hold_seconds", "300", False),
        "burst: X changed, Y kept": ("burst", burst_y, "hold_seconds", "45", False),
        "burst: X 45.6 typed (a whole number field)": ("burst", burst_y, "hold_seconds", "45.6", False),
        "burst: X 300 over a Y of 200": ("burst", burst_y, "hold_seconds", "300", False),
        "burst: Y unreadable": ("burst", burst_y, "max_hold_seconds", "", True),
        "burst (v1.2.0): another field": ("burst", burst_old, "trades", "15", False),
        "burst (v1.2.0): X changed": ("burst", burst_old, "hold_seconds", "45", False),
        "short-term: Y cleared": ("st", st_y, "max_hold_seconds", "", False),
        "short-term: X 150 over a Y of 120": ("st", st_y, "hold_seconds", "150", False),
        "short-term (v1.2.0): another field": ("st", st_old, "risk_min", "1", False),
        "short-term (v1.2.0): X changed": ("st", st_old, "hold_seconds", "45", False),
        "Rapid: Y cleared": ("rp", rp_y, "max_hold_seconds", "", False),
        "Rapid: X 120 over a Y of 60": ("rp", rp_y60, "hold_seconds", "120", False),
        "Rapid: another field": ("rp", rp_y, "orders", "12", False),
    }
    data = [{"card": c, "settings": {block[c]: served, "symbols": []}, "key": k, "value": v, "bad": bad,
             "min": served["hold_seconds"] if k == "max_hold_seconds" else 1, "max": 1800,
             "y": str(served.get("max_hold_seconds", served["hold_seconds"]))}     # the Y field as drawn
            for c, served, k, v, bad in cases.values()]
    body = """let savePatch = {}, saveSent = {};
const sent = [];
function saveSettings(p) { mergeSettings(S.settings, p); sent.push(JSON.parse(JSON.stringify(p))); }
let boxes = {};
function $(sel) { return boxes[sel] || null; }
const out = [];
for (const c of input) {
  S.settings = c.settings; sent.length = 0;
  const y = { min: String(c.settings[{ st: "short_term", rp: "rapid", burst: "burst" }[c.card]].hold_seconds), value: c.y };
  boxes = { [`[data-${c.card}=max_hold_seconds]`]: y };
  const h = {}, card = { addEventListener: (k, fn) => { h[k] = fn; } };
  ({ st: stWire, rp: rpWire, burst: burstWire })[c.card](card);
  const inp = { dataset: { [c.card]: c.key }, value: c.value, min: String(c.min), max: String(c.max), step: "1",
    validity: { badInput: c.bad }, matches: () => false };
  h.change({ target: inp });
  out.push({ sent: sent.slice(), shown: String(inp.value), y: [String(y.min), String(y.value)] });
}
process.stdout.write(JSON.stringify(out));"""
    got, err = node_run(["stWire", "rpWire", "burstWire"], body, data)
    check(got is not None, "the cards' change handlers run in node", err)
    if got is None:
        return
    for (name, (card, served, key, value, bad)), res in zip(cases.items(), got):
        blocks = [s[block[card]] for s in res["sent"] if block[card] in s]
        if len(blocks) != 1:
            check(False, f"{name}: one save of the {block[card]} block", res["sent"])
            continue
        sent = {k: v for k, v in blocks[0].items() if k != "symbols_picked"}   # main.py drops the page's marker
        y = sent.get("max_hold_seconds", "absent")
        typed = str(int(float(value) + 0.5)) if value else value     # numIn: a step 1 field keeps a whole number
        if key == "max_hold_seconds" and value == "" and not bad:
            want_y, want_shown = None, str(served["hold_seconds"])
        elif key == "max_hold_seconds":
            want_y, want_shown = int(typed) if value else served["hold_seconds"], typed or str(served["hold_seconds"])
        else:                                   # v1.3.1: only the keys edited go out, the server keeps its Y
            want_y, want_shown = "absent", typed
            y_had = served.get("max_hold_seconds")
            if key == "hold_seconds" and y_had is not None and y_had < int(typed):
                want_y = int(typed)             # holdFollow: Y raised to X, never under it
        x = "" if key == "max_hold_seconds" else f", {key} sent as {typed}"
        check(y == want_y and res["shown"] == want_shown and (not x or sent.get(key) == int(typed)),
              f"{name}: Y sent as {want_y}, the field shows {want_shown}{x}", (y, res["shown"], sent.get(key)))
        # the Y field: X typed, its minimum is X, and it shows X where Y is missing or under it, else Y as drawn
        drawn = str(served.get("max_hold_seconds", served["hold_seconds"]))
        if key == "hold_seconds":
            low = served.get("max_hold_seconds") is None or served["max_hold_seconds"] < int(typed)
            want_box = [typed, typed if low else drawn]
        else:
            want_box = [str(served["hold_seconds"]), drawn]
        check(res["y"] == want_box, f"{name}: the Y field's minimum and value are {want_box}", res["y"])
        # what the server keeps of it (main.py put_my_settings)
        if card == "burst":
            kept = B.raise_max_hold({**served, **B.clamp_burst(sent, 40, served)})
        elif card == "st":
            kept = SC.clamp_short(sent, served)
        else:
            kept = R.clamp_rapid(sent, served)
        if want_y == "absent" and served.get("max_hold_seconds") is not None:
            want_y = served["max_hold_seconds"]     # not sent: the server keeps the Y it has
        if want_y is None or want_y == "absent":
            ok = "max_hold_seconds" not in kept
            if card == "burst" and ok:
                p = B.params_for({"burst": kept, "risk_per_trade_pct": 1.0, "max_spread_pips": 3.0}, 40)
                ok = p["max_hold_seconds"] == p["hold_seconds"] and (served.get("sl_unit") != "pips"
                                                                     or B.needs_version(p) == B.MIN_CONNECTOR)
            check(ok, f"{name}: the server keeps no Y (it follows X)", kept)
        else:
            check(kept.get("max_hold_seconds") == max(kept["hold_seconds"], want_y),
                  f"{name}: the server keeps Y {want_y}", kept)


# the stand-ins of the handler checks: saveSettings records each patch (what the debounced PUT would carry), the
# page's save state is the body's own (app.js keeps it in a `let`)
SAVES = """let savePatch = {}, saveSent = {}, saveLoud = false, saveDone = 0, saveEpoch = 0;
const sent = [];
function saveSettings(p) { mergeSettings(S.settings, p); sent.push(JSON.parse(JSON.stringify(p))); }
"""


def test_version_label():
    """The header's label is V + the version /api/public/info gives (bot/version.py APP_VERSION, server/main.py
    VERSION), and nothing while that is not known (verTag runs in node)."""
    if not NODE:
        print("note: node is not installed: the version label check is skipped")
        return
    from bot.version import APP_VERSION
    body = """S.info = { version: input };
const out = [verTag()];
S.info = null; out.push(verTag());
S.info = {}; out.push(verTag());
process.stdout.write(JSON.stringify(out));"""
    got, err = node_run(["verTag"], body, APP_VERSION)
    check(got is not None and f">V{APP_VERSION}<" in got[0] and got[1:] == ["", ""],
          f"the header shows V{APP_VERSION}, nothing before /api/public/info answers", got or err)


def test_burst_gate():
    """burstUses130 (the burst's "an older connector skips it" banner) is the server's rule, block by block (run in
    node): needs_version of its params gives 1.3.0, or one of its picked pairs is a new one (new_pair)."""
    if not NODE:
        print("note: node is not installed: the burst gate check is skipped")
        return
    from server.settings import DEFAULT_USER_SETTINGS
    v120 = {"enabled": True, "trades": 12, "lead_seconds": 60, "offset_pips": 7.0, "sl_pips": 10.0,
            "tp_start_pips": 4.0, "tp_step_pips": 1.5, "hold_seconds": 30, "expire_seconds": 90, "currencies": []}
    old = B.legacy_burst(v120)
    blocks = {"a v1.2.0 block": old, "XAUUSD picked": {**old, "symbols": ["XAUUSD"]},
              "GBPJPY picked": {**old, "symbols": ["GBPJPY"]}, "GBPUSD picked": {**old, "symbols": ["GBPUSD"]},
              "Y over X": B.legacy_burst({**v120, "max_hold_seconds": 60}),
              "Y equal to X": B.legacy_burst({**v120, "max_hold_seconds": 30}),
              "the analysis on": {**old, "analysis": True}, "a stop in %": B.legacy_burst({**v120, "sl_unit": "pct"}),
              "a stop in $": B.legacy_burst({**v120, "sl_unit": "usd"}),
              "a new user": B.legacy_burst(DEFAULT_USER_SETTINGS["burst"])}
    want = [B.needs_version(B.params_for({"burst": b, "risk_per_trade_pct": 1.0, "max_spread_pips": 3.0}, 40)) == B.V130
            or any(B.new_pair(s) for s in b.get("symbols") or []) for b in blocks.values()]
    body = """const out = [];
for (const b of input) { S.settings = { burst: b }; out.push(burstUses130(burstSet())); }
process.stdout.write(JSON.stringify(out));"""
    got, err = node_run(["burstUses130", "burstSet"], body, list(blocks.values()))
    bad = {n: (g, w) for n, g, w in zip(blocks, got or [], want) if g != w}
    check(got is not None and not bad and True in want and False in want,
          f"burstUses130 is needs_version + new_pair for {len(blocks)} blocks", bad or err)


def test_variant_switch():
    """The short-term card's variant buttons and switch (stWire's real handlers, in node): switched on, a pick moves
    the flag to that variant, the other one switched off in the same save (the server keeps one of the two on);
    switched off, a pick saves nothing and the switch turns on the variant shown. A pick shows Rapid's card for Rapid
    only, and so does the page's first draw (pageSettings, its GET stood in)."""
    if not NODE:
        print("note: node is not installed: the variant checks are skipped")
        return
    st, rp = SC.clamp_short(None, dict(SC.DEFAULTS)), R.clamp_rapid(None)
    cases = {   # name: (short-term on, Rapid on, the variant the page shows when both are off, what is done)
        "Rapid on, classic picked": (False, True, None, ["click", "classic"]),
        "short-term on, Rapid picked": (True, False, None, ["click", "rapid"]),
        "both off, Rapid shown, switched on": (False, False, "rapid", ["switch", True]),
        "both off, classic shown, switched on": (False, False, "classic", ["switch", True]),
        "both off, Rapid picked": (False, False, "classic", ["click", "rapid"]),
    }
    want = {"Rapid on, classic picked": [["rapid", False], ["short_term", True]],
            "short-term on, Rapid picked": [["short_term", False], ["rapid", True]],
            "both off, Rapid shown, switched on": [["rapid", True]],
            "both off, classic shown, switched on": [["short_term", True]],
            "both off, Rapid picked": []}
    data = [{"settings": {"short_term": {**st, "enabled": s_on}, "rapid": {**rp, "enabled": r_on}, "symbols": []},
             "variant": shown, "do": do} for s_on, r_on, shown, do in cases.values()]
    body = SAVES + """const rc = { hidden: null, classList: { toggle: (k, on) => { if (k === "hidden") rc.hidden = on; } } };
function $(sel) { return sel === "#rpCard" ? rc : null; }
const out = [];
for (const c of input) {
  S.settings = c.settings; S.stVariant = c.variant; sent.length = 0; rc.hidden = null;
  const h = {}, card = { addEventListener: (k, fn) => { h[k] = fn; } };
  stWire(card);
  if (c.do[0] === "click") {
    const vb = { dataset: { v: c.do[1] } };
    h.click({ target: { closest: (sel) => (sel === "[data-st-variant] button" ? vb : null) } });
  } else h.change({ target: { matches: (sel) => sel === "[data-st-on]", checked: c.do[1], dataset: {} } });
  out.push({ saves: sent.flatMap((p) => Object.keys(p).map((k) => [k, p[k].enabled])), variant: stVariant(),
    on: [S.settings.short_term.enabled, S.settings.rapid.enabled], rpHidden: rc.hidden });
}
process.stdout.write(JSON.stringify(out));"""
    got, err = node_run(["stWire"], body, data)
    check(got is not None, "the variant handlers run in node", err)
    for (name, (s_on, r_on, shown, do)), res in zip(cases.items(), got or []):
        on = {"short_term": s_on, "rapid": r_on, **dict(want[name])}       # what the saves leave on, never both
        shown = do[1] if do[0] == "click" else shown
        hidden = (do[1] != "rapid") if do[0] == "click" else None          # the switch leaves the cards to the PUT
        check(res["saves"] == want[name] and res["on"] == [on["short_term"], on["rapid"]] and res["variant"] == shown
              and res["rpHidden"] == hidden, f"{name}: saves {want[name]}, shows {shown}"
              + ("" if hidden is None else f", Rapid's card {'hidden' if hidden else 'shown'}"), res)
    # the page's first draw: Rapid's card is there, hidden unless Rapid is the variant shown
    pages = {   # name: (short-term on, Rapid on, the variant picked on this page before)
        "Rapid on": (False, True, None), "short-term on": (True, False, None), "both off": (False, False, None),
        "both off, Rapid picked before": (False, False, "rapid")}
    base = {"execution": "notify", "modes": {"trend": True, "custom": False, "news_ai": False}, "symbols": [],
            "burst": B.legacy_burst({}), "min_confidence": 70, "follow_exits": False, "notify_browser": False,
            "notify_telegram": False}
    data = [{"settings": {**base, "short_term": {**st, "enabled": s_on}, "rapid": {**rp, "enabled": r_on}},
             "variant": v} for s_on, r_on, v in pages.values()]
    body = SAVES + """function $() { return { addEventListener() {} }; }
function $$() { return []; }
let served = null;
async function api(path) {
  if (path === "/api/me/settings") return { settings: JSON.parse(JSON.stringify(served)), symbols: ["EURUSD", "GBPUSD"],
    modes_enabled: { trend: true, custom: true, news_ai: true } };
  throw new Error("no GET " + path + " here");
}
const out = [];
(async () => {
  S.me = { name: "T", email: "t@example.com" }; S.info = { version: "1.3.0", telegram_enabled: false };
  for (const c of input) {
    S.settings = null; served = c.settings; S.stVariant = c.variant;
    const el = { innerHTML: "" };
    await pageSettings(el);
    const m = /<div class="card ([^"]*)" id="rpCard">/.exec(el.innerHTML);
    out.push(m ? m[1].trim() : null);
  }
  process.stdout.write(JSON.stringify(out));
})();"""
    got, err = node_run(["pageSettings", "mergeSettings", "isBlock"], body, data)   # used in a try (loadSettings): named here
    check(got is not None, "the Settings page draws in node", err)
    for (name, (s_on, r_on, v)), cls in zip(pages.items(), got or []):
        rapid = r_on or (not s_on and v == "rapid")
        check(cls == ("" if rapid else "hidden"), f"{name}: the page draws Rapid's card "
              f"{'shown' if rapid else 'hidden'}", cls)


def test_switch_text():
    """The line under the short-term switch follows the classic X typed in only while the classic variant is shown;
    while Rapid is, it keeps Rapid's own words, the X still saved (stWire's real change handler, in node, the line's
    box stood in)."""
    if not NODE:
        print("note: node is not installed: the switch line checks are skipped")
        return
    st, rp = SC.clamp_short(None, dict(SC.DEFAULTS)), R.clamp_rapid(None)
    cases = {   # name: (short-term on, Rapid on)
        "classic on": (True, False), "both off (classic shown)": (False, False), "Rapid on": (False, True)}
    data = [{"settings": {"short_term": {**st, "enabled": s_on}, "rapid": {**rp, "enabled": r_on}, "symbols": []}}
            for s_on, r_on in cases.values()]
    body = SAVES + """let boxes = {};
function $(sel) { return boxes[sel] || null; }
const out = [];
for (const c of input) {
  S.settings = JSON.parse(JSON.stringify(c.settings)); S.stVariant = null; sent.length = 0;
  const d = { innerHTML: "kept" };
  boxes = { "#stSwitchD": d };
  const h = {}, card = { addEventListener: (k, fn) => { h[k] = fn; } };
  stWire(card);
  h.change({ target: { dataset: { st: "hold_seconds" }, value: "45", min: "10", max: "300", step: "1",
    validity: { badInput: false }, matches: () => false } });
  out.push({ line: d.innerHTML, sent: sent.map((p) => p.short_term && p.short_term.hold_seconds) });
}
process.stdout.write(JSON.stringify(out));"""
    got, err = node_run(["stWire"], body, data)
    check(got is not None, "stWire's change handler runs in node with the switch line", err)
    for (name, (s_on, r_on)), res in zip(cases.items(), got or []):
        want = "kept" if r_on else EN["st_switch_d"].replace("{s}", "45")
        check(res == {"line": want, "sent": [45]}, f"{name}: X 45 saved, the switch line "
              + ("left with Rapid's words" if r_on else "says the classic variant's 45 s"), res)


def test_pairs_kept():
    """The short-term and Rapid pairs are kept apart (run in node): a pick of one block, still to go out or on its
    way, never marks the other's pairs picked; saveRapid marks them picked only for a pick; and a save still to go
    out that did not pick its pairs gets the server's (sendSettings), in both blocks, while a pick keeps its own. On a
    block's banner (its note pairs_gone, no pairs left) a save never marks an empty list picked (every pair) unless a
    pick is on its way, each block reading its own note (saveShort, saveRapid); after an answer each card's pair
    buttons show its own block's pairs (stPicksShow, rpPicksShow)."""
    if not NODE:
        print("note: node is not installed: the pair checks are skipped")
        return
    st, rp = SC.clamp_short(None, dict(SC.DEFAULTS)), R.clamp_rapid(None)
    settings = {"short_term": st, "rapid": rp, "symbols": [], "short_term_note": None, "rapid_note": None}
    server = {**settings, "short_term": {**st, "symbols": ["GBPUSD"]}, "rapid": {**rp, "symbols": ["USDJPY"]}}
    pending = [   # edits made while the PUT was on its way: not picked (the page's served pairs), then picked here
        {"short_term": {**st, "symbols": ["EURUSD"], "symbols_picked": False},
         "rapid": {**rp, "symbols": ["EURUSD"], "symbols_picked": False}},
        {"short_term": {**st, "symbols": ["AUDUSD"], "symbols_picked": True},
         "rapid": {**rp, "symbols": ["AUDUSD"], "symbols_picked": True}}]
    emptied = lambda key: {key: {"symbols": [], "symbols_picked": True}}      # noqa: E731
    way = lambda key: {key: {"symbols_picked": True}}                         # noqa: E731
    st_gone, rp_gone = {"short_term_note": "pairs_gone"}, {"rapid_note": "pairs_gone"}
    gone = {   # name: (block, its banner notes, a save still to go out, one on its way, the edit, marked picked)
        "short-term on its banner, a pick still to go out emptied, the risk changed":
            ("short_term", st_gone, emptied("short_term"), {}, {"risk_min": 1}, False),
        "Rapid on its banner, a pick still to go out emptied, the orders changed":
            ("rapid", rp_gone, emptied("rapid"), {}, {"orders": 12}, False),
        "short-term on its banner, its last pair taken off": ("short_term", st_gone, {}, {}, {"symbols": []}, False),
        "Rapid on its banner, its last pair taken off": ("rapid", rp_gone, {}, {}, {"symbols": []}, False),
        "short-term on its banner, a pick on its way": ("short_term", st_gone, {}, way("short_term"), {"risk_min": 1},
                                                        True),
        "Rapid on its banner, a pick on its way": ("rapid", rp_gone, {}, way("rapid"), {"orders": 12}, True),
        "Rapid, only short-term's banner up, a pick still to go out emptied":
            ("rapid", st_gone, emptied("rapid"), {}, {"orders": 12}, True),
        "short-term, only Rapid's banner up, a pick still to go out emptied":
            ("short_term", rp_gone, emptied("short_term"), {}, {"risk_min": 1}, True),
    }
    body = SAVES + """let boxes = {}, btns = {};
function $(sel) { return boxes[sel] || null; }
function $$(sel) { return btns[sel] || []; }
function toast() {}
function errText(e) { throw e; }   // sendSettings' catch must not swallow an error (node_run adds what it lacks)
let call = 0;
async function api() {   // the PUT: the edits made while it was on its way (none after the pending ones), then the server's answer
  savePatch = JSON.parse(JSON.stringify(input.pending[call++] || {}));
  return { settings: JSON.parse(JSON.stringify(input.server)) };
}
const out = {};
S.settings = JSON.parse(JSON.stringify(input.settings));
const picked = (key, pend, way) => { savePatch = pend; saveSent = way; return pairsPicked(key, {}, ["EURUSD"]); };
const yes = { symbols_picked: true };
out.picked = [picked("rapid", { short_term: yes }, {}), picked("short_term", { rapid: yes }, {}),
  picked("rapid", {}, { short_term: yes }), picked("short_term", {}, { rapid: yes }),
  picked("rapid", { rapid: yes }, {}), picked("short_term", {}, { short_term: yes })];
savePatch = {}; saveSent = {};
saveRapid({ orders: 12 }); saveRapid({ symbols: ["EURUSD"] });
out.saved = sent.map((p) => ("symbols" in p.rapid ? p.rapid.symbols_picked : "no pairs"));
// a block's banner up (its note), no pairs left: what the save marks
out.gone = input.gone.map(([key, notes, pend, way, patch]) => {
  S.settings = { ...JSON.parse(JSON.stringify(input.settings)), ...notes }; S.settings[key].symbols = [];
  savePatch = JSON.parse(JSON.stringify(pend)); saveSent = way; sent.length = 0;
  (key === "rapid" ? saveRapid : saveShort)(patch);
  return sent[0][key].symbols_picked;
});
savePatch = {}; saveSent = {};
(async () => {
  out.after = [];
  for (const _ of input.pending) {
    S.settings = JSON.parse(JSON.stringify(input.settings)); savePatch = { min_confidence: 70 };
    await sendSettings();
    out.after.push([savePatch.short_term.symbols, savePatch.rapid.symbols]);
  }
  // an answer with other pairs for each block: the cards are left, their pair buttons follow (every pair a button)
  const mk = (ds) => input.pairs.map((v) => { const b = { on: null, dataset: { [ds]: v } };
    b.classList = { toggle: (k, on) => { if (k === "on") b.on = on; } }; return b; });
  btns = { "[data-st-sym]": mk("stSym"), "[data-rp-sym]": mk("rpSym") };
  boxes = { "#stCard": { innerHTML: "kept" }, "#rpCard": { innerHTML: "kept", classList: { toggle() {} } } };
  S.settings = JSON.parse(JSON.stringify(input.settings)); savePatch = { min_confidence: 70 };
  await sendSettings();
  out.buttons = { st: btns["[data-st-sym]"].map((b) => [b.dataset.stSym, b.on]), rp: btns["[data-rp-sym]"].map((b) => [b.dataset.rpSym, b.on]),
    cards: [boxes["#stCard"].innerHTML, boxes["#rpCard"].innerHTML] };
  process.stdout.write(JSON.stringify(out));
})();"""
    pairs = ["EURUSD", "GBPUSD", "USDJPY", "AUDUSD"]
    got, err = node_run(["pairsPicked", "saveRapid", "saveShort", "sendSettings"], body,
                        {"settings": settings, "server": server, "pending": pending, "pairs": pairs,
                         "gone": [list(v[:5]) for v in gone.values()]})
    check(got is not None, "the pair handlers run in node", err)
    if got is None:
        return
    check(got["picked"] == [False, False, False, False, True, True], "a pick of one block (to go out, or on its way) "
          "never marks the other's pairs picked", got["picked"])
    check(got["saved"] == ["no pairs", True], "saveRapid marks its pairs picked only when the edit picks them; an "
          "edit of another field with no pick anywhere sends no pairs (v1.3.1: the server keeps them)", got["saved"])
    for (name, (*_, want)), res in zip(gone.items(), got["gone"]):
        check(res is want, f"{name}: the save marks its pairs {'picked' if want else 'not picked'}", res)
    check(got["after"] == [[["GBPUSD"], ["USDJPY"]], [["AUDUSD"], ["AUDUSD"]]], "a save still to go out shows the "
          "server's pairs unless it picked its own (short-term and Rapid)", got["after"])
    b = got["buttons"]
    check(b["st"] == [[p, p == "GBPUSD"] for p in pairs] and b["rp"] == [[p, p == "USDJPY"] for p in pairs]
          and b["cards"] == ["kept", "kept"], "after an answer the short-term card's pair buttons show its pairs "
          "(GBPUSD), Rapid's its own (USDJPY); the cards left as they are", b)


def test_rapid_raise():
    """A Rapid maximum typed under its minimum is raised to it, never swapped, as bot/rapid.py clamp_rapid does
    (rpWire's real handler, in node): what goes out is what the server keeps, and the field shows it."""
    if not NODE:
        print("note: node is not installed: the Rapid range checks are skipped")
        return
    served = R.clamp_rapid({"risk_min": 0.2, "risk_max": 0.5, "tp_min_usd": 10, "tp_max_usd": 15})
    cases = {   # name: (field, value typed, its step, min, max, the maximum it may raise, that maximum then)
        "risk 0.8 over a 0.5 maximum": ("risk_min", "0.8", "0.05", "0.05", "5", "risk_max", 0.8),
        "take-profit 20 over a 15 maximum": ("tp_min_usd", "20", "0.1", "1", "10000", "tp_max_usd", 20),
        "risk 0.3 under the maximum": ("risk_min", "0.3", "0.05", "0.05", "5", "risk_max", 0.5),
    }
    data = [{"settings": {"rapid": served, "symbols": []}, "key": k, "value": v, "step": s, "min": a, "max": z,
             "hi": hi} for k, v, s, a, z, hi, _ in cases.values()]
    body = SAVES + """let boxes = {};
function $(sel) { return boxes[sel] || null; }
const out = [];
for (const c of input) {
  S.settings = JSON.parse(JSON.stringify(c.settings)); sent.length = 0;
  boxes = { [`[data-rp=${c.hi}]`]: { value: String(c.settings.rapid[c.hi]) } };
  const h = {}, card = { addEventListener: (k, fn) => { h[k] = fn; } };
  rpWire(card);
  h.change({ target: { dataset: { rp: c.key }, value: c.value, step: c.step, min: c.min, max: c.max,
    validity: { badInput: false }, matches: () => false } });
  out.push({ sent: sent.map((p) => p.rapid), shown: boxes[`[data-rp=${c.hi}]`].value });
}
process.stdout.write(JSON.stringify(out));"""
    got, err = node_run(["rpWire"], body, data)
    check(got is not None, "rpWire's change handler runs in node", err)
    for (name, (key, _, _, _, _, hi, top)), res in zip(cases.items(), got or []):
        if len(res["sent"]) != 1:
            check(False, f"{name}: one save of the rapid block", res["sent"])
            continue
        sent = {k: v for k, v in res["sent"][0].items() if k != "symbols_picked"}
        kept = R.clamp_rapid(sent, served)
        out = sent.get(hi, served[hi])          # v1.3.1: a maximum not raised is not sent, the server keeps its own
        check(out == top and kept[hi] == top and kept[key] == sent[key] and float(res["shown"]) == top
              and (hi in sent) == (top != served[hi]),
              f"{name}: {hi} {'sent' if top != served[hi] else 'kept'} as {top}, as the server keeps it, and shown",
              (sent.get(hi), kept[hi], res["shown"]))


def test_unit_switch():
    """A unit picked on a card starts its amounts from that unit's defaults (burstWire's and rpWire's real click
    handlers, in node): the save sends the whole block, old amounts included, so the server's own reset (a unit
    changed without its amount) never fires and the page's is the only one; what goes out is what the server keeps,
    never the old unit's amount clamped to the new range (100 $ read as 10 %), the other keys as served."""
    if not NODE:
        print("note: node is not installed: the unit switch checks are skipped")
        return
    v120 = {"enabled": True, "trades": 12, "lead_seconds": 60, "offset_pips": 7.0, "sl_pips": 10.0,
            "tp_start_pips": 4.0, "tp_step_pips": 1.5, "hold_seconds": 30, "expire_seconds": 90, "currencies": ["USD"]}
    usd = B.legacy_burst({**v120, "sl_unit": "usd", "sl_money": 100.0})
    pct = B.legacy_burst({**v120, "sl_unit": "pct", "sl_money": 5.0})
    # a lot per order off the default (0.1): a stop unit picked that reset it would show in what the server keeps
    rp_usd = R.clamp_rapid({"risk_unit": "usd", "risk_min": 10, "risk_max": 10, "loss_unit": "usd", "daily_loss": 50,
                            "lots": 0.5})
    rp_pct = R.clamp_rapid({"risk_min": 0.5, "risk_max": 2, "daily_loss": 10})
    money = lambda u: {"sl_unit": u, "sl_money": B.SL_MONEY.get(u, B.SL_MONEY["pct"])[2]}   # pips reads pct's
    risk = lambda u: {"risk_unit": u, **{k: R.UNIT_DEFAULTS[u][k] for k in ("risk_min", "risk_max")}}
    loss = lambda u: {"loss_unit": u, "daily_loss": R.UNIT_DEFAULTS[u]["daily_loss"]}
    cases = {   # name: (card, served block, the unit's key, the unit picked, what is sent and kept; None: no save)
        "burst: $ 100 to %": ("burst", usd, "sl_unit", "pct", money("pct")),
        "burst: % 5 to $": ("burst", pct, "sl_unit", "usd", money("usd")),
        "burst: $ 100 to pips": ("burst", usd, "sl_unit", "pips", money("pips")),
        "burst: % picked again": ("burst", pct, "sl_unit", "pct", None),
        "Rapid: risk $ 10 to %": ("rp", rp_usd, "risk_unit", "pct", risk("pct")),
        "Rapid: daily loss $ 50 to %": ("rp", rp_usd, "loss_unit", "pct", loss("pct")),
        "Rapid: risk % to $": ("rp", rp_pct, "risk_unit", "usd", risk("usd")),
        "Rapid: daily loss % to $": ("rp", rp_pct, "loss_unit", "usd", loss("usd")),
        "Rapid: stop in pips to money": ("rp", rp_usd, "sl_unit", "money", {"sl_unit": "money"}),
        "Rapid: risk $ picked again": ("rp", rp_usd, "risk_unit", "usd", None),
    }
    block = {"burst": "burst", "rp": "rapid"}
    data = [{"card": c, "settings": {block[c]: served, "symbols": []}, "key": k, "v": u}
            for c, served, k, u, _ in cases.values()]
    body = SAVES + """function $() { return null; }
const out = [];
for (const c of input) {
  S.settings = JSON.parse(JSON.stringify(c.settings)); sent.length = 0;
  const h = {}, card = { innerHTML: "kept", addEventListener: (k, fn) => { h[k] = fn; } };
  ({ burst: burstWire, rp: rpWire })[c.card](card);
  const sel = c.card === "burst" ? "[data-burst-unit] button" : "[data-rp-seg] button";
  const btn = { dataset: { v: c.v }, parentElement: { dataset: { rpSeg: c.key } } };
  h.click({ target: { closest: (s) => (s === sel ? btn : null) } });
  out.push({ sent: sent.slice(), drawn: card.innerHTML !== "kept" });
}
process.stdout.write(JSON.stringify(out));"""
    got, err = node_run(["burstWire", "rpWire"], body, data)
    check(got is not None, "the unit buttons click in node", err)
    for (name, (card, served, key, u, want)), res in zip(cases.items(), got or []):
        if want is None:
            check(res["sent"] == [] and not res["drawn"], f"{name}: nothing saved, the card left as it is", res)
            continue
        blocks = [s[block[card]] for s in res["sent"] if block[card] in s]
        if len(blocks) != 1:
            check(False, f"{name}: one save of the {block[card]} block", res["sent"])
            continue
        sent = {k: v for k, v in blocks[0].items() if k != "symbols_picked"}   # main.py drops the page's marker
        if card == "burst":     # main.py put_my_settings
            kept = B.raise_max_hold({**served, **B.clamp_burst(sent, 40, served)})
        else:
            kept = R.clamp_rapid(sent, served)
        moved = {k: kept.get(k) for k, v in served.items() if k not in want and kept.get(k) != v}
        check({k: sent.get(k) for k in want} == want and {k: kept.get(k) for k in want} == want and not moved
              and res["drawn"], f"{name}: sends {want}, the server keeps it, the rest as served; the card drawn again",
              ({k: sent.get(k) for k in want}, {k: kept.get(k) for k in want}, moved))


def test_burst_money():
    """The burst's money preview follows a stop amount typed in at once, and only that field (burstWire's real change
    handler, in node, the preview's box stood in; a $ stop, connector 1.3.0 with a balance of 10,000)."""
    if not NODE:
        print("note: node is not installed: the burst money checks are skipped")
        return
    served = B.legacy_burst({**V120_BURST, "sl_unit": "usd", "sl_money": 100.0})
    lo, hi, step = const("BURST_RANGE")["sl_money"]["usd"]
    cases = {"the $ stop 150 typed in": ("sl_money", "150"), "the trades changed": ("trades", "15")}
    data = {"settings": {"symbols": [], "burst": served}, "conn": {"online": True, "version": "1.3.0", "account": ACCT},
            "cases": [[k, v, lo if k == "sl_money" else 1, hi if k == "sl_money" else 40,
                       step if k == "sl_money" else 1] for k, v in cases.values()]}
    body = SAVES + """let boxes = {};
function $(sel) { return boxes[sel] || null; }
const out = [];
for (const [key, v, a, z, s] of input.cases) {
  S.settings = JSON.parse(JSON.stringify(input.settings)); S.connector = input.conn; sent.length = 0;
  const m = { innerHTML: "kept" };
  boxes = { "#burstMoney": m };
  const h = {}, card = { addEventListener: (k, fn) => { h[k] = fn; } };
  burstWire(card);
  h.change({ target: { dataset: { burst: key }, value: v, min: String(a), max: String(z), step: String(s),
    validity: { badInput: false }, matches: () => false } });
  out.push({ money: m.innerHTML, sent: sent.map((p) => p.burst[key]) });
}
process.stdout.write(JSON.stringify(out));"""
    got, err = node_run(["burstWire"], body, data)
    check(got is not None, "burstWire's change handler runs in node with the money preview", err)
    for (name, (key, v)), res in zip(cases.items(), got or []):
        if key == "sl_money":
            ok = "≈ $150.00" in res["money"] and EN["burst_lots_d"] in res["money"] and res["sent"] == [150]
        else:
            ok = res == {"money": "kept", "sent": [15]}
        check(ok, f"{name}: saved, the preview " + ("shows the whole burst at ≈ $150.00" if key == "sl_money"
                                                   else "left as it is"), res)


def test_switches():
    """The analysis and trigger switches save what they show (burstWire's and rpWire's real change handlers, in node):
    unticked, the burst's analysis, Rapid's and each Rapid trigger go out off, the other trigger as served; ticked
    again, on; the server keeps each."""
    if not NODE:
        print("note: node is not installed: the switch checks are skipped")
        return
    v120 = {"enabled": True, "trades": 12, "lead_seconds": 60, "offset_pips": 7.0, "sl_pips": 10.0,
            "tp_start_pips": 4.0, "tp_step_pips": 1.5, "hold_seconds": 30, "expire_seconds": 90, "currencies": ["USD"]}
    burst = B.legacy_burst({**v120, "analysis": True})
    rp = R.clamp_rapid({"analysis": True, "triggers": {"signals": True, "news": True}})
    rp_news_off = R.clamp_rapid({"triggers": {"signals": True, "news": False}})
    cases = {   # name: (card, served block, the trigger (None: the analysis switch), ticked / unticked in turn)
        "burst analysis off, then on": ("burst", burst, None, [False, True]),
        "Rapid analysis off, then on": ("rp", rp, None, [False, True]),
        "Rapid news trigger off": ("rp", rp, "news", [False]),
        "Rapid signals trigger off": ("rp", rp, "signals", [False]),
        "Rapid news trigger on, then off (signals served on)": ("rp", rp_news_off, "news", [True, False]),
        "Rapid signals trigger off, then on (news served off)": ("rp", rp_news_off, "signals", [False, True]),
    }
    block = {"burst": "burst", "rp": "rapid"}
    data = [{"card": c, "settings": {block[c]: served, "symbols": []}, "trig": trig, "steps": steps}
            for c, served, trig, steps in cases.values()]
    body = SAVES + """function $() { return null; }
const out = [];
for (const c of input) {
  S.settings = JSON.parse(JSON.stringify(c.settings)); sent.length = 0;
  const h = {}, card = { addEventListener: (k, fn) => { h[k] = fn; } };
  ({ burst: burstWire, rp: rpWire })[c.card](card);
  const sel = c.card === "burst" ? "[data-burst-an]" : "[data-rp-an]";
  const dataset = c.trig == null ? {} : { rpTrig: c.trig };
  for (const on of c.steps) h.change({ target: { matches: (s) => c.trig == null && s === sel, checked: on, dataset } });
  out.push(sent.map((p) => p.burst || p.rapid));
}
process.stdout.write(JSON.stringify(out));"""
    got, err = node_run(["burstWire", "rpWire"], body, data)
    check(got is not None, "the switches' change handlers run in node", err)
    for (name, (card, served, trig, steps)), saves in zip(cases.items(), got or []):
        cur, want, ok, seen = served, dict(served.get("triggers") or {}), len(saves) == len(steps), []
        for on, save in zip(steps, saves):
            sent = {k: v for k, v in save.items() if k != "symbols_picked"}
            if card == "burst":     # main.py put_my_settings
                kept = B.raise_max_hold({**cur, **B.clamp_burst(sent, 40, cur)})
            else:
                kept = R.clamp_rapid(sent, cur)
            if trig is None:
                key, value = "analysis", on
            else:
                want[trig] = on
                key, value = "triggers", dict(want)
            seen.append((sent.get(key), kept.get(key)))
            ok = ok and sent.get(key) == value and kept.get(key) == value
            cur = kept
        check(ok, f"{name}: each save sends what the switch shows, as the server keeps it", seen)


def test_burst_picks_gone():
    """Burst pairs picked that are no longer offered (out of the user's pairs, or the engine's) stay on the card,
    picked and marked, so they can be taken off; the banner says the burst trades nothing when they are all such, as
    the server plans it (plan_for). A user with no pairs of their own (every pair) is offered each burst pair the
    engine trades. burstSetHtml and burstWire's real click handler run in node."""
    if not NODE:
        print("note: node is not installed: the burst pair checks are skipped")
        return
    every = list(B.BURST_SYMBOLS)
    mine = ["EURUSD", "GBPUSD"]
    cands = B.candidates([{"time": "2026-11-06T09:30:00", "currency": "GBP", "title": "GDP m/m", "impact": "High",
                           "id": 1}], {"symbols": every})
    plan = {tuple(p): [c["symbol"] for c in B.plan_for({"symbols": mine, "burst": {"symbols": list(p)}}, cands, every)]
            for p in ((), ("GBPJPY",), ("GBPUSD", "GBPJPY"))}
    check(plan == {(): ["GBPUSD"], ("GBPJPY",): [], ("GBPUSD", "GBPJPY"): ["GBPUSD"]},
          "server: with only picks the user does not trade, a release plans no burst", plan)
    served = B.legacy_burst({})
    draws = {"one pick, not traded": ["GBPJPY"], "a traded pick and one not": ["EURUSD", "GBPJPY"],
             "traded picks only": ["EURUSD"], "no picks": []}
    clicks = {   # name: (picks, the pair clicked)
        "the pick not traded taken off": (["GBPJPY"], "GBPJPY"),
        "a traded pair picked, none gone": (["EURUSD"], "GBPUSD"),
        "a traded pair picked next to one gone": (["GBPJPY"], "EURUSD"),
    }
    body = SAVES + """function $() { return null; }
const out = { drawn: [], clicked: [] };
S.symbols = input.every;
for (const p of input.draws) {
  S.settings = { symbols: input.mine, burst: { ...input.served, symbols: p } };
  out.drawn.push(burstSetHtml());
}
for (const [p, x] of input.clicks) {
  S.settings = { symbols: input.mine, burst: { ...input.served, symbols: p } }; sent.length = 0;
  const h = {}, toggled = [], card = { innerHTML: "kept", addEventListener: (k, fn) => { h[k] = fn; } };
  burstWire(card);
  const sym = { dataset: { burstSym: x }, classList: { toggle: (c, on) => toggled.push([c, on]) } };
  h.click({ target: { closest: (sel) => (sel === "[data-burst-sym]" ? sym : null) } });
  out.clicked.push({ saved: sent.map((s) => s.burst.symbols), card: card.innerHTML, toggled });
}
// no pairs of the user's own (settings.symbols empty: every pair): each burst pair the engine trades is offered
S.symbols = input.offered;
S.settings = { symbols: [], burst: { ...input.served, symbols: [] } }; sent.length = 0;
out.all = burstSetHtml();
{
  const h = {}, card = { innerHTML: "kept", addEventListener: (k, fn) => { h[k] = fn; } };
  burstWire(card);
  const sym = { dataset: { burstSym: input.allPick }, classList: { toggle: () => {} } };
  h.click({ target: { closest: (sel) => (sel === "[data-burst-sym]" ? sym : null) } });
  out.allSaved = sent.map((s) => s.burst.symbols);
}
process.stdout.write(JSON.stringify(out));"""
    offered = [p for p in every if p != "NZDUSD"]               # a pair the engine does not trade is not offered
    data = {"draws": list(draws.values()), "clicks": list(clicks.values()), "mine": mine, "served": served,
            "every": every, "offered": offered, "allPick": "GBPJPY"}
    got, err = node_run(["burstSetHtml", "burstWire"], body, data)
    check(got is not None, "the burst card draws and its pair buttons click in node", err)
    if got is None:
        return

    def buttons(html):
        return re.findall(r'<button class="([^"]*)" data-burst-sym="(\w+)"', html)
    want = [("", p) for p in B.BURST_SYMBOLS if p in offered]
    check(buttons(got["all"]) == want and got["allSaved"] == [["GBPJPY"]] and EN["burst_pairs_gone"] not in got["all"],
          "no pairs picked by the user (every pair): every burst pair the engine trades is offered, and a click "
          "picks it", (buttons(got["all"]), got["allSaved"]))
    for (name, picks), html in zip(draws.items(), got["drawn"]):
        gone = [p for p in picks if p not in mine]
        want = [("on" if p in picks else "", p) for p in B.BURST_SYMBOLS if p in mine] + [("on gone", p) for p in gone]
        banner = EN["burst_pairs_gone"] in html
        check(buttons(html) == want and banner == (bool(picks) and gone == picks) and (not gone or EN["burst_pair_gone"]
              in html), f"{name}: the pairs offered, then the picks not traded (marked); the banner only when "
              "every pick is such", (buttons(html), banner))
    for (name, (picks, x)), res in zip(clicks.items(), got["clicked"]):
        on = x not in picks
        saved = [*picks, x] if on else [p for p in picks if p != x]
        redrawn = any(p not in mine for p in picks)
        ok = res["saved"] == [saved] and (res["card"] != "kept") == redrawn and res["toggled"] == ([] if redrawn
                                                                                                  else [["on", on]])
        if ok and redrawn:
            gone = [p for p in saved if p not in mine]
            ok = [b for b in buttons(res["card"]) if "gone" in b[0]] == [("on gone", p) for p in gone] \
                and (EN["burst_pairs_gone"] in res["card"]) == (bool(saved) and gone == saved)
        check(ok, f"{name}: saves {saved}, " + ("draws the card again" if redrawn else "marks the button only"), res)


def test_settings_redraw():
    """On the Settings page a connector that comes online, goes offline, changes version or gets its account redraws
    the short-term, Rapid and burst cards (their 1.3.0 chips and banners, the money previews), a push of the same
    state does not, and a card being typed in (a text field, not a switch) waits for a push after the typing
    (connRedraw and its real card builders, in node)."""
    if not NODE:
        print("note: node is not installed: the settings redraw checks are skipped")
        return
    acct = {"balance": 10000, "currency": "USD", "demo": True}
    settings = {"symbols": [], "short_term": SC.clamp_short(None, dict(SC.DEFAULTS)),
                "rapid": R.clamp_rapid({"enabled": True}),
                "burst": B.legacy_burst({"enabled": True, "sl_unit": "pct", "sl_money": 2.0})}
    data = {"S": {"settings": settings, "symbols": ["EURUSD", "GBPUSD", "XAUUSD"], "connector": {"online": False}},
            "old": {"online": True, "version": "1.2.0", "account": acct},
            "new": {"online": True, "version": "1.3.0", "account": acct},
            "bare": {"online": True, "version": "1.3.0"}}     # online, its account not known yet
    body = """const cards = {};
for (const id of ["#stCard", "#rpCard", "#burstSetCard"]) cards[id] = { innerHTML: "", contains: (el) => !!el && el.card === id };
function $(sel) { return cards[sel] || null; }
const document = { activeElement: null };
function pageTrades() {} function pageConnector() {} function pageDashboard() {} function pageSignals() {}
function pageSettings() {}
const out = [];
const snap = () => out.push(Object.fromEntries(Object.entries(cards).map(([k, c]) => [k, c.innerHTML])));
const keep = () => { for (const c of Object.values(cards)) c.innerHTML = "kept"; };
const push = (state) => { const prev = S.connector; S.connector = JSON.parse(JSON.stringify(state)); connRedraw(prev); snap(); };
Object.assign(S, input.S); S.page = pageSettings;
cards["#stCard"].innerHTML = stCardHtml(); cards["#rpCard"].innerHTML = rpCardHtml();
cards["#burstSetCard"].innerHTML = burstSetHtml();
snap();
keep(); push(input.old);
keep(); push(input.old);
keep(); push(input.new);
keep(); document.activeElement = { card: "#rpCard", matches: () => true };
push(input.old);
push(input.old);
document.activeElement = { card: "#rpCard", matches: () => false };
push(input.old);
// a switch (a checkbox) focused holds nothing back: what the focused element is asked is recorded, and answered as a
// checkbox would (it matches a plain "input", never "input:not([type=checkbox])")
const asked = [];
document.activeElement = { card: "#rpCard", matches: (sel) => { asked.push(sel); return sel.split(",").some((p) => p.trim() === "input"); } };
keep(); push(input.bare);
document.activeElement = null;
keep(); push(input.new);
out.push(asked);
process.stdout.write(JSON.stringify(out));"""
    got, err = node_run(["connRedraw", "settingsConnRedraw", "stCardHtml", "rpCardHtml", "burstSetHtml"], body, data)
    check(got is not None, "connRedraw runs in node on the Settings page", err)
    if got is None:
        return
    chip, banner = f'<span class="chip warn">{EN["need_130"]}</span>', EN["burst_conn_130"]

    def marks(snap):   # each card's 1.3.0 mark: the chip on the short-term and Rapid cards, the burst's banner
        return {"#stCard": chip in snap["#stCard"], "#rpCard": chip in snap["#rpCard"],
                "#burstSetCard": banner in snap["#burstSetCard"]}

    def kept(snap):    # the cards not drawn again since keep()
        return sorted(k for k, v in snap.items() if v == "kept")
    all3, none3 = dict.fromkeys(marks(got[0]), True), dict.fromkeys(marks(got[0]), False)
    check(marks(got[0]) == none3, "drawn with the connector offline: no 1.3.0 marks", marks(got[0]))
    check(not kept(got[1]) and marks(got[1]) == all3, "online at 1.2.0: the cards are drawn again with their marks",
          (kept(got[1]), marks(got[1])))
    check(kept(got[2]) == ["#burstSetCard", "#rpCard", "#stCard"], "the same state pushed again: no card drawn again",
          kept(got[2]))
    check(not kept(got[3]) and marks(got[3]) == none3, "1.2.0 to 1.3.0: drawn again, the marks gone",
          (kept(got[3]), marks(got[3])))
    check(kept(got[4]) == ["#rpCard"] and marks(got[4])["#stCard"] and marks(got[4])["#burstSetCard"],
          "back to 1.2.0 while the Rapid card is typed in: that one is left, the others drawn", kept(got[4]))
    check(kept(got[5]) == ["#rpCard"], "a push while still typed in: still left", kept(got[5]))
    check(not kept(got[6]) and marks(got[6]) == all3, "the next push after the typing draws it", kept(got[6]))
    check(got[9] == ["input:not([type=checkbox]), select, textarea"], "a card is held back only for a focused text "
          "field, select or text area, never a switch", got[9])
    off = EN["burst_money_off"].replace("{v}", "2.0")

    def money(snap):   # the burst card's money preview
        return re.findall(r"≈[^<]*", snap["#burstSetCard"])
    check(not kept(got[7]) and off in got[7]["#burstSetCard"], "1.3.0 online, its account not known yet, a switch "
          "focused: drawn again, the burst's money as a % of the balance", (kept(got[7]), money(got[7])))
    check(not kept(got[8]) and "≈ $200.00" in got[8]["#burstSetCard"] and off not in got[8]["#burstSetCard"],
          "the account arrives (same version): drawn again, the money for its balance (2% of 10,000)",
          (kept(got[8]), money(got[8])))


V120_BURST = {"enabled": True, "trades": 12, "lead_seconds": 60, "offset_pips": 7.0, "sl_pips": 10.0,   # as saved
              "tp_start_pips": 4.0, "tp_step_pips": 1.5, "hold_seconds": 30, "expire_seconds": 90,     # by v1.2.0
              "currencies": ["USD"]}
ACCT = {"balance": 10000, "currency": "USD", "demo": True}


def burst_buttons(html: str) -> list:
    """The burst card's pair buttons: (class, pair), in the card's order."""
    return re.findall(r'<button class="([^"]*)" data-burst-sym="(\w+)"', html)


def test_burst_banner():
    """The burst card's "an older connector skips it" banner follows each edit at once (burstWire's real handlers, in
    node, the banner's box stood in), on the Dashboard card's rule: on for an older connector when the burst's options
    need 1.3.0 (the analysis, a Y over X, a new pair picked) or Rapid is on, off again once they are undone, never for
    connector 1.3.0. The rest of the card is left as it is, and the server's rule (needs_version, new_pair, Rapid's
    runs) says the same of what it keeps."""
    if not NODE:
        print("note: node is not installed: the burst banner checks are skipped")
        return
    old = B.legacy_burst(V120_BURST)
    y60 = B.legacy_burst({**V120_BURST, "max_hold_seconds": 60})
    cases = {   # name: (served block, Rapid on, connector version, what is done, the banner before and after)
        "the analysis switched on": (old, False, "1.2.0", ["an", True], (False, True)),
        "the analysis switched off": ({**old, "analysis": True}, False, "1.2.0", ["an", False], (True, False)),
        "Y 60 typed over an X of 30": (old, False, "1.2.0", ["field", "max_hold_seconds", "60"], (False, True)),
        "Y cleared": (y60, False, "1.2.0", ["field", "max_hold_seconds", ""], (True, False)),
        "XAUUSD picked": (old, False, "1.2.0", ["sym", "XAUUSD"], (False, True)),
        "XAUUSD taken off": ({**old, "symbols": ["XAUUSD"]}, False, "1.2.0", ["sym", "XAUUSD"], (True, False)),
        "Rapid on, the trades changed": (old, True, "1.2.0", ["field", "trades", "15"], (True, True)),
        "connector 1.3.0, the analysis switched on": (old, False, "1.3.0", ["an", True], (False, False)),
    }
    data = {"every": list(B.BURST_SYMBOLS),
            "cases": [{"settings": {"symbols": [], "burst": served, "rapid": R.clamp_rapid({"enabled": rp_on})},
                       "conn": {"online": True, "version": ver, "account": ACCT}, "do": do}
                      for served, rp_on, ver, do, _ in cases.values()]}
    body = SAVES + """let boxes = {};
function $(sel) { return boxes[sel] || null; }
const out = [];
S.symbols = input.every;
for (const c of input.cases) {
  S.settings = JSON.parse(JSON.stringify(c.settings)); S.connector = c.conn; sent.length = 0;
  const conn = { innerHTML: burstConnHtml(burstSet()) }, before = conn.innerHTML;
  boxes = { "#burstConn": conn };
  const drawn = burstSetHtml().includes(`<div id="burstConn">${before}</div>`);   // the card draws its banner there
  const h = {}, card = { innerHTML: "kept", addEventListener: (k, fn) => { h[k] = fn; } };
  burstWire(card);
  const [kind, key, v] = c.do;
  if (kind === "an") h.change({ target: { matches: (s) => s === "[data-burst-an]", checked: key, dataset: {} } });
  else if (kind === "field") h.change({ target: { dataset: { burst: key }, value: v, min: "1", max: "1800", step: "1",
    validity: { badInput: false }, matches: () => false } });
  else h.click({ target: { closest: (s) => (s === "[data-burst-sym]" ? { dataset: { burstSym: key }, classList: { toggle() {} } } : null) } });
  out.push({ before, after: conn.innerHTML, drawn, card: card.innerHTML, sent: sent.map((p) => p.burst) });
}
process.stdout.write(JSON.stringify(out));"""
    got, err = node_run(["burstConnHtml", "burstSetHtml", "burstWire"], body, data)
    check(got is not None, "the burst card's handlers run in node with its banner", err)
    banner = EN["burst_conn_130"]

    def rule(b, older, rapid):   # server/burst.py: a run held back from a connector before 1.3.0
        p = B.params_for({"burst": b, "risk_per_trade_pct": 1.0, "max_spread_pips": 3.0}, 40)
        return older and (B.needs_version(p) == B.V130 or any(B.new_pair(s) for s in b.get("symbols") or []) or rapid)
    word = lambda on: "on" if on else "off"
    for (name, (served, rp_on, ver, do, want)), res in zip(cases.items(), got or []):
        if len(res["sent"]) != 1:
            check(False, f"{name}: one save of the burst block", res["sent"])
            continue
        kept = B.raise_max_hold({**served, **B.clamp_burst(res["sent"][0], 40, served)})   # main.py put_my_settings
        older = ver != "1.3.0"
        shown = (banner in res["before"], banner in res["after"])
        server = (rule(served, older, rp_on), rule(kept, older, rp_on))
        check(shown == want and server == want and res["drawn"] and res["card"] == "kept",
              f"{name}: the banner {word(want[0])}, then {word(want[1])} at once, as the server rules; the rest of "
              "the card left as it is", (shown, server, res["drawn"], res["card"][:40]))


def test_send_redraw():
    """A save answered (sendSettings, in node: its PUT, $ and the cards stood in) draws the short-term card again only
    when the variant or its switch changed, shows Rapid's card for Rapid only, draws Rapid's card again only when its
    note changed (its pairs gone, or back: the banner follows), and redraws the burst's banner, which counts Rapid on
    (connector 1.2.0, a v1.2.0 burst)."""
    if not NODE:
        print("note: node is not installed: the save redraw checks are skipped")
        return
    st, rp = SC.clamp_short(None, dict(SC.DEFAULTS)), R.clamp_rapid(None)
    cases = {   # name: (short-term on, Rapid on, then as the server answers, the variant picked on the page, Rapid's
        #          note before and in the answer)
        "classic to Rapid": (True, False, False, True, None, None, None),
        "Rapid to classic": (False, True, True, False, None, None, None),
        "classic switched off": (True, False, False, False, None, None, None),
        "Rapid switched off (its card shown)": (False, True, False, False, "rapid", None, None),
        "Rapid kept on": (False, True, False, True, None, None, None),
        "classic kept on": (True, False, True, False, None, None, None),
        "Rapid on, its pairs gone in the answer": (False, True, False, True, None, None, "pairs_gone"),
        "Rapid on, its pairs back in the answer": (False, True, False, True, None, "pairs_gone", None),
        "Rapid on, its pairs still gone": (False, True, False, True, None, "pairs_gone", "pairs_gone"),
    }

    def settings(s_on, r_on, note):
        return {"symbols": [], "short_term": {**st, "enabled": s_on}, "rapid": {**rp, "enabled": r_on},
                "burst": B.legacy_burst(V120_BURST), "short_term_note": None, "rapid_note": note}
    data = {"conn": {"online": True, "version": "1.2.0", "account": ACCT},
            "cases": [{"before": settings(s0, r0, n0), "after": settings(s1, r1, n1), "variant": v}
                      for s0, r0, s1, r1, v, n0, n1 in cases.values()]}
    body = SAVES + """let boxes = {};
function $(sel) { return boxes[sel] || null; }
function $$() { return []; }
function toast() {}
function errText(e) { throw e; }   // sendSettings' catch must not swallow an error (node_run adds what it lacks)
let answer = null;
async function api() { return { settings: JSON.parse(JSON.stringify(answer)) }; }
const out = [];
(async () => {
  S.connector = input.conn; S.symbols = ["EURUSD", "GBPUSD"];
  for (const c of input.cases) {
    S.settings = JSON.parse(JSON.stringify(c.before)); S.stVariant = c.variant; answer = c.after;
    const rc = { innerHTML: "kept", hidden: null, classList: { toggle: (k, on) => { if (k === "hidden") rc.hidden = on; } } };
    boxes = { "#stCard": { innerHTML: "kept" }, "#rpCard": rc, "#burstConn": { innerHTML: "kept" } };
    savePatch = { min_confidence: 71 };
    await sendSettings();
    out.push({ st: boxes["#stCard"].innerHTML === "kept" ? "kept" : "drawn", rp: rc.innerHTML === "kept" ? "kept" : rc.innerHTML,
      hidden: rc.hidden, conn: boxes["#burstConn"].innerHTML, variant: stVariant() });
  }
  process.stdout.write(JSON.stringify(out));
})();"""
    got, err = node_run(["sendSettings"], body, data)
    check(got is not None, "sendSettings runs in node with the cards", err)
    banner, gone = EN["burst_conn_130"], EN["rp_pairs_gone"]

    def variant(s_on, r_on, v):   # stVariant
        return "rapid" if r_on else "classic" if s_on else v or "classic"
    for (name, (s0, r0, s1, r1, v, n0, n1)), res in zip(cases.items(), got or []):
        was, now = variant(s0, r0, v), variant(s1, r1, v)
        drawn = was != now or (s0 or r0) != (s1 or r1)
        seen = {"st": res["st"], "rp": res["rp"] if res["rp"] == "kept" else "gone" if gone in res["rp"] else "drawn",
                "hidden": res["hidden"], "variant": res["variant"],
                "banner": res["conn"] if res["conn"] == "kept" else banner in res["conn"]}
        rp_want = "kept" if n0 == n1 else "gone" if n1 == "pairs_gone" else "drawn"
        rp_how = {"kept": "left", "gone": "drawn again with its 'pairs gone' banner", "drawn": "drawn again, no banner"}
        check(seen == {"st": "drawn" if drawn else "kept", "rp": rp_want, "hidden": now != "rapid", "variant": now,
                       "banner": r1}, f"{name}: the short-term card {'drawn again' if drawn else 'left'}, Rapid's "
              f"{'shown' if now == 'rapid' else 'hidden'} and {rp_how[rp_want]}, the burst's banner "
              f"{'on' if r1 else 'off'}", seen)


def test_pairs_redraw():
    """A pair picked or taken off under Pairs (the page's click listener, run in node) saves the pairs left on and
    draws the burst card again at once: it offers only those pairs (each burst pair when none is left on), its picks
    no longer offered marked."""
    if not NODE:
        print("note: node is not installed: the Pairs click checks are skipped")
        return
    listener = next((a for a in call_args("document.addEventListener") if a.startswith('"click"')), None)
    check(listener is not None, "the page's click listener is found in app.js")
    if listener is None:
        return
    every = list(B.BURST_SYMBOLS)
    cases = {   # name: (the user's pairs, the burst's picks, the pair clicked)
        "GBPUSD taken off": (["EURUSD", "GBPUSD"], [], "GBPUSD"),
        "USDJPY picked": (["EURUSD"], [], "USDJPY"),
        "the last pair taken off (every pair)": (["EURUSD"], [], "EURUSD"),
        "a pair the burst picked taken off": (["EURUSD", "GBPUSD"], ["GBPUSD"], "GBPUSD"),
    }
    body = SAVES + """let clicked = null;
const document = { addEventListener: (k, fn) => { if (k === "click") clicked = fn; } };
document.addEventListener(""" + listener + """);
const bc = { innerHTML: "kept" };
let btns = [];
function $(sel) { return sel === "#burstSetCard" ? bc : null; }
function $$(sel) { return sel === "[data-action=sym].on" ? btns.filter((b) => b.on) : []; }
const out = [];
(async () => {
  S.symbols = input.every;
  for (const [mine, picks, x] of input.cases) {
    S.settings = { symbols: mine.slice(), burst: { ...input.served, symbols: picks } }; sent.length = 0; bc.innerHTML = "kept";
    btns = input.every.map((v) => { const b = { on: mine.includes(v), dataset: { action: "sym", v } };
      b.classList = { toggle: (k) => { if (k === "on") b.on = !b.on; } }; return b; });
    const a = btns.find((b) => b.dataset.v === x);
    await clicked({ target: { closest: (sel) => (sel === "[data-action]" ? a : null) } });
    out.push({ saved: sent.map((p) => p.symbols), card: bc.innerHTML, now: burstSetHtml() });
  }
  process.stdout.write(JSON.stringify(out));
})();"""
    data = {"every": every, "served": B.legacy_burst({}), "cases": list(cases.values())}
    got, err = node_run(["burstSetHtml"], body, data)
    check(got is not None, "the page's click listener runs in node", err)
    for (name, (mine, picks, x)), res in zip(cases.items(), got or []):
        left = [p for p in every if (p in mine) != (p == x)]
        offered = [p for p in B.BURST_SYMBOLS if not left or p in left]
        want = [("on" if p in picks else "", p) for p in offered] + [("on gone", p) for p in picks if p not in offered]
        check(res["saved"] == [left] and res["card"] == res["now"] and burst_buttons(res["card"]) == want,
              f"{name}: saves {left}, the burst card drawn again with {[p for _, p in want]}",
              (res["saved"], burst_buttons(res["card"]), res["card"] == res["now"]))


def test_pick_chips():
    """A burst currency or a Rapid pair picked, or taken off, on its card (burstWire's and rpWire's real click
    handlers, in node): the button follows, the rest of the card is left as it is, the save sends the list the buttons
    show and the server keeps it (clamp_burst, clamp_rapid); Rapid's pairs go out marked picked."""
    if not NODE:
        print("note: node is not installed: the pick button checks are skipped")
        return
    old = B.legacy_burst(V120_BURST)
    cases = {   # name: (card, served block, the button clicked, the list then)
        "burst: USD picked next to EUR": ("burst", {**old, "currencies": ["EUR"]}, "USD", ["EUR", "USD"]),
        "burst: USD taken off": ("burst", {**old, "currencies": ["USD", "EUR"]}, "USD", ["EUR"]),
        "Rapid: EURUSD picked next to GBPUSD": ("rp", R.clamp_rapid({"symbols": ["GBPUSD"]}), "EURUSD",
                                                ["GBPUSD", "EURUSD"]),
        "Rapid: EURUSD taken off": ("rp", R.clamp_rapid({"symbols": ["EURUSD", "GBPUSD"]}), "EURUSD", ["GBPUSD"]),
    }
    block, key = {"burst": "burst", "rp": "rapid"}, {"burst": "currencies", "rp": "symbols"}
    data = [{"card": c, "settings": {block[c]: served, "symbols": []}, "x": x} for c, served, x, _ in cases.values()]
    body = SAVES + """function $() { return null; }
const out = [];
for (const c of input) {
  S.settings = JSON.parse(JSON.stringify(c.settings)); sent.length = 0;
  const h = {}, toggled = [], card = { innerHTML: "kept", addEventListener: (k, fn) => { h[k] = fn; } };
  ({ burst: burstWire, rp: rpWire })[c.card](card);
  const sel = c.card === "burst" ? "[data-burst-cur]" : "[data-rp-sym]";
  const btn = { dataset: c.card === "burst" ? { burstCur: c.x } : { rpSym: c.x }, classList: { toggle: (k, on) => toggled.push([k, on]) } };
  h.click({ target: { closest: (s) => (s === sel ? btn : null) } });
  out.push({ sent: sent.map((p) => p.burst || p.rapid), toggled, card: card.innerHTML });
}
process.stdout.write(JSON.stringify(out));"""
    got, err = node_run(["burstWire", "rpWire"], body, data)
    check(got is not None, "the pick buttons click in node", err)
    for (name, (card, served, x, want)), res in zip(cases.items(), got or []):
        if len(res["sent"]) != 1:
            check(False, f"{name}: one save of the {block[card]} block", res["sent"])
            continue
        sent = {k: v for k, v in res["sent"][0].items() if k != "symbols_picked"}   # main.py drops the page's marker
        if card == "burst":     # main.py put_my_settings
            kept = B.raise_max_hold({**served, **B.clamp_burst(sent, 40, served)})
        else:
            kept = R.clamp_rapid(sent, served)
        ok = sent[key[card]] == want and kept[key[card]] == want and res["toggled"] == [["on", x in want]] \
            and res["card"] == "kept" and (card == "burst" or res["sent"][0].get("symbols_picked") is True)
        check(ok, f"{name}: the button {'on' if x in want else 'off'}, {want} sent and kept",
              (sent[key[card]], kept[key[card]], res["toggled"], res["card"][:40]))


def test_burst_test():
    """The burst test's form (burstTest, in node: modal and the POST stood in) offers each upcoming release once, a
    release a classic burst and a Rapid run share included, and sends the one picked as event_id (none picked, or
    none coming: no event_id)."""
    if not NODE:
        print("note: node is not installed: the burst test form checks are skipped")
        return
    from datetime import datetime, timedelta, timezone
    at = (datetime.now(timezone.utc) + timedelta(days=2)).isoformat()
    upcoming = [{"event_id": "ev1", "time": at, "currency": "USD", "title": "CPI m/m", "symbol": "XAUUSD",
                 "kind": "burst"},
                {"event_id": "ev1", "time": at, "currency": "USD", "title": "CPI m/m", "symbol": "EURUSD",
                 "kind": "rapid"},
                {"event_id": "ev2", "time": at, "currency": "GBP", "title": "GDP m/m", "symbol": "GBPUSD",
                 "kind": "burst"}]
    cases = {   # name: (the upcoming releases, the release picked; None: no list shown)
        "a release picked": (upcoming, "ev1"), "no release picked": (upcoming, ""), "no release coming": ([], None)}
    body = """function $() { return null; }
function toast() {}
function errText(e) { throw e; }
let pick = null, shown = null;
const calls = [];
async function modal(o) {   // the form as answered: GBPUSD, offset 3, the release picked (when the form has the list)
  shown = o.body;
  const vals = { "#bSym": { value: "GBPUSD" }, "#bOff": { value: "3" }, "#bEv": o.body.includes('id="bEv"') ? { value: pick } : null };
  return { querySelector: (s) => vals[s] || null };
}
async function api(path, opts) { calls.push([path, opts && opts.method, opts && opts.body]); return { id: 9, status: "scheduled" }; }
const out = [];
(async () => {
  S.symbols = ["EURUSD", "GBPUSD"];
  for (const [up, p] of input) {
    S.burst = { upcoming: up, bursts: [] }; pick = p; calls.length = 0;
    await burstTest();
    out.push({ calls: calls.slice(), shown });
  }
  process.stdout.write(JSON.stringify(out));
})();"""
    got, err = node_run(["burstTest"], body, list(cases.values()))
    check(got is not None, "burstTest runs in node", err)
    for (name, (up, p)), res in zip(cases.items(), got or []):
        sent = {"symbol": "GBPUSD", "offset_pips": 3, **({"event_id": p} if p else {})}
        options = re.findall(r'<option value="([^"]*)"', res["shown"])
        listed = ["", "ev1", "ev2"] if up else []
        check(res["calls"] == [["/api/bursts/test", "POST", sent]] and options == listed,
              f"{name}: the form lists {listed[1:] or 'no release'}, the test sent with "
              f"{'event_id ' + p if p else 'no event_id'}", (res["calls"], options))


def test_boot_reset():
    """A new session (boot, in node: its GETs stood in) forgets the variant picked on the page before and a redraw
    still owed to the Settings cards, so the next account's page starts from its own settings."""
    if not NODE:
        print("note: node is not installed: the session reset check is skipped")
        return
    st, rp = SC.clamp_short(None, dict(SC.DEFAULTS)), R.clamp_rapid(None)
    settings = {"symbols": [], "short_term": {**st, "enabled": False}, "rapid": {**rp, "enabled": False}}
    body = SAVES + """let saveTimer = null;
const location = { hash: "#/" };
const document = { documentElement: {} };
function renderShell() {} function openWS() {} function route() {} function renderAuth() {}
async function api(path) {
  if (path === "/api/public/info") return { version: "1.3.0", setup_required: false };
  if (path === "/api/auth/me") return { name: "T", lang: "en" };
  if (path === "/api/me/settings") return { settings: JSON.parse(JSON.stringify(input)), symbols: ["EURUSD"], modes_enabled: {} };
  throw new Error("no GET " + path);
}
(async () => {
  S.settings = JSON.parse(JSON.stringify(input)); S.stVariant = "rapid"; S.setConnStale = true;
  const before = stVariant();
  await boot();
  process.stdout.write(JSON.stringify({ before, variant: S.stVariant, stale: S.setConnStale, shown: stVariant() }));
})();"""
    got, err = node_run(["boot", "stVariant"], body, settings)
    check(got == {"before": "rapid", "variant": None, "stale": False, "shown": "classic"}, "a new session drops the "
          "variant picked before (both off: classic shown) and the Settings redraw still owed", got or err)


def test_rapid_labels():
    """Rapid's runs are named apart from the classic burst's (run in node): on the Market page's release chip
    (pageMarket, its GETs stood in), the Dashboard card's next release (burstCardHtml) and the burst toasts (onWS);
    the release chip is gold when one of its runs trades on its own kind's switch (Rapid's news runs, or the classic
    burst; the admin's above both); and the burst's money with no connector reads as a % of the balance in both
    languages (burstMoneyText)."""
    if not NODE:
        print("note: node is not installed: the Rapid label checks are skipped")
        return
    from datetime import datetime, timedelta, timezone
    at = (datetime.now(timezone.utc) + timedelta(days=2)).isoformat()
    cal = [{"id": i, "time": at, "currency": c, "impact": "High", "title": title, "actual": "", "forecast": "0.3%",
            "previous": "0.2%", "source": "forexfactory"}
           for i, c, title in (("ev1", "USD", "CPI m/m"), ("ev3", "JPY", "BoJ Policy Rate"), ("ev4", "CAD", "CPI m/m"))]
    upcoming = [{"event_id": "ev1", "time": at, "currency": "USD", "title": "CPI m/m", "symbol": "GBPUSD",
                 "kind": "rapid"},
                {"event_id": "ev1", "time": at, "currency": "USD", "title": "CPI m/m", "symbol": "EURUSD",
                 "kind": "burst"},
                {"event_id": "ev3", "time": at, "currency": "JPY", "title": "BoJ Policy Rate", "symbol": "USDJPY",
                 "kind": "rapid"},
                {"event_id": "ev4", "time": at, "currency": "CAD", "title": "CPI m/m", "symbol": "USDCAD",
                 "kind": "burst"}]
    gold = [   # the classic burst on, Rapid on, its news trigger, the admin's burst switch
        (True, False, True, True), (False, True, True, True), (False, True, False, True), (True, True, True, False)]
    nxt = [{"event_id": "ev2", "time": at, "currency": "GBP", "title": "GDP m/m", "symbol": "GBPUSD", "kind": k}
           for k in ("rapid", "burst")]
    run = {"symbol": "GBPUSD", "event_title": "GDP m/m", "side": "buy"}
    ws = [{**run, "id": 1, "status": "triggered", "kind": "rapid"},
          {**run, "id": 2, "status": "triggered", "kind": "burst"},
          {**run, "id": 1, "status": "closed", "kind": "rapid", "pnl": 12.5}]
    data = {"cal": cal, "upcoming": upcoming, "next": nxt, "ws": ws, "burst": B.legacy_burst(V120_BURST),
            "rapid": R.clamp_rapid(None), "gold": gold}
    body = """function $() { return null; }
const toasts = [];
function toast(m) { toasts.push(m); }
function notify() {}
function pageTrades() {} function pageDashboard() {} function refreshSoft() {}
let app = { enabled: true, max_trades: 40 };
async function api(path) {
  if (path.startsWith("/api/market/calendar")) return input.cal;
  if (path.startsWith("/api/market/news")) return [];
  if (path === "/api/market/analysis") return { available: false, ai_enabled: false };
  if (path === "/api/bursts") return { upcoming: input.upcoming, bursts: [], app };
  throw new Error("no GET " + path);
}
const out = {};
(async () => {
  S.symbols = ["EURUSD", "GBPUSD"]; S.settings = { symbols: [], burst: input.burst };
  const el = { innerHTML: "" };
  await pageMarket(el, new URLSearchParams());
  out.market = el.innerHTML;
  out.gold = [];
  for (const [b, r, news, admin] of input.gold) {
    S.settings = { symbols: [], burst: { ...input.burst, enabled: b }, rapid: { ...input.rapid, enabled: r, triggers: { signals: true, news } } };
    app = { enabled: admin, max_trades: 40 };
    const g = { innerHTML: "" };
    await pageMarket(g, new URLSearchParams());
    out.gold.push(g.innerHTML);
  }
  app = { enabled: true, max_trades: 40 }; S.burstApp = app; S.settings = { symbols: [], burst: input.burst };
  out.next = input.next.map((u) => { S.burst = { upcoming: [u], bursts: [], connector: { online: false } }; return burstCardHtml(); });
  S.burst = null; S.burstSeen = {}; S.page = null;
  for (const b of input.ws) onWS({ type: "burst", burst: b });
  out.toasts = toasts;
  out.money = ["en", "ro"].map((l) => { S.lang = l; return burstMoneyText({ sl_unit: "pct", sl_money: 2 }); });
  process.stdout.write(JSON.stringify(out));
})();"""
    got, err = node_run(["pageMarket", "burstCardHtml", "onWS", "burstMoneyText"], body, data)
    check(got is not None, "the Market page, the Dashboard card and the burst toasts draw in node", err)
    if got is None:
        return
    rapid = EN["v_rapid"]
    chip = f"⚡ {EN['burst_chip']} · GBPUSD ({rapid}), EURUSD</span>"
    check(chip in got["market"], "Market: the release's chip names the Rapid run's pair (Rapid), the burst's bare",
          re.findall(r"⚡[^<]*", got["market"]))
    mark = re.compile(r'<span class="chip (gold|)" title="[^"]*">⚡ [^·]*· ([^<]*)</span>')
    for (b_on, r_on, news, admin), html in zip(gold, got["gold"]):
        chips = dict((p, c == "gold") for c, p in mark.findall(html))
        rp = r_on and news
        want = {f"GBPUSD ({rapid}), EURUSD": admin and (b_on or rp), f"USDJPY ({rapid})": admin and rp,
                "USDCAD": admin and b_on}
        check(chips == want, f"Market, the classic burst {'on' if b_on else 'off'}, Rapid {'on' if r_on else 'off'}"
              f"{'' if news else ' (its news trigger off)'}{'' if admin else ', the admin switch off'}: a release's "
              "chip gold when one of its runs trades on its own kind's switch", chips)
    check(f"GBP · GBPUSD · {rapid} · " in got["next"][0] and "GBP · GBPUSD · " in got["next"][1]
          and f"GBPUSD · {rapid}" not in got["next"][1], "Dashboard: the next release says Rapid for a Rapid run only",
          [re.findall(r"GBP · [^<]*", h) for h in got["next"]])
    toasts = got["toasts"]
    check(len(toasts) == 3 and f"<br>GBPUSD ({rapid}) · GDP m/m · " in toasts[0] and "<br>GBPUSD · GDP m/m · "
          in toasts[1] and f"<br>GBPUSD ({rapid}) · GDP m/m · " in toasts[2] and "$12.50" in toasts[2],
          "the burst toasts say Rapid for a Rapid run only (triggered, closed)", toasts)
    want = [EN["burst_money_off"].replace("{v}", "2.0"), RO["burst_money_off"].replace("{v}", "2,0")]
    check(got["money"] == want, "no connector: the burst's money as a % of the balance (EN 2.0, RO 2,0)",
          got["money"])


def html_escape(s: str) -> str:
    """As app.js esc()."""
    return "".join({"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"}.get(c, c) for c in s)


def test_stale_page():
    """v1.3.1: a card sends only the keys edited on it (the real saveBurst, saveShort, saveRapid, saveSettings and
    sendSettings in node, the PUT stood in), so a page left open since another page changed a key never sends that
    key back: what main.py put_my_settings keeps of each body is the other page's value (the burst's SL in % of the
    balance, Rapid on). Two edits within one save both go out, a money value goes with the unit this page shows, a
    modes switch sends only itself, and after the answer the page holds whole blocks again."""
    if not NODE:
        print("note: node is not installed: the stale page checks are skipped")
        return
    st, rp = SC.clamp_short(None, dict(SC.DEFAULTS)), R.clamp_rapid(None)
    burst = B.legacy_burst({"enabled": True, "trades": 10, "hold_seconds": 30, "currencies": []})
    page = {"burst": burst, "short_term": {**st, "enabled": True}, "rapid": {**rp, "enabled": False},
            "modes": {"trend": True, "custom": False, "news_ai": False}, "symbols": [], "execution": "auto",
            "short_term_note": None, "rapid_note": None}
    server = {**page, "burst": {**burst, "sl_unit": "pct", "sl_money": 2.0}, "short_term": {**st, "enabled": False},
              "rapid": {**rp, "enabled": True}, "modes": {"trend": False, "custom": False, "news_ai": False}}
    steps = {   # name: the edits made on the old page within one save (block, patch)
        "two burst fields": [["burst", {"trades": 15}], ["burst", {"hold_seconds": 45}]],
        "a short-term field": [["short_term", {"max_open": 3}]],
        "a Rapid field": [["rapid", {"orders": 12}]],
        "a mode": [["modes", {"custom": True}]],
        "the burst's stop money": [["burst", {"sl_money": 3}]],
    }
    body = """let saveTimer = null, savePatch = {}, saveSent = {}, saveLoud = false, saveChain = Promise.resolve(),
  saveDone = 0, saveEpoch = 0;
function $() { return null; }
function burstConnShow() {}
const puts = [], errs = [];
function esc(x) { return String(x); }
function errText(e) { return String((e && e.stack) || e); }
function toast(msg, kind) { if (kind === "err") errs.push(msg); }   // a save that failed
console.error = (...a) => errs.push(a.map(String).join(" "));   // a save's error (saveChain catches it)
let slow = false, seen = null;
async function api(path, o) {
  puts.push(JSON.parse(JSON.stringify(o.body)));
  if (slow && puts.length === 2) seen = { keys: Object.keys(S.settings.burst).sort(), hold: S.settings.burst.hold_seconds };
  if (slow) await new Promise((r) => setTimeout(r, 200));     // the answer takes a while: an edit can come meanwhile
  return { settings: JSON.parse(JSON.stringify(input.server)) };
}
const save = { burst: (p) => saveBurst(p), short_term: (p) => saveShort(p), rapid: (p) => saveRapid(p),
  modes: (p) => saveSettings({ modes: p }) };
(async () => {
  const out = [];
  for (const edits of input.steps) {
    S.settings = JSON.parse(JSON.stringify(input.page)); puts.length = 0;
    for (const [block, p] of edits) save[block](p);
    await new Promise((r) => setTimeout(r, 400)); await saveChain;
    out.push({ puts: puts.slice(), after: Object.fromEntries(["burst", "short_term", "rapid", "modes"].map((k) => [k, Object.keys(S.settings[k]).sort()])) });
  }
  // an edit made while a PUT is on its way: on top of that PUT's answer, the block whole, then its own PUT
  S.settings = JSON.parse(JSON.stringify(input.page)); puts.length = 0; slow = true;
  saveBurst({ trades: 15 });
  await new Promise((r) => setTimeout(r, 380));
  saveBurst({ hold_seconds: 50 });
  await new Promise((r) => setTimeout(r, 900)); await saveChain;
  process.stdout.write(JSON.stringify({ steps: out, onway: { puts: puts.slice(), seen }, errs }));
})();"""
    # what sendSettings reads in its try: named here, since saveChain catches what is missing (node_run cannot see it)
    got, err = node_run(["saveBurst", "saveShort", "saveRapid", "saveSettings", "sendSettings", "stVariant", "stOn", "t",
                         "pageSettings"], body,
                        {"page": page, "server": server, "steps": list(steps.values())})
    check(got is not None, "the save code runs in node (stand-in PUT)", err)
    if got is None:
        return

    def keep(cur: dict, b: dict) -> dict:      # main.py put_my_settings, for the blocks sent here
        cur = json.loads(json.dumps(cur))
        if "burst" in b:
            cur["burst"] = B.raise_max_hold({**cur["burst"], **B.clamp_burst(b["burst"], 40, cur["burst"])})
        if "short_term" in b:
            cur["short_term"] = SC.clamp_short({k: v for k, v in b["short_term"].items() if k != "symbols_picked"},
                                               cur["short_term"])
        if "rapid" in b:
            cur["rapid"] = R.clamp_rapid({k: v for k, v in b["rapid"].items() if k != "symbols_picked"}, cur["rapid"])
        if "modes" in b:
            cur["modes"] = {m: bool(b["modes"].get(m, cur["modes"].get(m))) for m in ("trend", "custom", "news_ai")}
        return cur
    want = {
        "two burst fields": {"burst": {"trades": 15, "hold_seconds": 45}},
        "a short-term field": {"short_term": {"max_open": 3}},
        "a Rapid field": {"rapid": {"orders": 12}},
        "a mode": {"modes": {"custom": True}},
        "the burst's stop money": {"burst": {"sl_money": 3, "sl_unit": "pips"}},
    }
    check(got["errs"] == [], "no save fails on the way (saveChain's catch got nothing)", got["errs"][:3])
    w = got["onway"]
    check(w["puts"] == [{"burst": {"trades": 15}}, {"burst": {"hold_seconds": 50}}]
          and w["seen"] == {"keys": sorted(server["burst"]), "hold": 50},
          "an edit made while a PUT is on its way stays on top of its answer (the block whole), then goes out alone", w)
    for (name, _), res in zip(steps.items(), got["steps"]):
        check(res["puts"] == [want[name]], f"{name}: one PUT with only the keys edited ({want[name]})", res["puts"])
        if len(res["puts"]) != 1:
            continue
        kept = keep(server, res["puts"][0])
        if name == "the burst's stop money":
            ok = kept["burst"]["sl_unit"] == "pips" and kept["burst"]["sl_money"] == 3
            check(ok, f"{name}: kept in the unit this page shows (3 pips-mode money, never 3 % read as $)",
                  kept["burst"])
        else:
            ok = (kept["burst"]["sl_unit"] == "pct" and kept["burst"]["sl_money"] == 2.0
                  and kept["short_term"]["enabled"] is False and kept["rapid"]["enabled"] is True
                  and kept["modes"]["trend"] is False)
            check(ok, f"{name}: the other page's changes stay (burst SL in %, Rapid on, its modes)",
                  {k: kept[k] for k in ("burst", "modes")})
        whole = all(res["after"][k] == sorted(set(server[k]) | set(want[name].get(k, {}))) for k in
                    ("burst", "short_term", "rapid", "modes"))
        check(whole, f"{name}: after the answer the page holds whole blocks", res["after"])


def test_market_clock():
    """v1.3.1: the top bar's market clock (fxClosed, mktState, mktLeft, updateMktPill, in node) says shut exactly when
    bot/mt5_broker.py _fx_closed does, hour by hour over two years (both US DST switches, Christmas, New Year), and
    counts down to the first moment the state flips; its texts in both languages."""
    if not NODE:
        print("note: node is not installed: the market clock checks are skipped")
        return
    import bot.mt5_broker as MB
    from datetime import datetime, timezone
    t0 = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp())
    hours = [t0 + i * 3600 for i in range(2 * 366 * 24)]
    probes = [h + d for h in hours for d in (0, 1799, 3599)]
    samples = hours[::7]
    body = """let mktNext = { at: 0, closed: null };
const els = { "#mktDot": { className: "" }, "#mktText": { textContent: "" },
  "#mktPill": { title: "", hidden: false, classList: { toggle(c, on) { if (c === "hidden") els["#mktPill"].hidden = on; } } } };
function $(sel) { return els[sel] || null; }
const out = { closed: input.probes.map((s) => fxClosed(s * 1000)), next: [], left: [], pill: [] };
for (const s of input.samples) { mktNext = { at: 0, closed: null }; const m = mktState(s * 1000); out.next.push([m.closed, m.at / 1000]); }
for (const [lang, ms] of input.left) { S.lang = lang; out.left.push(mktLeft(ms)); }
for (const [lang, s, market] of input.pill) {
  S.lang = lang; S.settings = market ? { market } : null; Date.now = () => s * 1000; mktNext = { at: 0, closed: null };
  els["#mktText"].textContent = ""; updateMktPill();
  out.pill.push([els["#mktDot"].className, els["#mktText"].textContent, els["#mktPill"].title, els["#mktPill"].hidden]);
}
process.stdout.write(JSON.stringify(out));"""
    fri = int(datetime(2026, 11, 13, 20, 0, tzinfo=timezone.utc).timestamp())    # Friday, 2 h before the close
    sat = int(datetime(2026, 11, 14, 12, 0, tzinfo=timezone.utc).timestamp())    # Saturday noon: shut, opens in 34 h
    tue = int(datetime(2026, 11, 10, 12, 0, tzinfo=timezone.utc).timestamp())    # Tuesday: open, days to go
    thu = int(datetime(2026, 11, 12, 19, 0, tzinfo=timezone.utc).timestamp())    # Thursday: Sydney's Friday next
    none = {"sessions": {k: {"on": False} for k in ("sydney", "tokyo", "london", "newyork")}}
    data = {"probes": probes, "samples": samples,
            "left": [["ro", 90061e3], ["en", 90061e3], ["ro", 3723e3], ["en", -5e3]],
            "pill": [["ro", sat], ["en", sat], ["ro", fri], ["ro", tue], ["en", tue], ["ro", thu], ["ro", tue, none],
                     ["ro", fri, none], ["ro", tue, {"pill": False}]]}
    got, err = node_run(["fxClosed", "mktState", "mktLeft", "updateMktPill"], body, data)
    check(got is not None, "the market clock runs in node", err)
    if got is None:
        return
    bad = [(datetime.fromtimestamp(s, timezone.utc).isoformat(), g) for s, g in zip(probes, got["closed"])
           if g != MB._fx_closed(s)]
    check(not bad, f"fxClosed is bot/mt5_broker.py _fx_closed at {len(probes)} moments over 2026-2027", bad[:5])
    wrong = []
    for s, (closed, at) in zip(samples, got["next"]):
        flips = int(at)
        same = all(MB._fx_closed(x) == closed for x in range(s, flips, 1800)) and MB._fx_closed(flips - 1) == closed
        if closed != MB._fx_closed(s) or not same or MB._fx_closed(flips) == closed:
            wrong.append((datetime.fromtimestamp(s, timezone.utc).isoformat(), closed,
                          datetime.fromtimestamp(flips, timezone.utc).isoformat()))
    check(not wrong, f"mktState counts down to the first moment the market opens or shuts ({len(samples)} samples)",
          wrong[:5])
    check(got["left"] == ["1z 01:01:01", "1d 01:01:01", "01:02:03", "00:00:00"],
          "mktLeft: days, then hh:mm:ss (z in Romanian, d in English), never below zero", got["left"])
    p = got["pill"]
    ok = (p[0][0] == "dot off" and p[0][1] == "Piața se deschide în 1z 10:00:00" and p[0][2].startswith("Piața forex se deschide")
          and p[1][1] == "Market opens in 1d 10:00:00" and p[2][0] == "dot warn" and p[2][1] == "Piața se închide în 02:00:00"
          and p[3][0] == "dot on" and p[3][1] == "Piață deschisă · New York în 01:00:00"
          and p[4][1] == "Market open · New York in 01:00:00"
          and p[3][2].startswith("Piața forex se închide") and p[3][2].endswith(" · Deschise acum: Londra.")
          and p[5] == ["dot on", "Piață deschisă · Sydney în 01:00:00", p[5][2], False]
          and p[5][2].endswith("Deschise acum: New York.")
          and p[6][:2] == ["dot on", "Piață deschisă"] and p[6][2].endswith("Nicio sesiune nu e deschisă acum.")
          and p[7][:2] == ["dot warn", "Piața se închide în 02:00:00"] and p[8][3] is True
          and not any(x[3] for x in p[:8]))
    check(ok, "the pill: shut, the time until it opens (red); open, the next session followed that opens before the "
          "close (v1.3.2), else in its last day the time until it shuts (amber), else open (green); the tooltip names "
          "the moment and the sessions open; switched off in Settings, hidden; in both languages", p)


TZ_OF = {"sydney": "Australia/Sydney", "tokyo": "Asia/Tokyo", "london": "Europe/London", "newyork": "America/New_York"}


def test_market_sessions():
    """v1.3.2: the trading sessions (sessOpen, sessState, in node) are open exactly when each city's own clock, by
    Python's zoneinfo, is on a weekday inside its hours while the market is open, hour by hour over 2026-2027 (every
    daylight-saving switch of the four cities), with the default hours and with others; each counts down to its first
    flip; the Dashboard card draws each session's pieces of this PC's day (in UTC, and in India, half an hour off UTC),
    its hours in this PC's time and the London / New York overlap; the Settings card draws, and each change sends the
    sessions whole (a close at or before the opening moved after it); "Usual hours" puts the hours back and keeps the
    ticks."""
    if not NODE:
        print("note: node is not installed: the session checks are skipped")
        return
    import bot.mt5_broker as MB
    from datetime import datetime, timezone
    from zoneinfo import ZoneInfo
    from server.dashboard import MARKET_DEFAULT
    custom = {"sydney": {"on": True, "from": 10, "to": 11}, "tokyo": {"on": False, "from": 9, "to": 18},
              "london": {"on": True, "from": 0, "to": 24}, "newyork": {"on": True, "from": 23, "to": 24}}
    zones = {k: ZoneInfo(v) for k, v in TZ_OF.items()}

    def ref(sid, c, s):
        if not c["on"]:
            return False
        d = datetime.fromtimestamp(s, zones[sid])
        return d.weekday() < 5 and c["from"] <= d.hour < c["to"] and not MB._fx_closed(s)

    t0 = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp())
    hours = [t0 + i * 3600 for i in range(2 * 366 * 24)]
    probes = [h + d for h in hours for d in (0, 3599)]
    samples = hours[::23]
    tue = int(datetime(2026, 11, 10, 12, 0, tzinfo=timezone.utc).timestamp())
    body = """const out = { open: {}, custom: {}, next: {}, card: [], set: [], saved: [] };
const D = { sessions: input.def };
for (const [id] of SESSIONS) {
  out.open[id] = input.probes.map((s) => sessOpen(id, s * 1000, D));
  out.custom[id] = input.probes.filter((_, i) => i % 3 === 0).map((s) => sessOpen(id, s * 1000, { sessions: input.custom }));
  out.next[id] = input.samples.map((s) => { const n = sessState(id, s * 1000, D); return [n.open, n.at / 1000]; });
}
for (const [tz, lang] of [["UTC", "en"], ["Asia/Kolkata", "ro"]]) {
  process.env.TZ = tz; S.lang = lang; out.card.push(sessCardHtml(input.tue * 1000, D));
}
process.env.TZ = "UTC"; S.lang = "en";
out.card.push(sessCardHtml(input.tue * 1000, { sessions: Object.fromEntries(SESSIONS.map(([id]) => [id, { ...input.def[id], on: false }])) }));
Date.now = () => input.tue * 1000;
const listeners = {}, box = { innerHTML: "", addEventListener(k, f) { listeners[k] = f; } };
function saveSettings(p) { out.saved.push(JSON.parse(JSON.stringify(p))); mergeSettings(S.settings, p); }
function updateMktPill() {}
for (const lang of ["ro", "en"]) { S.lang = lang; S.settings = { market: JSON.parse(JSON.stringify(input.mk)) }; out.set.push(mkSetHtml()); }
mkWire(box);
const ev = (dataset, more = {}) => listeners.change({ target: { dataset, ...more } });
ev({ mkH: "to", id: "london" }, { value: "8" });
ev({ mkH: "from", id: "newyork" }, { value: "23" });
ev({ mkOn: "tokyo" }, { checked: true });
ev({ mk: "pill" }, { checked: false });
ev({ mkH: "from", id: "paris" }, { value: "3" });
ev({ mkOn: "sydney" }, { checked: false });
listeners.click({ target: { closest: (q) => (q === "[data-mk-reset]" ? {} : null) } });
listeners.click({ target: { closest: () => null } });
out.set.push(box.innerHTML);
process.stdout.write(JSON.stringify(out));"""
    mk = {"pill": True, "widget": True, "sessions": {**MARKET_DEFAULT["sessions"], "tokyo": {"on": False, "from": 10, "to": 19}}}
    data = {"probes": probes, "samples": samples, "def": MARKET_DEFAULT["sessions"], "custom": custom, "tue": tue, "mk": mk}
    got, err = node_run(["sessOpen", "sessState", "sessCardHtml", "mkSetHtml", "mkWire", "mergeSettings"], body, data)
    check(got is not None, "the sessions run in node", err)
    if got is None:
        return
    bad = {sid: [datetime.fromtimestamp(s, timezone.utc).isoformat() for s, g in zip(probes, got["open"][sid])
                 if g != ref(sid, MARKET_DEFAULT["sessions"][sid], s)][:3] for sid in TZ_OF}
    check(not any(bad.values()), f"each session is open when its city's clock says so ({len(probes)} moments, 2026-2027)",
          bad)
    few = probes[::3]
    bad = {sid: [datetime.fromtimestamp(s, timezone.utc).isoformat() for s, g in zip(few, got["custom"][sid])
                 if g != ref(sid, custom[sid], s)][:3] for sid in TZ_OF}
    check(not any(bad.values()), "the same with other hours (a whole day, one hour, the last hour, a session off)", bad)
    wrong = []
    for sid in TZ_OF:
        c = MARKET_DEFAULT["sessions"][sid]
        for s, (opn, at) in zip(samples, got["next"][sid]):
            flips = int(at)
            if opn != ref(sid, c, s) or ref(sid, c, flips) == opn or any(ref(sid, c, x) != opn for x in range(s, flips, 1800)):
                wrong.append((sid, datetime.fromtimestamp(s, timezone.utc).isoformat()))
    check(not wrong, f"each session counts down to its first opening or closing ({len(samples)} samples each)", wrong[:5])

    def rows(html):
        out = {}
        for m in re.finditer(r'data-sess="(\w+)">(.*?)</div></div>', html, re.S):
            spans = [(float(a), float(b)) for a, b in re.findall(r'left:([\d.]+)%;width:([\d.]+)%', m[2])]
            hrs = re.search(r'<span class="faint">(.*?)</span>', m[2])
            out[m[1]] = (spans, hrs and hrs[1])
        return out
    utc, ist, off = got["card"]
    want = {"tokyo": ([(0.0, 37.5)], "Wed 00:00 – 09:00 your time"), "london": ([(33.33, 37.5)], "08:00 – 17:00 your time"),
            "newyork": ([(54.17, 37.5)], "13:00 – 22:00 your time"),
            "sydney": ([(0.0, 20.83), (83.33, 16.67)], "20:00 – 05:00 your time")}
    check(rows(utc) == want and "London and New York overlap 13:00 – 17:00 your time" in utc
          and re.findall(r'<span style="left:([\d.]+)%">(\d\d)</span>', utc)
          == [("0.00", "00"), ("25.00", "06"), ("50.00", "12"), ("75.00", "18"), ("100.00", "24")],
          "the card on a PC in UTC: each session's pieces of the day (Sydney's two), its hours (the next ones when "
          "closed; another day's with the weekday), the overlap, the hour marks", rows(utc))
    want = {"tokyo": [(22.92, 37.5)], "london": [(56.25, 37.5)], "newyork": [(0.0, 14.58), (77.08, 22.92)],
            "sydney": [(6.25, 37.5)]}
    r_ist = rows(ist)
    check({k: v[0] for k, v in r_ist.items()} == want and r_ist["london"][1] == "13:30 – 22:30 ora dvs."
          and r_ist["newyork"][1] == "18:30 – 03:30 ora dvs."
          and "Londra și New York sunt deschise împreună 18:30 – 22:30 ora dvs." in ist,
          "the card on a PC in India (half an hour off UTC), in Romanian: the pieces cut at its midnight", r_ist)
    check("sess-row" not in off and "No session is followed" in off,
          "no session followed: the card says so, with the way to Settings", off[:200])
    check(not any(x in h for h in got["card"] + got["set"] for x in ("undefined", "NaN", "null")),
          "nothing undefined, NaN or null in the cards")
    ro, en = got["set"][:2]
    check(ro.count('data-mk-on="') == 4 and ro.count("<option") == 4 * (24 + 24) and 'data-mk-on="tokyo" >' in ro
          and ro.count(" checked") == 2 + 3 and 'value="10" selected' in ro and "Ceas de piață" in ro
          and "Market clock" in en and "Orele obișnuite" in ro,
          "the Settings card: both switches, the four sessions with their hours (opens 00-23, closes 01-24) as saved",
          ro[:300])
    sv = got["saved"]
    full = lambda **kw: {**mk["sessions"], **kw}  # noqa: E731
    check(len(sv) == 6
          and sv[0] == {"market": {"sessions": full(london={"on": True, "from": 7, "to": 8})}}
          and sv[1] == {"market": {"sessions": full(london={"on": True, "from": 7, "to": 8},
                                                    newyork={"on": True, "from": 23, "to": 24})}}
          and sv[2]["market"]["sessions"]["tokyo"] == {"on": True, "from": 10, "to": 19}
          and sv[3] == {"market": {"pill": False}}
          and sv[4]["market"]["sessions"]["sydney"] == {"on": False, "from": 7, "to": 16}
          and sv[5] == {"market": {"sessions": {k: {**MARKET_DEFAULT["sessions"][k], "on": k != "sydney"}
                                                for k in TZ_OF}}},
          "each change sends the sessions whole (a close at or before the opening moved), a switch alone, an unknown "
          "session nothing; Usual hours puts the hours back and keeps the ticks", sv)
    check("data-mk-on" in got["set"][2], "the card is drawn again after a change", got["set"][2][:120])


def test_nothing_left():
    after = {d: sorted(p.name for p in (ROOT / d).glob("*")) for d in _KEPT}
    check(after == _before, "nothing new in data/ or logs/", {d: sorted(set(after[d]) - set(_before[d])) for d in _KEPT})


if __name__ == "__main__":
    t0 = time.time()
    try:
        for fn in (test_texts, test_analysis_texts, test_error_codes, test_assets, test_node, test_mirrors,
                   test_saved_fields, test_legacy, test_node_logic, test_cards, test_hold_clear, test_version_label,
                   test_burst_gate, test_variant_switch, test_switch_text, test_pairs_kept, test_rapid_raise,
                   test_unit_switch, test_burst_money, test_switches, test_burst_picks_gone, test_settings_redraw,
                   test_burst_banner, test_send_redraw, test_pairs_redraw, test_pick_chips, test_burst_test,
                   test_boot_reset, test_rapid_labels, test_stale_page, test_market_clock, test_market_sessions,
                   test_nothing_left):
            print(f"── {fn.__name__}")
            try:
                fn()
            except Exception as exc:                            # a crash is a failure, the next test still runs
                import traceback
                traceback.print_exc()
                check(False, f"{fn.__name__} crashed", exc)
    finally:
        shutil.rmtree(TMP, ignore_errors=True)
    print(f"\n{len(PASSED)} passed / {len(FAILED)} failed  ({time.time() - t0:.1f} s)")
    sys.exit(1 if FAILED else 0)
