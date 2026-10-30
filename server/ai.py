"""Claude-powered market analysis and trading coach.

analyze_market(): economic calendar + fresh headlines + recent prices  ->  per-headline tags and a
                  structured trade bias per symbol (direction, confidence, rationale in EN and RO).
coach_reply():    an educational assistant that explains signals and trading concepts to clients.
post_mortem():    short lessons from the analyst's own losing calls (self-learning, opt-in, admin-approved).

Both run synchronously; the server calls them via asyncio.to_thread.
"""
import json
import logging
from datetime import datetime
from typing import Literal

import anthropic
from pydantic import BaseModel, Field

from bot.symbols import price_digits

from .settings import AI_MODEL

log = logging.getLogger("ai")
FALLBACK_BETA = "server-side-fallback-2026-07-01"
_client: anthropic.Anthropic | None = None


class AIError(RuntimeError):
    pass


def client() -> anthropic.Anthropic:
    global _client
    if _client is None:
        _client = anthropic.Anthropic(max_retries=3)
    return _client


# ── structured output schema ───────────────────────────────────
class CurrencyEffect(BaseModel):
    currency: str = Field(description="ISO code, e.g. USD, EUR, JPY")
    effect: Literal["positive", "negative", "neutral"]


class HeadlineTag(BaseModel):
    id: str = Field(description="The headline id exactly as given")
    effects: list[CurrencyEffect]
    impact: Literal["high", "medium", "low"]
    note: str = Field(description="One short sentence: why it matters for FX (English)")


class PairBias(BaseModel):
    symbol: str = Field(description="One of the symbols provided, exactly as written")
    direction: Literal["buy", "sell", "neutral"]
    confidence: int = Field(description="0-100. Above 70 only with clear, fresh, consistent evidence")
    horizon_hours: int = Field(description="How long the bias should hold, 1-48")
    rationale_en: str = Field(description="2-3 plain sentences a beginner can follow")
    rationale_ro: str = Field(description="The same rationale in Romanian")
    drivers: list[str] = Field(description="Headline ids or calendar event titles behind this bias")


class MarketAnalysis(BaseModel):
    headlines: list[HeadlineTag]
    biases: list[PairBias]
    summary_en: str = Field(description="3-4 sentence overview of today's FX drivers")
    summary_ro: str = Field(description="The same overview in Romanian")


ANALYST_SYSTEM = """You are the macro/news analyst of a forex signals platform. Each run you receive the
economic calendar around now, fresh and recent FX headlines, and recent price action for a fixed list of
currency pairs. Produce a tag for every headline and a bias for every pair.

How to judge:
- Base views only on the material provided. Do not invent data releases, numbers or quotes.
- A currency strengthens on hawkish central-bank signals, data beating forecast, risk flows in its favour;
  it weakens on the opposite. A pair's direction is base-currency strength minus quote-currency strength.
- Released data matters more than speculation. Headlines often state the actual number of a release;
  compare it with the calendar forecast when both are available.
- Be conservative. Default to "neutral" with low confidence when evidence is thin, stale, mixed, or
  already reflected in the recent price move. Confidence above 70 needs clear, fresh, consistent evidence.
- Lower confidence when a high-impact event for either currency is due within the next few hours.
- Write rationales for learners: plain words, name the driver, say what would invalidate the view.
- This is analysis for an automated system and its subscribers, not personal financial advice."""


def _fmt_events(events: list[dict]) -> str:
    if not events:
        return "(none)"
    return "\n".join(
        f"- {e['time']:%a %d %b %H:%M} UTC | {e['currency']} | {e['impact']} | {e['title']}"
        f" | actual {e.get('actual') or '-'} | forecast {e.get('forecast') or '-'} | previous {e.get('previous') or '-'}"
        for e in events)


def _fmt_news(items: list[dict]) -> str:
    if not items:
        return "(none)"
    return "\n".join(f"- id={n['id'][:12]} | {n['published_at']:%d %b %H:%M} UTC | {n['source']} | {n['title']}"
                     + (f" — {n['summary'][:240]}" if n.get("summary") else "") for n in items)


def analyze_market(now: datetime, symbols: list[str], prices: dict[str, dict], events: list[dict],
                   fresh_news: list[dict], recent_news: list[dict],
                   lessons: list[str] | None = None) -> tuple[MarketAnalysis, dict]:
    price_lines = "\n".join(
        f"- {s}: last {p['last']:.{price_digits(s)}f}, 24h change {p['change_24h_pips']:+.1f} pips, "
        f"ATR {p['atr_pips']:.1f} pips" for s, p in prices.items()) or "(no price data)"
    user = (
        f"Current time: {now:%A %d %B %Y %H:%M} UTC\n\n"
        f"Pairs to assess (give exactly one bias each): {', '.join(symbols)}\n\n"
        f"Recent price action:\n{price_lines}\n\n"
        f"Economic calendar (last 24h and next 24h, medium/high impact):\n{_fmt_events(events)}\n\n"
        f"FRESH headlines since the last run (tag each one, use the id prefix shown):\n{_fmt_news(fresh_news)}\n\n"
        f"Earlier headlines from the last 12h (context only, do not tag):\n{_fmt_news(recent_news)}"
    )
    if lessons:
        user += ("\n\nLessons from your past losing calls (approved by an admin; weigh them, they are not rules):\n"
                 + "\n".join(f"- {x}" for x in lessons))
    try:
        resp = client().beta.messages.parse(
            model=AI_MODEL,
            max_tokens=16000,
            betas=[FALLBACK_BETA],
            fallbacks="default",
            system=[{"type": "text", "text": ANALYST_SYSTEM, "cache_control": {"type": "ephemeral"}}],
            output_config={"effort": "medium"},
            output_format=MarketAnalysis,
            messages=[{"role": "user", "content": user}],
        )
    except anthropic.APIStatusError as e:
        raise AIError(f"Claude API error {e.status_code}: {e.message}") from e
    except anthropic.APIConnectionError as e:
        raise AIError("Cannot reach the Claude API") from e
    if resp.stop_reason == "refusal":
        raise AIError("The model declined this analysis")
    if resp.parsed_output is None:
        raise AIError(f"No structured output (stop_reason={resp.stop_reason})")
    usage = {"input_tokens": resp.usage.input_tokens, "output_tokens": resp.usage.output_tokens, "model": resp.model}
    return resp.parsed_output, usage


# ── post-mortem (self-learning) ────────────────────────────────
class Lessons(BaseModel):
    lessons: list[str] = Field(description="0-3 lessons, each one sentence under 200 characters")


POST_MORTEM_SYSTEM = """You review the losing calls of a forex news analyst (yourself, in earlier runs).
Find what the losing calls had in common and write at most three short, general lessons the analyst should
weigh next time (for example: "After a data surprise, lower confidence when the pair already moved more than
its ATR"). Rules: base lessons only on the calls given; no prices, dates or ids; no lesson that would only
fit one trade; never promise results. If nothing general can be learned, return an empty list."""


def post_mortem(losses: list[dict]) -> list[str]:
    """Lessons from losing AI signals (Signal.to_dict() rows)."""
    lines = "\n".join(
        f"- {s['symbol']} {s['side']} confidence {s.get('confidence')}, opened {s.get('created_at')}, "
        f"closed by {s.get('close_reason') or s.get('status')}, {s.get('result_pips') or 0:+.1f} pips. "
        f"Rationale: {(s.get('rationale') or '')[:400]}" for s in losses)
    try:
        resp = client().beta.messages.parse(
            model=AI_MODEL,
            max_tokens=4000,
            betas=[FALLBACK_BETA],
            fallbacks="default",
            system=[{"type": "text", "text": POST_MORTEM_SYSTEM}],
            output_config={"effort": "medium"},
            output_format=Lessons,
            messages=[{"role": "user", "content": f"Losing calls:\n{lines}"}],
        )
    except anthropic.APIStatusError as e:
        raise AIError(f"Claude API error {e.status_code}: {e.message}") from e
    except anthropic.APIConnectionError as e:
        raise AIError("Cannot reach the Claude API") from e
    if resp.stop_reason == "refusal" or resp.parsed_output is None:
        raise AIError(f"No lessons returned (stop_reason={resp.stop_reason})")
    return [x.strip()[:200] for x in resp.parsed_output.lessons if x.strip()][:3]


# ── coach ──────────────────────────────────────────────────────
COACH_SYSTEM = """You are the trading coach inside a forex signals platform. Clients are mostly beginners.
Teach clearly and honestly: explain concepts (pips, lots, leverage, stop-loss, risk per trade, economic
releases, trend vs. range), walk through why a given signal was issued and what would invalidate it, and
help them review their own trades.

Rules:
- Answer in the client's language (given below). Keep answers short and practical; use small examples.
- Always stress risk management: small risk per trade, stop-losses, no revenge trading, demo first.
- Never promise profits or certainty. Signals can and do lose.
- You give education, not personal financial advice: do not tell a client how much of their savings to
  invest or that a trade is suitable for their personal situation.
- Platform context (signals, the client's settings, market summary) may be attached; use it when relevant."""


def coach_reply(lang: str, history: list[dict], question: str, context: dict) -> str:
    lang_name = "Romanian" if lang == "ro" else "English"
    messages = list(history[-12:])
    messages.append({"role": "user", "content":
                     f"[Client language: {lang_name}]\n[Platform context]\n{json.dumps(context, default=str)[:6000]}\n\n"
                     f"{question}"})
    try:
        resp = client().beta.messages.create(
            model=AI_MODEL,
            max_tokens=4000,
            betas=[FALLBACK_BETA],
            fallbacks="default",
            system=[{"type": "text", "text": COACH_SYSTEM, "cache_control": {"type": "ephemeral"}}],
            output_config={"effort": "low"},
            messages=messages,
        )
    except anthropic.RateLimitError as e:
        raise AIError("The coach is busy, please try again in a minute") from e
    except anthropic.APIStatusError as e:
        raise AIError(f"Claude API error {e.status_code}") from e
    except anthropic.APIConnectionError as e:
        raise AIError("Cannot reach the Claude API") from e
    if resp.stop_reason == "refusal":
        return ("I can't help with that one. Ask me about a signal, a trading concept or your risk settings."
                if lang != "ro" else
                "Nu te pot ajuta cu asta. Întreabă-mă despre un semnal, un concept de trading sau setările de risc.")
    return "".join(b.text for b in resp.content if b.type == "text").strip()
