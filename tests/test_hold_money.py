"""Hold X / Y and the stop loss in money (v1.3.0): the pure helpers in bot/hold.py (hold_action, min_profit,
news_against) and bot/risk.py (money_for, money_stop, stop_floor_pips).

    .venv\\Scripts\\python tests\\test_hold_money.py [scenario ...]

No broker, no network, no files. Exit code 0 = every check passed.
"""
import math
import random
import sys
import time
import traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from bot.hold import hold_action, min_profit, news_against                    # noqa: E402
from bot.risk import money_for, money_stop, stop_floor_pips                   # noqa: E402
from bot.scalp import MAX_RISK_PCT                                            # noqa: E402
from bot.symbols import SymbolSpec                                            # noqa: E402

PIP = 0.0001
# the broker probe's specs: 5-digit EURUSD and 3-digit USDJPY (10 points a pip), gold with 2 digits, contract 100
# (a 0.1 pip is 10 $ a lot) and a 20 point freeze level
EURUSD = SymbolSpec("EURUSD", pip=PIP, pip_value_per_lot=10.0, digits=5, point=0.00001, volume_min=0.01,
                    volume_step=0.01, volume_max=100.0)
USDJPY = SymbolSpec("USDJPY", pip=0.01, pip_value_per_lot=6.7, digits=3, point=0.001, volume_min=0.01,
                    volume_step=0.01, volume_max=100.0)
XAUUSD = SymbolSpec("XAUUSD", pip=0.1, pip_value_per_lot=10.0, digits=2, point=0.01, volume_min=0.01,
                    volume_step=0.01, volume_max=100.0, freeze_distance=0.2, scale=5.0)
NAN, INF = float("nan"), float("inf")

TESTS = []


def scenario(fn):
    TESTS.append(fn)
    return fn


def check(cond, msg):
    if not cond:
        raise AssertionError(msg)


def near(a, b, eps=1e-6):
    return a is not None and b is not None and abs(a - b) < eps


def loss(n, lots, spec, sl_pips):
    """What `n` orders of `lots` lose together at a stop `sl_pips` away."""
    return n * lots * spec.pip_value_per_lot * sl_pips


def points(sl_pips, spec):
    """sl_pips in points, and whether that is a whole number of them."""
    p = sl_pips * spec.pip / spec.point
    return round(p), abs(p - round(p)) < 1e-6


# ── bot/hold.py ────────────────────────────────────────────────
X, Y, MP = 30, 120, 0.1
HOLD_TABLE = [
    # (what, (age, hold, max_hold, profit, min_profit, against), expected)
    ("before X, in loss", (10, X, Y, -5.0, MP, False), None),
    ("before X, in profit: not yet", (10, X, Y, 5.0, MP, False), None),
    ("at X, in profit", (X, X, Y, 1.0, MP, False), "profit"),
    ("at X, in loss: kept", (X, X, Y, -1.0, MP, False), None),
    ("between X and Y, still in loss", (60, X, Y, -0.5, MP, False), None),
    ("between X and Y, turned profitable", (61, X, Y, 0.5, MP, False), "profit"),
    ("between X and Y, above 0 but under min_profit", (61, X, Y, 0.05, MP, False), None),
    ("against before X", (5, X, Y, -2.0, MP, True), "against"),
    ("against before X, even in profit", (5, X, Y, 2.0, MP, True), "against"),
    ("against between X and Y, in loss", (40, X, Y, -2.0, MP, True), "against"),
    ("against after X, in profit: closed as a profit", (40, X, Y, 2.0, MP, True), "profit"),
    ("against past Y, in loss: against first", (200, X, Y, -2.0, MP, True), "against"),
    ("just before Y, in loss", (119.9, X, Y, -3.0, MP, False), None),
    ("at Y, in loss", (Y, X, Y, -3.0, MP, False), "max"),
    ("past Y, in loss", (500, X, Y, -3.0, MP, False), "max"),
    ("legacy Y = X: just before X", (29.9, X, X, -1.0, MP, False), None),
    ("legacy Y = X: at X in loss, closed", (X, X, X, -1.0, MP, False), "max"),
    ("legacy Y = X: at X in profit", (X, X, X, 1.0, MP, False), "profit"),
    ("Y missing (None) = X: before X", (29, X, None, -1.0, MP, False), None),
    ("Y missing (None) = X: at X", (X, X, None, -1.0, MP, False), "max"),
    ("Y under X counts as X: past Y, before X", (15, X, 10, -1.0, MP, False), None),
    ("Y under X counts as X: at X", (X, X, 10, -1.0, MP, False), "max"),
    # Y = X is v1.2.0 (a stored user or a plan with no Y: Y follows X): `against` is not heeded
    ("legacy Y = X: against before X, kept", (5, X, X, -2.0, MP, True), None),
    ("Y missing (None): against before X, kept", (5, X, None, -2.0, MP, True), None),
    ("Y under X: against before X, kept", (5, X, 10, -2.0, MP, True), None),
    ("legacy Y = X: against at X, closed as max", (X, X, X, -2.0, MP, True), "max"),
    ("Y missing (None): against at X in profit", (X, X, None, 2.0, MP, True), "profit"),
    ("Y one second above X: against heeded", (5, X, X + 1, -2.0, MP, True), "against"),
    ("profit exactly min_profit", (X, X, Y, MP, MP, False), "profit"),
    ("profit a hair under min_profit", (X, X, Y, MP - 1e-9, MP, False), None),
    ("profit unknown (None): not in profit", (60, X, Y, None, MP, False), None),
    ("profit unknown (None) at Y", (Y, X, Y, None, MP, False), "max"),
    ("profit NaN: not in profit", (60, X, Y, NAN, MP, False), None),
    ("min_profit 0: break-even counts", (X, X, Y, 0.0, 0.0, False), "profit"),
]


@scenario
def hold_table():
    for what, args, want in HOLD_TABLE:
        got = hold_action(*args)
        check(got == want, f"{what}: hold_action{args} = {got!r}, want {want!r}")


@scenario
def hold_legacy_and_path():
    # v1.2.0: Y = X (or Y missing, or under X) closes every trade at exactly X, win or lose, never before, whatever
    # the signal says
    for y in (X, None, 10):
        for against in (False, True):
            for age in [0, 1, 10, 29, 29.999, 30, 30.001, 45, 600]:
                for profit in (-50.0, -0.01, 0.0, 0.05, 0.1, 7.0):
                    got = hold_action(age, X, y, profit, MP, against)
                    check((got is not None) == (age >= X) and got != "against",
                          f"legacy: Y {y}, against {against}, age {age}, profit {profit}: {got!r}")
    # one trade's P/L second by second: in loss until 44 s, in profit from 45 s: the first answer is a profit at 45 s
    first = next((age, a) for age in range(0, 200)
                 if (a := hold_action(age, X, Y, -1.0 if age < 45 else 0.3, MP, False)))
    check(first == (45, "profit"), f"the first close of a trade turning profitable after X: {first}")
    # never profitable: closed at Y
    first = next((age, a) for age in range(0, 200) if (a := hold_action(age, X, Y, -1.0, MP, False)))
    check(first == (Y, "max"), f"never profitable: closed at Y: {first}")
    # the signal turns at 12 s: closed then, before X
    first = next((age, a) for age in range(0, 200) if (a := hold_action(age, X, Y, -1.0, MP, age >= 12)))
    check(first == (12, "against"), f"turned against at 12 s: {first}")


@scenario
def min_profit_per_symbol():
    check(near(min_profit(EURUSD, 0.10), 0.10, 1e-12),
          f"EURUSD 0.10 lot: a point is 0.10 $: {min_profit(EURUSD, 0.10)}")
    check(near(min_profit(EURUSD, 1.0), 1.0, 1e-12), "EURUSD 1 lot: 1 $")
    check(near(min_profit(USDJPY, 0.10), 0.067, 1e-12), f"USDJPY 0.10 lot: 0.067 $: {min_profit(USDJPY, 0.10)}")
    check(near(min_profit(XAUUSD, 0.10), 0.10, 1e-12), f"XAUUSD 0.10 lot: a cent of gold is 0.10 $: "
                                                       f"{min_profit(XAUUSD, 0.10)}")
    check(near(min_profit(XAUUSD, 2.5), 2.5, 1e-12), "XAUUSD 2.5 lots: 2.5 $")
    check(min_profit(EURUSD, 0) == 0 and min_profit(EURUSD, -1) == 0 and min_profit(EURUSD, NAN) == 0,
          "no lots: 0")
    check(min_profit(SymbolSpec("BAD", pip=PIP, pip_value_per_lot=10.0, point=0.0), 1.0) == 0
          and min_profit(SymbolSpec("BAD", pip=0.0, pip_value_per_lot=10.0), 1.0) == 0, "no point or pip: 0")
    # a broken spec with a negative pip value: 0, never a negative buffer (a loss after X would then close as "profit")
    bad = SymbolSpec("B", pip=PIP, pip_value_per_lot=-10.0)
    check(min_profit(bad, 1.0) == 0.0 and hold_action(X, X, Y, -0.5, min_profit(bad, 1.0), False) is None,
          f"a negative pip value never makes a loss count as profit: {min_profit(bad, 1.0)}")
    # what it is for: a scalp of 0.10 lot showing 0.05 $ after X is not yet in profit, one showing 0.10 $ is
    mp = min_profit(EURUSD, 0.10)
    check(hold_action(X, X, Y, 0.05, mp, False) is None and hold_action(X, X, Y, 0.10, mp, False) == "profit",
          "min_profit is the buffer hold_action uses")


@scenario
def news_against_sides():
    mid = 1.10000
    # (bid, ask) quotes 0.2 pip wide around a mid
    check(news_against("buy", 1.09989, 1.09991, mid) and news_against("buy", mid, mid, mid),
          "buy: the mid at or under the pre-release mid: against")
    check(not news_against("buy", 1.10009, 1.10011, mid), "buy: the mid above it: still with it")
    check(news_against("sell", 1.10009, 1.10011, mid) and news_against("sell", mid, mid, mid),
          "sell: the mid at or over the pre-release mid: against")
    check(not news_against("sell", 1.09989, 1.09991, mid), "sell: the mid under it: still with it")
    # an unknown pre-release mid (the connector may store 0.0 for "no quote"): never against, never an exception.
    # A 0 taken as a price would close every sell at once (any mid is >= 0)
    for pre in (None, 0.0, -1.0, NAN, INF, "x", True, 10 ** 400):
        check(not news_against("buy", 1.0, 1.0, pre) and not news_against("sell", 2.0, 2.0, pre),
              f"pre-release mid {pre!r}: never")
    for bid, ask in ((None, 1.1), (1.1, None), (None, None), (0.0, 1.1), (1.1, 0.0), (NAN, 1.1), (1.1, INF),
                     ("x", 1.1), (True, 1.1), (10 ** 400, 1.1), (1.1, -1.1)):
        check(not news_against("buy", bid, ask, 1.2) and not news_against("sell", bid, ask, 1.0),
              f"no usable quote ({bid!r}, {ask!r}): never")
    check(not news_against("BUY?", 1.0, 1.0, mid) and not news_against(None, 1.0, 1.0, mid), "an unknown side: never")
    check(news_against("sell", 2400.10, 2400.20, 2400.10) and not news_against("buy", 2400.10, 2400.20, 2400.10),
          "gold, 2 digits")
    # the spread widens at the release while the market keeps going the trade's way: NOT against. EURUSD, offset
    # 2 pips: quoted 1.10000 / 1.10002 before (pre_mid 1.10001); the buy fills at ask 1.10017 with bid 1.09996 (a
    # 2.1 pip spread): the bid is under the pre-release mid, the mid (1.100065) is above it
    check(not news_against("buy", 1.09996, 1.10017, 1.10001),
          "buy: a bid under the pre-release mid on the spread's widening alone: still with it")
    check(not news_against("sell", 1.09985, 1.10006, 1.10001),
          "sell: an ask over the pre-release mid on the spread's widening alone: still with it")
    # ... and hold_action does not close it at its fill (Y above X: against would be heeded)
    check(hold_action(0, X, Y, -2.1, MP, news_against("buy", 1.09996, 1.10017, 1.10001)) is None,
          "a fill at the widest spread is not closed as against at age 0")


# ── bot/risk.py ────────────────────────────────────────────────
@scenario
def money_for_units():
    check(MAX_RISK_PCT == 10.0, f"the cap these checks assume: {MAX_RISK_PCT}")
    check(near(money_for("pct", 2, 10_000), 200), "pct: 2 % of 10 000 = 200")
    check(near(money_for("pct", 0.1, 10_000), 10), "pct: 0.1 %")
    check(near(money_for("usd", 150, 10_000), 150), "usd: the amount as it is")
    check(near(money_for("pct", 10, 10_000), 1000) and near(money_for("pct", 25, 10_000), 1000),
          "pct: capped at MAX_RISK_PCT % of the balance")
    check(near(money_for("usd", 5000, 1000), 100), "usd: capped at MAX_RISK_PCT % of the balance too")
    check(near(money_for("usd", 50, 1000), 50), "usd under the cap: kept")
    for junk in (NAN, INF, -INF, -1, 0, None, "abc", True, [], {}):
        check(money_for("pct", junk, 10_000) == 0 and money_for("usd", junk, 10_000) == 0, f"junk value {junk!r}: 0")
    for bal in (0, -5, NAN, INF, None, "x"):
        check(money_for("pct", 2, bal) == 0 and money_for("usd", 100, bal) == 0, f"junk balance {bal!r}: 0")
    for unit in ("eur", "PCT", "", None, "pips"):
        check(money_for(unit, 2, 10_000) == 0, f"unknown unit {unit!r}: 0")


@scenario
def money_stop_basic():
    # the user's example: 2 % of 10 000 = 200 $ over 10 orders of 0.10 lot -> 200 / (10 × 0.10 × 10 $) = 20 pips
    money = money_for("pct", 2, 10_000)
    floor = stop_floor_pips(EURUSD, 0.8, 1.5)
    sl, lots = money_stop(money, 0.10, 10, EURUSD, floor, 200)
    check((sl, lots) == (20.0, 0.10), f"EURUSD 200 $ / 10 x 0.10: 20 pips: {(sl, lots)}")
    check(near(loss(10, lots, EURUSD, sl), 200), "every order at its stop loses exactly the money")
    check(near(sl * EURUSD.pip, 0.002, 1e-12), "the price distance: 0.00200")
    # one order of 2 lots, 100 $: 5 pips
    check(money_stop(100, 2.0, 1, EURUSD, 3, 200) == (5.0, 2.0), "one order of 2 lots, 100 $: 5 pips")


@scenario
def money_stop_rounds_down_to_the_point():
    for money, want in ((203.45, 20.3), (199.99, 19.9), (200.0099, 20.0), (200.01, 20.0), (201.0, 20.1)):
        sl, lots = money_stop(money, 0.10, 10, EURUSD, 3, 200)
        pts, whole = points(sl, EURUSD)
        check(near(sl, want, 1e-12) and lots == 0.10, f"{money} $: {want} pips: {(sl, lots)}")
        check(whole, f"{money} $: {sl} pips is a whole number of points")
        check(loss(10, lots, EURUSD, sl) <= money, f"{money} $: never above the money")
        check(loss(10, lots, EURUSD, sl + 0.1) > money, f"{money} $: one point more would be above it")
    # a whole-point stop whose quotient floats a hair under the point (12 / (3 × 0.10 × 10) × 10 = 39.99999999999999,
    # 14 / (0.07 × 10) × 10 = 199.99999999999997) is that point, not one less
    got = money_stop(12, 0.10, 3, EURUSD, 0, None)
    check(got == (4.0, 0.10), f"12 $ over 3 x 0.10 lot: 4.0 pips, not 3.9: {got}")
    got = money_stop(14, 0.07, 1, EURUSD, 0, None)
    check(got == (20.0, 0.07), f"14 $ on 0.07 lot: 20.0 pips, not 19.9: {got}")


@scenario
def money_stop_floor():
    # 20 $ over 10 x 0.10 lot is a 2 pip stop: under a 4.5 pip floor -> 4.5 pips, the lots cut to 0.04 (18 $)
    sl, lots = money_stop(20, 0.10, 10, EURUSD, 4.5, 200)
    check((sl, lots) == (4.5, 0.04), f"floor: the stop is the floor, the lots are cut: {(sl, lots)}")
    check(loss(10, lots, EURUSD, sl) <= 20 and loss(10, lots + 0.01, EURUSD, sl) > 20,
          "cut DOWN to the step, never above the money")
    # a floor between two points is rounded UP (the stop may not sit inside it)
    sl, lots = money_stop(20, 0.10, 10, EURUSD, 4.53, 200)
    check(near(sl, 4.6, 1e-12) and points(sl, EURUSD)[1] and lots == 0.04, f"floor 4.53 -> 4.6 pips: {(sl, lots)}")
    check(loss(10, lots, EURUSD, sl) <= 20, "floor rounded up: still never above the money")
    # cut under volume_min: lots 0, the caller skips
    check(money_stop(2, 0.10, 10, EURUSD, 4.5, 200) == (4.5, 0.0), "cut under volume_min: lots 0 (skip)")
    check(money_stop(0.4, 0.10, 1, EURUSD, 4.5, 200) == (4.5, 0.0), "one order, 0.4 $ at a 4.5 pip floor: skip")
    coarse = SymbolSpec("XAUUSD", pip=0.1, pip_value_per_lot=10.0, digits=2, point=0.01, volume_min=0.1,
                        volume_step=0.1)
    check(money_stop(200, 0.5, 10, coarse, 45, 1000) == (45.0, 0.0), "coarse gold: 0.04 lot is under its 0.1 minimum")
    # a stop that lands exactly on the floor is not cut
    check(money_stop(45, 0.10, 10, EURUSD, 4.5, 200) == (4.5, 0.10), "exactly on the floor: lots kept")
    # floor 0 still means one point at least
    check(money_stop(0.01, 0.10, 10, EURUSD, 0, 200) == (0.1, 0.0), "under one point: the stop is one point")


@scenario
def money_stop_cap():
    sl, lots = money_stop(5000, 0.10, 10, EURUSD, 3, 200)
    check((sl, lots) == (200.0, 0.10), f"cap: the stop is the cap: {(sl, lots)}")
    check(loss(10, lots, EURUSD, sl) < 5000, "cap: the run risks less than the money")
    check(money_stop(5000, 0.10, 10, EURUSD, 3, 33.33) == (33.3, 0.10), "a cap between two points: rounded DOWN")
    check(money_stop(5000, 0.10, 10, EURUSD, 3, None) == (500.0, 0.10), "no cap (None)")
    check(money_stop(5000, 0.10, 10, EURUSD, 3, 0) == (500.0, 0.10), "no cap (0)")
    check(money_stop(5000, 0.10, 10, EURUSD, 4.5, 3) == (4.5, 0.10), "a cap under the floor gives way to the floor")
    check(money_stop(40, 0.10, 10, EURUSD, 4.5, 3) == (4.5, 0.08), "... and the lots are still cut to the money")
    # a cap built as a product carries float noise (the Rapid TP clamp: sl_pips x 3): 0.7 x 3 = 2.0999999999999996
    # pips is the 21 point cap it means, not 20; on 6 digits 0.29 pip is 28.999999999999996 points, still 29
    check(0.7 * 3 < 2.1 and 0.29 * 100 < 29, "the noise this guards against is there (0.7 x 3 < 2.1, 0.29 x 100 < 29)")
    got = money_stop(5000, 0.10, 10, EURUSD, 0, 0.7 * 3)
    check(got == (2.1, 0.10), f"a cap of 2.0999999999999996 pips is 21 points, not 20: {got}")
    six = SymbolSpec("EURUSD", pip=PIP, pip_value_per_lot=10.0, digits=6, point=0.000001)
    got = money_stop(5000, 0.10, 10, six, 0, 0.29)
    check(got == (0.29, 0.10), f"6 digits: a cap of 0.29 pip is 29 points, not 28: {got}")


@scenario
def money_stop_symbols():
    # gold: pip 0.1, 10 $ a pip a lot: 200 $ over 10 x 0.10 is 20 pips = 2.00 $ of price (200 points)
    sl, lots = money_stop(200, 0.10, 10, XAUUSD, 3, 200 * XAUUSD.scale)
    check((sl, lots) == (20.0, 0.10) and near(sl * XAUUSD.pip, 2.0, 1e-12) and points(sl, XAUUSD) == (200, True),
          f"XAUUSD: 20 pips, 2.00 of price: {(sl, lots)}")
    # with gold's real floor: freeze 20 points (2 pips), a 1.5 pip spread under a 3 pip × scale 5 cap -> 3 × 15 = 45
    floor = stop_floor_pips(XAUUSD, 1.5, 3 * XAUUSD.scale)
    check(near(floor, 45), f"gold's floor: 45 pips: {floor}")
    sl, lots = money_stop(200, 0.10, 10, XAUUSD, floor, 200 * XAUUSD.scale)
    check((sl, lots) == (45.0, 0.04) and loss(10, lots, XAUUSD, sl) <= 200, f"gold at its floor: {(sl, lots)}")
    # USDJPY, 6.7 $ a pip a lot: 200 / 6.7 = 29.85 pips -> 29.8 (298 points), 199.66 $
    sl, lots = money_stop(200, 0.10, 10, USDJPY, 3, 200)
    check((sl, lots) == (29.8, 0.10) and points(sl, USDJPY) == (298, True), f"USDJPY: 29.8 pips: {(sl, lots)}")
    check(near(loss(10, lots, USDJPY, sl), 199.66) and loss(10, lots, USDJPY, sl + 0.1) > 200,
          "USDJPY: the largest stop in points under the money")


@scenario
def money_stop_lots_and_junk():
    small_max = SymbolSpec("EURUSD", pip=PIP, pip_value_per_lot=10.0, volume_max=0.05)
    check(money_stop(200, 0.10, 10, small_max, 3, 200) == (20.0, 0.10), "above volume_max: kept (the caller splits)")
    check(money_stop(30_000, 150.0, 1, EURUSD, 3, 200) == (20.0, 150.0), "150 lots on a 100 lot max: kept")
    sl, lots = money_stop(15, 150.0, 1, small_max, 3, 200)
    check((sl, lots) == (3.0, 0.5), f"a cut from above volume_max: to the money, not to volume_max: {(sl, lots)}")
    check(money_stop(200, 0.105, 10, EURUSD, 3, 200) == (20.0, 0.10), "lots rounded DOWN to the step first")
    check(money_stop(200, 0.019, 10, EURUSD, 3, 200) == (200.0, 0.01), "0.019 -> 0.01 lot (its stop is the cap)")
    check(money_stop(200, 0.005, 10, EURUSD, 3, 200) == (0.0, 0.0), "a lot under volume_min: skip")
    # common lots whose quotient floats a hair under the step (0.29 / 0.01 = 28.999999999999996) are kept, not cut
    for lot in (0.29, 0.57, 0.58, 1.13):
        check(lot / 0.01 < round(lot / 0.01), f"the noise this guards against is there: {lot} / 0.01 = {lot / 0.01!r}")
        got = money_stop(1000 * lot, lot, 10, EURUSD, 3, 200)
        check(got == (10.0, lot), f"{lot} lot is kept, not cut a step by float noise: {got}")
    for args in ((0, 0.1, 10), (-1, 0.1, 10), (NAN, 0.1, 10), (INF, 0.1, 10), (None, 0.1, 10), (200, 0, 10),
                 (200, -0.1, 10), (200, NAN, 10), (200, 0.1, 0), (200, 0.1, -3), (200, 0.1, NAN), ("x", 0.1, 10),
                 (200, 0.1, 0.5), (200, 0.1, True), (200, 0.1, "x")):
        check(money_stop(*args, EURUSD, 3, 200) == (0.0, 0.0), f"junk {args}: (0, 0)")
    for bad in (SymbolSpec("B", pip=PIP, pip_value_per_lot=0.0), SymbolSpec("B", pip=0.0, pip_value_per_lot=10.0),
                SymbolSpec("B", pip=PIP, pip_value_per_lot=10.0, point=0.0),
                SymbolSpec("B", pip=PIP, pip_value_per_lot=10.0, volume_step=0.0)):
        check(money_stop(200, 0.1, 10, bad, 3, 200) == (0.0, 0.0), f"a broken spec: (0, 0): {bad}")
    check(money_stop(200, 0.1, 10, EURUSD, NAN, INF) == (20.0, 0.10), "junk floor / cap: none")
    # a value too big for a float is junk, not an exception
    check(money_for("usd", 10 ** 400, 10_000) == 0.0 and money_for("pct", 2, 10 ** 400) == 0.0,
          "an int too big for a float: 0")
    # a money so large the stop overflows: kept finite (the cap at 1e12 points), no OverflowError
    sl, lots = money_stop(1e308, 0.01, 1, EURUSD, 3, None)
    check(math.isfinite(sl) and sl > 0 and lots == 0.01, f"a huge money without a cap: a finite stop: {(sl, lots)}")
    # volume_min 0 and a lot under one step: rounded down to 0 lots, a skip, not a division by zero
    zero_min = SymbolSpec("EURUSD", pip=PIP, pip_value_per_lot=10.0, volume_min=0.0)
    check(money_stop(200, 0.005, 10, zero_min, 3, 200) == (0.0, 0.0), "volume_min 0, a lot under one step: skip")
    # a 6-digit quote: pip / point = 100.00000000000001 in floats, still 100 points a pip, so 20 pips stays 20.0
    six = SymbolSpec("EURUSD", pip=PIP, pip_value_per_lot=10.0, digits=6, point=0.000001)
    got = money_stop(200, 0.10, 10, six, 3, 200)
    check(got == (20.0, 0.10), f"6 digits: 20.0 pips, not 19.999999999999996: {got}")
    check(money_stop(200.5, 0.10, 10, six, 3, 200) == (20.05, 0.10), "6 digits: rounded DOWN to its own point")


@scenario
def stop_floor():
    s = SymbolSpec("EURUSD", pip=PIP, pip_value_per_lot=10.0, min_stop_distance=5 * 0.00001)
    check(near(stop_floor_pips(s, 0.8, 1.5), 4.5), "three times the spread cap (above the spread)")
    check(near(stop_floor_pips(s, 2.0, 1.5), 6.0), "three times the spread when it is above its cap")
    check(near(stop_floor_pips(SymbolSpec("EURUSD", pip=PIP, pip_value_per_lot=10.0, min_stop_distance=0.001), 0.8,
                               1.5), 10.0), "the stops level when it is the largest")
    check(near(stop_floor_pips(XAUUSD, 0.3, 0.5), 2.0), "gold: the freeze level (20 points = 2 pips) when largest")
    check(near(stop_floor_pips(XAUUSD, 1.5, 15), 45.0), "gold: 3 x its scaled cap")
    check(near(stop_floor_pips(s, None, 1.5), 4.5) and near(stop_floor_pips(s, 0.8, None), 2.4),
          "an unknown spread or cap counts 0")
    check(near(stop_floor_pips(s, None, None), 0.5) and near(stop_floor_pips(s, "x", NAN), 0.5),
          "nothing known: the stops level")
    check(stop_floor_pips(EURUSD, -3, -1) == 0.0, "negative spreads: 0")
    check(stop_floor_pips(SymbolSpec("B", pip=0.0, pip_value_per_lot=10.0), 1, 1) == 0.0, "no pip: 0")
    # money_stop takes the floor exactly, even with the float noise of stops level / pip
    check(money_stop(0.4, 0.10, 1, s, stop_floor_pips(s, None, None), 200) == (0.5, 0.08),
          f"a 5 point floor is 5 points, not 6: {money_stop(0.4, 0.10, 1, s, stop_floor_pips(s, None, None), 200)}")
    # ... noise that is really there: a 7 point stops level is 7.000000000000001 points, a 2.1 pip spread cap a
    # floor of 6.300000000000001 pips; rounding either UP a whole point would cut the lots harder than needed
    s7 = SymbolSpec("EURUSD", pip=PIP, pip_value_per_lot=10.0, min_stop_distance=7 * 0.00001)
    check(stop_floor_pips(s7, None, None) * 10 > 7, "the noise this guards against is there (7 points > 7)")
    got = money_stop(0.4, 0.10, 1, s7, stop_floor_pips(s7, None, None), 200)
    check(got[0] == 0.7, f"a 7 point stops level is a 7 point stop, not 8: {got}")
    check(stop_floor_pips(EURUSD, None, 2.1) > 6.3, "the noise is there (3 x 2.1 > 6.3)")
    got = money_stop(20, 0.10, 10, EURUSD, stop_floor_pips(EURUSD, None, 2.1), 200)
    check(got[0] == 6.3, f"a 2.1 pip spread cap is a 6.3 pip floor, not 6.4: {got}")


@scenario
def money_stop_property():
    """Random money, lots, orders, floors, caps and specs: n × lots × pip value × sl never above the money; sl a whole
    number of points, never under the floor; lots on the step, at least volume_min and never above what was asked;
    and the answer tight: one point more (or one step more after a cut) would be above the money."""
    rnd = random.Random(130)
    seen = {"plain": 0, "floor": 0, "cut": 0, "cap": 0, "skip": 0}
    for i in range(30_000):
        base = rnd.choice((EURUSD, USDJPY, XAUUSD))
        step = rnd.choice((0.01, 0.1, 0.001))
        spec = SymbolSpec(base.symbol, pip=base.pip, pip_value_per_lot=rnd.choice((base.pip_value_per_lot,
                          rnd.uniform(0.3, 25))), digits=base.digits, point=base.point,
                          volume_min=round(step * rnd.randint(1, 3), 8), volume_step=step,
                          volume_max=rnd.choice((0.5, 5.0, 100.0)))
        money = 10 ** rnd.uniform(-2, 5.5)
        lots_in = 10 ** rnd.uniform(-3, 2.3)
        n = rnd.randint(1, 40)
        floor = rnd.choice((0, rnd.uniform(0, 60), round(rnd.uniform(0, 60), 1)))
        cap = rnd.choice((None, 0, rnd.uniform(0.05, 600), float(rnd.randint(1, 600))))
        sl, lots = money_stop(money, lots_in, n, spec, floor, cap)
        what = f"#{i} money_stop({money}, {lots_in}, {n}, {spec}, {floor}, {cap}) = {(sl, lots)}"
        ppp = spec.pip / spec.point
        norm = round(math.floor(lots_in / step + 1e-9) * step, 8)
        if norm < spec.volume_min:
            check((sl, lots) == (0.0, 0.0), f"lot under volume_min: skip: {what}")
            seen["skip"] += 1
            continue
        pts, whole = points(sl, spec)
        lo = max(1, math.ceil(floor * ppp - 1e-9))
        hi = max(1, math.floor(cap * ppp + 1e-9)) if cap else None
        check(whole and pts >= lo, f"whole points, not under the floor: {what}")
        check(hi is None or pts <= max(hi, lo), f"not over the cap (unless the floor is above it): {what}")
        if lots == 0:
            check(pts == lo and loss(n, spec.volume_min, spec, sl) > money * (1 - 1e-9),
                  f"skipped only when even volume_min at the floor is too much: {what}")
            seen["skip"] += 1
            continue
        check(loss(n, lots, spec, sl) <= money * (1 + 1e-9), f"NEVER above the money: {what}")
        steps = lots / step
        check(abs(steps - round(steps)) < 1e-6 and lots >= spec.volume_min - 1e-12 and lots <= norm + 1e-12,
              f"lots on the step, at least volume_min, at most what was asked: {what}")
        if lots < norm:
            check(pts == lo and loss(n, lots + step, spec, sl) > money * (1 - 1e-9),
                  f"cut only at the floor, and DOWN by less than one step: {what}")
            seen["cut"] += 1
        elif pts == lo and lo > 1:
            seen["floor"] += 1
        elif hi is not None and pts == hi:
            seen["cap"] += 1
        else:
            check(loss(n, lots, spec, (pts + 1) / ppp) > money * (1 - 1e-9),
                  f"the largest whole point under the money: {what}")
            seen["plain"] += 1
    # "floor" without a cut is rare: it needs a cap under the floor and a money above the floor
    check(all(v >= 50 for v in seen.values()), f"every branch exercised: {seen}")


# ── runner ─────────────────────────────────────────────────────
def main() -> int:
    failed = 0
    for fn in TESTS:
        t0 = time.monotonic()
        try:
            fn()
            print(f"PASS {fn.__name__} ({time.monotonic() - t0:.2f}s)")
        except Exception as e:
            failed += 1
            print(f"FAIL {fn.__name__}: {e}")
            traceback.print_exc()
    return failed


if __name__ == "__main__":
    picked = [fn for fn in TESTS if fn.__name__ in sys.argv[1:]]
    TESTS[:] = picked or TESTS
    failures = main()
    print(f"\n{len(TESTS) - failures}/{len(TESTS)} scenarios passed")
    sys.exit(1 if failures else 0)
