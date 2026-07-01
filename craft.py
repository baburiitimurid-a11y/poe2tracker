#!/usr/bin/env python3
"""
craft.py — оценка ОЖИДАЕМОЙ стоимости крафта популярных вещей (модуль poe2tracker).

Почему Монте-Карло, а не формула:
  В PoE2 0.5 крафт смешанный — часть шагов детерминированная (эссенция гарантирует мод,
  руна = сокет), часть вероятностная (exalt-слэм добавляет СЛУЧАЙНЫЙ мод из пула, annul
  удаляет случайный). Когда стратегия включает «слэмим, пока не выпадет нужный мод, иначе
  annul/рестарт» — точной замкнутой формулы нет из-за брикинга и порядка. Симуляция это
  честно моделирует и даёт не только среднее, но и p50/p90 (что важнее среднего: хвост
  дорогих неудач и есть реальный риск).

Цены валют берутся из БД трекера (та же SQLite), поэтому оценка — в текущих ценах рынка.

Модель стратегии (упрощённая, но честная):
  base -> [essence: гарантирует mod'ы из essence_mods] -> exalt-слэмы по открытым слотам,
  цель — добить нужные target-моды. Неудачный слэм занимает слот «мусором». Если слоты
  кончились без всех target — annul (до max_annuls) и продолжаем; если и annul не помог —
  рестарт (докупаем base). Omen of Sinistral/Dextral и т.п. можно смоделировать через
  сдвиг вероятности p_hit (параметр).
"""

import random
import sqlite3
import statistics
from dataclasses import dataclass, field


# ---- цены из БД ------------------------------------------------------------

def price_lookup(con, league):
    """Возвращает функцию name->цена(exalt) по последним снимкам. Терпима к неточным именам."""
    rows = con.execute(
        "SELECT item_name, price FROM snapshots WHERE league=? AND price IS NOT NULL "
        "AND id IN (SELECT MAX(id) FROM snapshots WHERE league=? GROUP BY item_id)",
        (league, league)).fetchall()
    table = {r[0].lower(): r[1] for r in rows}
    def get(name, default=None):
        n = name.lower()
        if n in table:
            return table[n]
        for k, v in table.items():          # частичное совпадение
            if n in k or k in n:
                return v
        return default
    return get


# ---- описание рецепта ------------------------------------------------------

@dataclass
class CraftPlan:
    base_name: str                 # что за база (для цены) — напр. "Warmonger Bow" (rare base)
    base_cost: float               # цена чистой базы в exalt (если 0 — берём из price_lookup)
    affix_slots: int = 6           # сколько всего аффикс-слотов у базы (напр. 3 pref + 3 suf)
    essence_mods: int = 1          # сколько модов гарантирует стартовая эссенция
    essence_name: str = "Essence"  # имя для цены
    target_mods: int = 3           # сколько НУЖНЫХ модов всего надо (вкл. essence_mods)
    p_hit: float = 0.08            # вероятность, что один exalt-слэм даст нужный мод
    max_annuls: int = 2            # сколько annul пробуем перед рестартом
    p_annul_good: float = 0.5      # шанс, что annul снимет «мусорный» (не нужный) мод
    prices: dict = field(default_factory=dict)  # переопределение цен валют


DEFAULT_PRICES = {   # запасные цены в exalt, если в БД нет (грубо; реальные придут из БД)
    "exalted orb": 1.0, "regal orb": 0.4, "chaos orb": 0.02,
    "orb of annulment": 3.0, "essence": 0.5, "divine orb": 200.0,
}


def _price(plan, con_get, name):
    if name.lower() in plan.prices:
        return plan.prices[name.lower()]
    if con_get:
        v = con_get(name)
        if v is not None:
            return v
    return DEFAULT_PRICES.get(name.lower(), 1.0)


# ---- одна попытка крафта (симуляция) ---------------------------------------

def _one_attempt(plan, cost):
    """Моделирует крафт одной базы 'с нуля'. Возвращает True если добили target,
    аккумулируя стоимость в dict cost. Может вызываться повторно при рестарте."""
    px = cost["_px"]
    cost["base"] += px("base")
    filled = 0            # занятые слоты
    good = 0             # из них нужные
    # эссенция гарантирует essence_mods нужных
    if plan.essence_mods > 0:
        cost["essence"] += px("essence")
        good += min(plan.essence_mods, plan.target_mods)
        filled += plan.essence_mods
    annuls_left = plan.max_annuls
    # добиваем оставшиеся нужные моды exalt-слэмами
    while good < plan.target_mods:
        if filled >= plan.affix_slots:
            # слоты кончились — пробуем annul снять мусор
            if annuls_left <= 0:
                return False               # брик -> рестарт
            cost["annul"] += px("orb of annulment")
            annuls_left -= 1
            if random.random() < plan.p_annul_good:
                filled -= 1                # сняли мусорный слот
            else:
                good = max(0, good - 1)     # не повезло — сняли нужный
                filled -= 1
            continue
        cost["exalt"] += px("exalted orb")
        filled += 1
        if random.random() < plan.p_hit:
            good += 1
    return True


def simulate(plan, con_get=None, trials=20000, seed=None):
    if seed is not None:
        random.seed(seed)
    px = lambda n: _price(plan, con_get, "base") if n == "base" else _price(plan, con_get, n)
    # разрешаем цену базы: base_cost или из БД по base_name
    base_price = plan.base_cost or (con_get(plan.base_name) if con_get else None) or 1.0
    def price(n):
        return base_price if n == "base" else _price(plan, con_get, n)
    totals = []
    breakdown = {"base": 0.0, "essence": 0.0, "exalt": 0.0, "annul": 0.0}
    for _ in range(trials):
        cost = {"base": 0.0, "essence": 0.0, "exalt": 0.0, "annul": 0.0, "_px": price}
        # рестартуем, пока не добьём (учитываем стоимость каждой запоротой базы)
        while not _one_attempt(plan, cost):
            pass
        cost.pop("_px")
        totals.append(sum(cost.values()))
        for k in breakdown:
            breakdown[k] += cost[k]
    totals.sort()
    n = len(totals)
    return {
        "mean": statistics.mean(totals),
        "p50": totals[n // 2],
        "p90": totals[int(n * 0.9)],
        "p10": totals[int(n * 0.1)],
        "breakdown": {k: v / trials for k, v in breakdown.items()},
        "trials": trials,
    }


def report(plan, res):
    print(f"\n=== Ожидаемая стоимость крафта: {plan.base_name} ===")
    print(f"  цель: {plan.target_mods} нужных мода(ов), p(слэм в нужный)={plan.p_hit:.2f}, "
          f"слотов {plan.affix_slots}, annul<= {plan.max_annuls}")
    print(f"  среднее : {res['mean']:.1f} ex")
    print(f"  медиана : {res['p50']:.1f} ex   (половина крафтов дешевле)")
    print(f"  p90     : {res['p90']:.1f} ex   (1 из 10 крафтов дороже — это и есть риск)")
    print(f"  разбивка (ex): база {res['breakdown']['base']:.1f} · эссенция "
          f"{res['breakdown']['essence']:.2f} · exalt {res['breakdown']['exalt']:.1f} · "
          f"annul {res['breakdown']['annul']:.1f}")


def craft_from_db(db_path, league, plan):
    con = sqlite3.connect(db_path)
    try:
        get = price_lookup(con, league)
        res = simulate(plan, con_get=get)
    finally:
        con.close()
    report(plan, res)
    return res


# ---- selftest --------------------------------------------------------------

def _selftest():
    # Кейс: bow под Lightning Arrow Deadeye. Эссенция даёт 1 нужный, добиваем ещє 2 слэмами.
    plan = CraftPlan(base_name="Warmonger Bow", base_cost=0.5, affix_slots=6,
                     essence_mods=1, target_mods=3, p_hit=0.10, max_annuls=2,
                     prices={"exalted orb": 1.0, "essence": 0.5, "orb of annulment": 3.0})
    res = simulate(plan, trials=20000, seed=42)
    report(plan, res)
    # Санити: среднее должно быть заметно больше стоимости базы, p90 > p50 > 0
    assert res["mean"] > plan.base_cost * 2, res
    assert res["p90"] > res["p50"] > 0, res
    # Чем ниже p_hit, тем дороже — проверим монотонность
    cheap = simulate(CraftPlan(base_name="b", base_cost=0.5, p_hit=0.25, target_mods=3), trials=8000, seed=1)
    dear  = simulate(CraftPlan(base_name="b", base_cost=0.5, p_hit=0.05, target_mods=3), trials=8000, seed=1)
    assert dear["mean"] > cheap["mean"], (cheap["mean"], dear["mean"])
    print(f"\n  монотонность: p_hit 0.25 -> {cheap['mean']:.1f} ex  <  p_hit 0.05 -> {dear['mean']:.1f} ex")
    print("\n✅ craft selftest passed — симуляция и чувствительность к вероятности работают.")


if __name__ == "__main__":
    _selftest()
