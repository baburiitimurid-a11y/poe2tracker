#!/usr/bin/env python3
"""
poe2_builds — модуль анализа билдов для poe2tracker.

Идея:
  1. Тянем build overview с poe.ninja (список асцендансов + их популярность в %).
  2. Для КАЖДОГО асценданса берём столько топ-персонажей, сколько задаёт популярность:
     30% -> top200, 0.1% -> top3 (линейно, с клампом [MIN_N, MAX_N]).
     Логика: у массовых билдов выборка большая (нужна точная статистика по метам),
     у нишевых — маленькая (там и данных мало, и спрос на их шмот низкий).
  3. Из каждого персонажа достаём экипировку: слот -> база + ключевые моды + главный скилл.
  4. Агрегируем по (асцендансу, слоту): какие базы/моды встречаются и в каком % выборки.
     Это и есть «что заранее нужно людям — до баз, модов и слотов».

ВЕЖЛИВОСТЬ К ИСТОЧНИКУ (важно, иначе бан/абьюз):
  - Билды меняются медленно: обновляем раз в BUILDS_REFRESH_HOURS (по умолч. 12ч), НЕ каждые 30 мин.
  - Жёсткий rate-limit и общий потолок MAX_TOTAL_CHARS на прогон.
  - poe.ninja PoE2 пути могут отличаться — вынесены в NINJA_EP, сверь через overview-ответ.
"""

import math
import sqlite3
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone

# ---- конфиг ----------------------------------------------------------------
NINJA_BASE = "https://poe.ninja/api"
# Шаблоны эндпоинтов PoE2 (СВЕРЬ на своей стороне — poe.ninja меняет пути):
NINJA_EP = {
    "overview":  "/poe2/{league}/builds/overview",         # список асцендансов + %
    "character": "/poe2/{league}/builds/character/{cid}",  # детали одного персонажа
}

SCALE = {"pct_at_max": 30.0, "max_n": 200, "min_n": 3}  # 30%->200, 0.1%->3
BUILDS_REFRESH_HOURS = 12
MAX_TOTAL_CHARS = 1500          # потолок запросов персонажей за один прогон
RATE_DELAY = 0.6               # сек между запросами

# ---- масштабирование выборки ----------------------------------------------

def scaled_top_n(pct, s=SCALE):
    """Популярность (%) -> сколько топ-персонажей брать. Линейно к опорным точкам."""
    n = round((pct or 0) / s["pct_at_max"] * s["max_n"])
    return max(s["min_n"], min(s["max_n"], n))

# ---- схема БД --------------------------------------------------------------

def init_builds_db(con):
    con.executescript("""
    CREATE TABLE IF NOT EXISTS build_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, league TEXT
    );
    -- агрегаты: по асцендансу/слоту какая база/мод и в каком проценте выборки
    CREATE TABLE IF NOT EXISTS build_agg (
        ts TEXT, league TEXT, ascendancy TEXT, pct REAL, sample_n INTEGER,
        slot TEXT, kind TEXT,           -- kind: 'base' | 'mod' | 'skill'
        value TEXT, share REAL, count INTEGER
    );
    CREATE INDEX IF NOT EXISTS ix_agg ON build_agg(league, ascendancy, slot, kind);
    """)
    con.commit()

def _stale(con, league):
    r = con.execute(
        "SELECT ts FROM build_runs WHERE league=? ORDER BY ts DESC LIMIT 1", (league,)
    ).fetchone()
    if not r:
        return True
    age_h = (datetime.now(timezone.utc) - datetime.fromisoformat(r[0])).total_seconds()/3600
    return age_h >= BUILDS_REFRESH_HOURS

# ---- извлечение шмота из персонажа ----------------------------------------

def extract_character(char):
    """Возвращает {'skill': str, 'slots': {slot: {'base': str, 'mods': [str,...]}}}.
    Терпимо к разным формам ответа poe.ninja."""
    out = {"skill": None, "slots": {}}
    if not isinstance(char, dict):
        return out
    out["skill"] = char.get("mainSkill") or char.get("skill") or \
                   (char.get("skills") or [None])[0]
    items = char.get("items") or char.get("equipment") or []
    for it in items:
        if not isinstance(it, dict):
            continue
        slot = it.get("slot") or it.get("inventoryId") or it.get("type") or "?"
        base = it.get("baseType") or it.get("base") or it.get("typeLine") or "?"
        mods = []
        for key in ("explicitMods", "explicit", "mods", "modifiers"):
            v = it.get(key)
            if isinstance(v, list):
                mods.extend([m if isinstance(m, str) else (m.get("text") if isinstance(m, dict) else str(m)) for m in v])
        out["slots"][str(slot)] = {"base": str(base), "mods": [str(m) for m in mods if m]}
    return out

# ---- агрегация -------------------------------------------------------------

def aggregate(characters):
    """characters: list[extract_character(...)] -> агрегаты по слотам."""
    n = len(characters)
    slot_bases = defaultdict(Counter)   # slot -> Counter(base)
    slot_mods  = defaultdict(Counter)   # slot -> Counter(mod)
    skills = Counter()
    for ch in characters:
        if ch.get("skill"):
            skills[ch["skill"]] += 1
        for slot, data in ch["slots"].items():
            slot_bases[slot][data["base"]] += 1
            # мод считаем один раз на персонажа (set), иначе дубли в одном слоте раздувают %
            for m in set(data["mods"]):
                slot_mods[slot][m] += 1
    return {"n": n, "slot_bases": slot_bases, "slot_mods": slot_mods, "skills": skills}

def agg_rows(ts, league, asc, pct, agg, top_bases=3, top_mods=5, top_skills=3):
    """Разворачивает агрегаты в строки для build_agg (с долями)."""
    n = max(agg["n"], 1)
    rows = []
    for value, cnt in agg["skills"].most_common(top_skills):
        rows.append((ts, league, asc, pct, agg["n"], "*", "skill", value, cnt/n, cnt))
    for slot, counter in agg["slot_bases"].items():
        for value, cnt in counter.most_common(top_bases):
            rows.append((ts, league, asc, pct, agg["n"], slot, "base", value, cnt/n, cnt))
    for slot, counter in agg["slot_mods"].items():
        for value, cnt in counter.most_common(top_mods):
            rows.append((ts, league, asc, pct, agg["n"], slot, "mod", value, cnt/n, cnt))
    return rows

# ---- сетевой прогон (реальный) --------------------------------------------

def refresh_builds(con, league, http_get, force=False):
    """http_get(url)->dict|None прокидывается из основного трекера (единый rate-limit)."""
    from urllib.parse import quote
    lq = quote(league)
    init_builds_db(con)
    if not force and not _stale(con, league):
        print("[builds] свежие данные есть — пропускаю (см. BUILDS_REFRESH_HOURS)")
        return 0
    ov = http_get(NINJA_BASE + NINJA_EP["overview"].format(league=lq))
    if not ov:
        print("[builds] overview недоступен — сверь NINJA_EP пути")
        return 0
    ascendancies = ov.get("ascendancies") or ov.get("classes") or ov.get("data") or []
    ts = datetime.now(timezone.utc).isoformat()
    total_chars = 0
    all_rows = []
    for a in ascendancies:
        asc = a.get("name") or a.get("ascendancy") or "?"
        pct = float(a.get("percentage") or a.get("pct") or a.get("popularity") or 0)
        n = scaled_top_n(pct)
        if total_chars + n > MAX_TOTAL_CHARS:
            n = max(0, MAX_TOTAL_CHARS - total_chars)
        if n <= 0:
            break
        char_ids = (a.get("characterIds") or a.get("characters") or [])[:n]
        chars = []
        for cid in char_ids:
            cd = http_get(NINJA_BASE + NINJA_EP["character"].format(league=lq, cid=cid))
            if cd:
                chars.append(extract_character(cd))
            total_chars += 1
        agg = aggregate(chars)
        all_rows.extend(agg_rows(ts, league, asc, pct, agg))
        print(f"[builds] {asc}: {pct:.2f}% -> top{n}, собрано {agg['n']} чаров")
    if all_rows:
        con.execute("DELETE FROM build_agg WHERE league=?", (league,))  # держим последний срез
        con.executemany(
            "INSERT INTO build_agg VALUES (?,?,?,?,?,?,?,?,?,?)", all_rows)
        con.execute("INSERT INTO build_runs(ts,league) VALUES (?,?)", (ts, league))
        con.commit()
    return len(all_rows)

# ---- отчёт -----------------------------------------------------------------

def report_builds(con, league, min_share=0.30):
    init_builds_db(con)
    ascs = con.execute(
        "SELECT DISTINCT ascendancy, pct, sample_n FROM build_agg WHERE league=? "
        "ORDER BY pct DESC", (league,)).fetchall()
    if not ascs:
        print("Нет данных билдов. Запусти refresh (--builds)."); return
    for asc, pct, n in ascs:
        print(f"\n=== {asc}  ({pct:.2f}% игроков, выборка top{n}) ===")
        sk = con.execute("SELECT value,share FROM build_agg WHERE league=? AND ascendancy=? "
                         "AND kind='skill' ORDER BY share DESC", (league, asc)).fetchall()
        if sk:
            print("  скилл:", ", ".join(f"{v} {s*100:.0f}%" for v, s in sk))
        slots = con.execute("SELECT DISTINCT slot FROM build_agg WHERE league=? AND ascendancy=? "
                            "AND kind='base'", (league, asc)).fetchall()
        for (slot,) in slots:
            bases = con.execute("SELECT value,share FROM build_agg WHERE league=? AND ascendancy=? "
                               "AND slot=? AND kind='base' ORDER BY share DESC",
                               (league, asc, slot)).fetchall()
            mods = con.execute("SELECT value,share FROM build_agg WHERE league=? AND ascendancy=? "
                              "AND slot=? AND kind='mod' AND share>=? ORDER BY share DESC",
                              (league, asc, slot, min_share)).fetchall()
            btxt = ", ".join(f"{v} {s*100:.0f}%" for v, s in bases)
            print(f"  [{slot}] база: {btxt}")
            for v, s in mods:
                print(f"       └ мод {s*100:.0f}%: {v[:60]}")

# ---- selftest (без сети) ---------------------------------------------------

def _selftest():
    assert scaled_top_n(30) == 200 and scaled_top_n(0.1) == 3 and scaled_top_n(0.05) == 3
    assert scaled_top_n(5) == 33 and scaled_top_n(1) == 7
    # синтетическая выборка: 10 «Deadeye», у 8/10 лук Warmonger Bow с мод'ом +Lightning
    chars = []
    for i in range(10):
        base = "Warmonger Bow" if i < 8 else "Spine Bow"
        mods = ["Adds Lightning Damage", "Increased Attack Speed"] if i < 8 else ["Adds Cold Damage"]
        chars.append({"skill": "Lightning Arrow" if i < 9 else "Ice Shot",
                      "items": [{"slot": "Weapon", "baseType": base, "explicitMods": mods},
                                {"slot": "Helmet", "baseType": "Fanatic Crown",
                                 "explicitMods": ["+% Evasion", "Increased Rarity"]}]})
    extracted = [extract_character(c) for c in chars]
    agg = aggregate(extracted)
    con = sqlite3.connect(":memory:")
    init_builds_db(con)
    ts = datetime.now(timezone.utc).isoformat()
    rows = agg_rows(ts, "TestLeague", "Deadeye", 34.0, agg)
    con.executemany("INSERT INTO build_agg VALUES (?,?,?,?,?,?,?,?,?,?)", rows)
    con.execute("INSERT INTO build_runs(ts,league) VALUES (?,?)", (ts, "TestLeague"))
    con.commit()
    # проверки
    wpn = {v: s for (v, s) in con.execute(
        "SELECT value,share FROM build_agg WHERE slot='Weapon' AND kind='base'").fetchall()}
    assert abs(wpn["Warmonger Bow"] - 0.8) < 1e-6, wpn
    sk = {v: s for (v, s) in con.execute(
        "SELECT value,share FROM build_agg WHERE kind='skill'").fetchall()}
    assert abs(sk["Lightning Arrow"] - 0.9) < 1e-6, sk
    print("Отчёт по синтетике:")
    report_builds(con, "TestLeague", min_share=0.30)
    print("\n✅ builds selftest passed — масштабирование, агрегация баз/модов/слотов работают.")

if __name__ == "__main__":
    _selftest()
