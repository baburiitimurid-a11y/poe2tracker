#!/usr/bin/env python3
"""
poe2tracker — автономный трекер экономики и билдов Path of Exile 2.

Что делает:
  1. Периодически тянет данные с poe2scout (цены + ОБЪЁМ торгов) и с poe.ninja (билды).
  2. Складывает снимки в локальную SQLite (единый файл, никакой инфраструктуры).
  3. По накопленной истории считает выводы: моментум цены, классификацию объёма
     (Busy/Steady/Quiet), всплески (z-score), и «горячие» позиции (объём растёт + цена растёт).
  4. Отправляет отфильтрованные уведомления (консоль + опц. Discord/Telegram).

Источники и почему так:
  - poe2scout: только высоколиквидные предметы (валюта/руны/эссенции/уники). Для «что много
    торгуется» это идеально — там есть volume. gear с модами там пустой (это by design).
  - poe.ninja builds: популярность классов/асцендансов + топ-скиллы. Отсюда — спрос на архетипы,
    что косвенно указывает на востребованные базы/слоты. Точный разбор баз/аффиксов — уже вручную
    по топ-чарам, автоматом ladder такого не отдаёт стабильно.

ВАЖНО ПРО ЛЕГальность:
  - Мы НЕ дёргаем официальное trade API GGG для наполнения БД (это против их rate-limit/ToS и
    путь к бану). Мы читаем агрегаторы, которые уже делают это легально.
  - Никакой автоматизации действий в игре. Только сбор данных и уведомления.
  - poe2scout ТРЕБУЕТ User-Agent с контактом. Заполни CONTACT ниже своим email.

Запуск:
  python3 poe2tracker.py --discover      # показать реальные пути API (сверить на своей машине)
  python3 poe2tracker.py --selftest      # прогнать логику анализа на синтетике (без сети)
  python3 poe2tracker.py --once          # один цикл сбора + анализ + алерты
  python3 poe2tracker.py --run           # автономный режим: цикл каждые POLL_MINUTES минут
  python3 poe2tracker.py --report        # показать текущие выводы из накопленной БД
"""

import argparse
import json
import math
import os
import sqlite3
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone

# ----------------------------------------------------------------------------
# КОНФИГ — правь под себя
# ----------------------------------------------------------------------------

CONTACT = os.environ.get("POE2_CONTACT", "your-email@example.com")  # ОБЯЗАТЕЛЬНО заполни
LEAGUE = os.environ.get("POE2_LEAGUE", "auto")            # 'auto' -> определится сама из /leagues
DB_PATH = os.environ.get("POE2_DB", "poe2tracker.db")
POLL_MINUTES = int(os.environ.get("POE2_POLL_MINUTES", "30"))

# Базы API. Пути вынесены наверх — если poe2scout сменит схему, правишь тут одну строку.
SCOUT_BASE = "https://api.poe2scout.com"
NINJA_BASE = "https://poe.ninja/api"

# Эндпоинты poe2scout (подтверждены по их repo/swagger; на всякий — проверь через --discover).
SCOUT_ENDPOINTS = {
    "leagues": "/leagues",
    "categories": "/items/categories",
    "currency": "/items/currency",     # список валюты с price + volume
    "unique": "/items/unique",         # уники
    "history": "/Items/{item_id}/History",   # ВЕРИФИЦИРОВАНО: заглавные I/H
}

# Какие категории мониторить и с какими порогами.
WATCH = {
    "currency": {"min_volume": 50},
    "unique":   {"min_volume": 5},
}

# Пороги для выводов/алертов.
ALERT_RULES = {
    "momentum_pct": 15.0,   # алерт, если цена сдвинулась > 15% за окно
    "volume_z": 2.0,        # алерт, если объём аномально высок (z-score > 2)
    "price_z": 2.5,         # алерт на ценовой всплеск
    "window": 12,           # сколько последних снимков берём для скользящих статистик
    "min_history": 5,       # не делать выводов, пока меньше N снимков
}

# Каналы уведомлений (опционально; по умолчанию только консоль).
DISCORD_WEBHOOK = os.environ.get("POE2_DISCORD_WEBHOOK", "")
TELEGRAM_TOKEN = os.environ.get("POE2_TELEGRAM_TOKEN", "")
TELEGRAM_CHAT = os.environ.get("POE2_TELEGRAM_CHAT", "")

USER_AGENT = f"poe2tracker/1.0 (contact: {CONTACT})"
RATE_DELAY = 0.6  # сек между запросами -> ~1.6 req/s, ниже лимита 2/s

# ----------------------------------------------------------------------------
# HTTP c вежливым rate-limit
# ----------------------------------------------------------------------------

_last_call = [0.0]

def http_get(url, params=None):
    if params:
        from urllib.parse import urlencode
        url = f"{url}?{urlencode(params)}"
    # простой throttle
    dt = time.time() - _last_call[0]
    if dt < RATE_DELAY:
        time.sleep(RATE_DELAY - dt)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            _last_call[0] = time.time()
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        print(f"[http] {e.code} on {url}", file=sys.stderr)
        if e.code == 429:
            time.sleep(5)
        return None
    except Exception as e:
        print(f"[http] error on {url}: {e}", file=sys.stderr)
        return None

# ----------------------------------------------------------------------------
# БД
# ----------------------------------------------------------------------------

def db():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con

def init_db():
    con = db()
    con.executescript("""
    CREATE TABLE IF NOT EXISTS snapshots (
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        ts        TEXT NOT NULL,
        league    TEXT NOT NULL,
        category  TEXT NOT NULL,
        item_id   TEXT NOT NULL,
        item_name TEXT NOT NULL,
        price     REAL,            -- в reference currency (exalt)
        volume    REAL             -- кол-во листингов/сделок за период
    );
    CREATE INDEX IF NOT EXISTS ix_snap_item ON snapshots(item_id, ts);
    CREATE INDEX IF NOT EXISTS ix_snap_cat  ON snapshots(category, ts);

    CREATE TABLE IF NOT EXISTS builds (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        ts         TEXT NOT NULL,
        league     TEXT NOT NULL,
        class_name TEXT NOT NULL,
        skill      TEXT,
        pct        REAL             -- доля игроков (%)
    );
    CREATE INDEX IF NOT EXISTS ix_build ON builds(class_name, ts);

    CREATE TABLE IF NOT EXISTS alerts_sent (
        key TEXT PRIMARY KEY,
        ts  TEXT
    );
    """)
    con.commit()
    con.close()

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def insert_snapshots(rows):
    con = db()
    con.executemany(
        "INSERT INTO snapshots(ts,league,category,item_id,item_name,price,volume) VALUES (?,?,?,?,?,?,?)",
        rows,
    )
    con.commit(); con.close()

def insert_builds(rows):
    con = db()
    con.executemany(
        "INSERT INTO builds(ts,league,class_name,skill,pct) VALUES (?,?,?,?,?)", rows,
    )
    con.commit(); con.close()

# ----------------------------------------------------------------------------
# Парсеры источников (терпимы к вариациям схемы — берём поля по нескольким именам)
# ----------------------------------------------------------------------------

def _pick(d, *names, default=None):
    for n in names:
        if isinstance(d, dict) and n in d and d[n] is not None:
            return d[n]
    return default

def parse_scout_items(payload, category):
    """poe2scout отдаёт список предметов. Схема слегка гуляет между эндпоинтами,
    поэтому достаём поля устойчиво по нескольким возможным ключам."""
    if payload is None:
        return []
    items = payload
    if isinstance(payload, dict):
        items = _pick(payload, "items", "data", "results", default=[])
    out = []
    for it in items or []:
        item_id = str(_pick(it, "id", "itemId", "apiId", default=_pick(it, "name", "text", default="?")))
        name = str(_pick(it, "name", "text", "currencyName", default=item_id))
        price = _pick(it, "price", "chaosValue", "exaltedValue", "currentPrice", "value")
        volume = _pick(it, "volume", "quantity", "listingCount", "count", default=0)
        try:
            price = float(price) if price is not None else None
            volume = float(volume) if volume is not None else 0.0
        except (TypeError, ValueError):
            continue
        out.append((now_iso(), LEAGUE, category, item_id, name, price, volume))
    return out

_resolved = [False]

def autodetect_league():
    """Если POE2_LEAGUE не задан или ='auto' — сам берём текущую временную лигу из /leagues."""
    global LEAGUE
    if LEAGUE and LEAGUE.lower() != "auto":
        return
    data = http_get(SCOUT_BASE + SCOUT_ENDPOINTS["leagues"])
    leagues = data.get("leagues", data) if isinstance(data, dict) else data
    names = []
    for lg in (leagues or []):
        nm = lg.get("value") or lg.get("id") or lg.get("name") if isinstance(lg, dict) else str(lg)
        if nm:
            names.append(nm)
    # предпочитаем НЕ Standard/Hardcore (это временная лига текущего патча)
    pick = next((n for n in names if n.lower() not in ("standard", "hardcore")), None) \
           or (names[0] if names else "Standard")
    print(f"[league] авто-определена лига: {pick}")
    LEAGUE = pick

def fetch_scout():
    if not _resolved[0]:
        try:
            resolve_endpoints()   # само-сверка путей по openapi при первом запуске
            autodetect_league()   # само-определение текущей лиги
        except Exception as e:
            print(f"[discover] авто-настройка не удалась: {e}", file=sys.stderr)
        _resolved[0] = True
    all_rows = []
    for cat, cfg in WATCH.items():
        ep = SCOUT_ENDPOINTS.get(cat)
        if not ep:
            continue
        payload = http_get(SCOUT_BASE + ep, params={"league": LEAGUE})
        rows = parse_scout_items(payload, cat)
        # фильтр по минимальному объёму на входе
        rows = [r for r in rows if (r[6] or 0) >= cfg.get("min_volume", 0)]
        print(f"[scout] {cat}: {len(rows)} позиций")
        all_rows.extend(rows)
    if all_rows:
        insert_snapshots(all_rows)
    return len(all_rows)

def fetch_ninja_builds():
    """poe.ninja PoE2 builds. Эндпоинт может отличаться — если пусто, просто пропускаем."""
    from urllib.parse import quote
    payload = http_get(f"{NINJA_BASE}/poe2/{quote(LEAGUE)}/builds/analysis")  # проверь путь через сайт
    if not payload:
        print("[ninja] билды недоступны по этому пути — пропускаю (см. --discover)")
        return 0
    rows = []
    classes = _pick(payload, "classes", "data", default=[])
    for c in classes or []:
        cname = _pick(c, "class_name", "name", default="?")
        pct = _pick(c, "popularity_percentage", "percentage", "pct", default=None)
        for sk in _pick(c, "top_skills", "skills", default=[]) or []:
            rows.append((now_iso(), LEAGUE, str(cname),
                         str(_pick(sk, "name", "skill", default="?")),
                         float(_pick(sk, "percentage", "pct", default=0) or 0)))
        if pct is not None:
            rows.append((now_iso(), LEAGUE, str(cname), None, float(pct)))
    if rows:
        insert_builds(rows)
    print(f"[ninja] builds: {len(rows)} строк")
    return len(rows)

# ----------------------------------------------------------------------------
# Анализ: скользящие статистики -> выводы
# ----------------------------------------------------------------------------

def _mean(xs): return sum(xs)/len(xs) if xs else 0.0
def _std(xs):
    if len(xs) < 2: return 0.0
    m = _mean(xs); return math.sqrt(sum((x-m)**2 for x in xs)/(len(xs)-1))

def _zscore(cur, mean, std):
    """z-score с обработкой плоского бейзлайна: если разброс ~0, но текущее значение
    заметно отклоняется от среднего — это сильный сигнал, а не 0."""
    if std > 1e-9:
        return (cur - mean) / std
    if mean == 0:
        return 0.0
    rel = (cur - mean) / abs(mean)     # относительное отклонение от плоской линии
    if abs(rel) < 0.05:                # <5% от плоского бейзлайна — считаем шумом
        return 0.0
    return math.copysign(min(abs(rel) * 20.0, 99.0), rel)

def analyze():
    """Возвращает список сигналов по каждому предмету на основе истории в БД."""
    con = db()
    items = con.execute(
        "SELECT DISTINCT item_id, item_name, category FROM snapshots WHERE league=?",
        (LEAGUE,),
    ).fetchall()
    signals = []
    W = ALERT_RULES["window"]
    for row in items:
        hist = con.execute(
            "SELECT price, volume, ts FROM snapshots WHERE item_id=? AND league=? "
            "ORDER BY ts DESC LIMIT ?",
            (row["item_id"], LEAGUE, W),
        ).fetchall()
        if len(hist) < ALERT_RULES["min_history"]:
            continue
        prices = [h["price"] for h in hist if h["price"] is not None]
        vols   = [h["volume"] for h in hist if h["volume"] is not None]
        if not prices or not vols:
            continue
        cur_price, old_price = prices[0], prices[-1]
        cur_vol = vols[0]
        momentum = ((cur_price - old_price) / old_price * 100.0) if old_price else 0.0
        vmean, vstd = _mean(vols[1:] or vols), _std(vols[1:] or vols)
        pmean, pstd = _mean(prices[1:] or prices), _std(prices[1:] or prices)
        vol_z = _zscore(cur_vol, vmean, vstd)
        price_z = _zscore(cur_price, pmean, pstd)

        if vol_z > 1.0: vclass = "Busy"
        elif vol_z < -1.0: vclass = "Quiet"
        else: vclass = "Steady"

        # «горячо» = растёт объём И растёт цена (спрос обгоняет предложение)
        hot = vol_z > 1.0 and momentum > 5.0

        signals.append({
            "item_id": row["item_id"], "name": row["item_name"], "category": row["category"],
            "price": round(cur_price, 3), "volume": round(cur_vol, 1),
            "momentum_pct": round(momentum, 1), "vol_z": round(vol_z, 2),
            "price_z": round(price_z, 2), "vclass": vclass, "hot": hot,
        })
    con.close()
    signals.sort(key=lambda s: (s["hot"], abs(s["momentum_pct"]), s["vol_z"]), reverse=True)
    return signals

def signals_to_alerts(signals):
    r = ALERT_RULES
    fired = []
    for s in signals:
        reasons = []
        if abs(s["momentum_pct"]) >= r["momentum_pct"]:
            reasons.append(f"цена {s['momentum_pct']:+.1f}%")
        if s["vol_z"] >= r["volume_z"]:
            reasons.append(f"объём z={s['vol_z']}")
        if abs(s["price_z"]) >= r["price_z"]:
            reasons.append(f"ценовой всплеск z={s['price_z']}")
        if reasons:
            s = dict(s); s["reasons"] = reasons
            fired.append(s)
    return fired

# ----------------------------------------------------------------------------
# Уведомления (с дедупликацией, чтобы не спамить)
# ----------------------------------------------------------------------------

def _already_sent(key, ttl_min=180):
    con = db()
    r = con.execute("SELECT ts FROM alerts_sent WHERE key=?", (key,)).fetchone()
    con.close()
    if not r: return False
    age = (datetime.now(timezone.utc) - datetime.fromisoformat(r["ts"])).total_seconds()/60
    return age < ttl_min

def _mark_sent(key):
    con = db()
    con.execute("INSERT OR REPLACE INTO alerts_sent(key,ts) VALUES (?,?)", (key, now_iso()))
    con.commit(); con.close()

def notify(text):
    print("\n🔔 " + text.replace("\n", "\n   "))
    if DISCORD_WEBHOOK:
        try:
            data = json.dumps({"content": text}).encode()
            req = urllib.request.Request(DISCORD_WEBHOOK, data=data,
                                         headers={"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=10)
        except Exception as e:
            print(f"[discord] {e}", file=sys.stderr)
    if TELEGRAM_TOKEN and TELEGRAM_CHAT:
        try:
            from urllib.parse import urlencode
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage?" + \
                  urlencode({"chat_id": TELEGRAM_CHAT, "text": text})
            urllib.request.urlopen(url, timeout=10)
        except Exception as e:
            print(f"[telegram] {e}", file=sys.stderr)

def dispatch_alerts(alerts):
    for a in alerts:
        key = f"{a['item_id']}|{'|'.join(a['reasons'])}"
        if _already_sent(key):
            continue
        msg = (f"[{a['category']}] {a['name']} — {', '.join(a['reasons'])}\n"
               f"цена {a['price']} ex · объём {a['volume']} ({a['vclass']})"
               + (" · 🔥HOT" if a['hot'] else ""))
        notify(msg)
        _mark_sent(key)

# ----------------------------------------------------------------------------
# Команды
# ----------------------------------------------------------------------------

def resolve_endpoints():
    """Тянет openapi.json и авто-мапит реальные пути (само-исправление регистра/структуры).
    Возвращает True, если что-то удалось уточнить."""
    spec = http_get(SCOUT_BASE + "/openapi.json")
    if not spec or "paths" not in spec:
        return False
    paths = list(spec["paths"].keys())
    def find(*keys, needs=()):
        for p in paths:
            low = p.lower()
            if all(k in low for k in keys) and all(n in p for n in needs):
                return p
        return None
    cur = find("currenc")
    uni = find("uniqu")
    his = find("histor") or find("{", "item")  # history с path-параметром
    lea = find("league")
    changed = False
    for name, val in (("currency", cur), ("unique", uni), ("history", his), ("leagues", lea)):
        if val and val != SCOUT_ENDPOINTS.get(name):
            print(f"[discover] {name}: {SCOUT_ENDPOINTS.get(name)} -> {val}")
            SCOUT_ENDPOINTS[name] = val
            changed = True
    return changed

def cmd_discover():
    print("Тяну реальную схему API (openapi.json) для сверки путей...\n")
    spec = http_get(SCOUT_BASE + "/openapi.json")
    if spec and "paths" in spec:
        print("Доступные пути:")
        for p in sorted(spec["paths"].keys()):
            print("  ", p)
        print()
        resolve_endpoints()
    else:
        print("openapi.json недоступен — пробую прямые запросы.")
    print("\nЛиги:")
    print(json.dumps(http_get(SCOUT_BASE + SCOUT_ENDPOINTS["leagues"]), indent=2, ensure_ascii=False)[:1000])
    print("\nПробный currency (первые записи):")
    sample = http_get(SCOUT_BASE + SCOUT_ENDPOINTS["currency"], {"league": LEAGUE})
    print(json.dumps(sample, indent=2, ensure_ascii=False)[:1000])

def export_dashboard_json(path="docs/data.json"):
    """Собирает выводы экономики + билдов в data.json для веб-дашборда."""
    import builds as bld
    con = db()
    try:
        snaps = con.execute("SELECT COUNT(*) FROM snapshots WHERE league=?", (LEAGUE,)).fetchone()[0]
        div = con.execute(
            "SELECT price FROM snapshots WHERE league=? AND lower(item_name) LIKE '%divine%' "
            "ORDER BY ts DESC LIMIT 1", (LEAGUE,)).fetchone()
        builds_rows = con.execute(
            "SELECT DISTINCT ascendancy, pct, sample_n FROM build_agg WHERE league=? ORDER BY pct DESC",
            (LEAGUE,)).fetchall()
        builds_out = []
        for asc, pct, n in builds_rows:
            skills = con.execute("SELECT value,share FROM build_agg WHERE league=? AND ascendancy=? "
                                 "AND kind='skill' ORDER BY share DESC LIMIT 3", (LEAGUE, asc)).fetchall()
            slots = []
            for (slot,) in con.execute("SELECT DISTINCT slot FROM build_agg WHERE league=? AND ascendancy=? "
                                       "AND kind='base'", (LEAGUE, asc)).fetchall():
                bases = con.execute("SELECT value,share FROM build_agg WHERE league=? AND ascendancy=? "
                                    "AND slot=? AND kind='base' ORDER BY share DESC LIMIT 2",
                                    (LEAGUE, asc, slot)).fetchall()
                mods = con.execute("SELECT value,share FROM build_agg WHERE league=? AND ascendancy=? "
                                   "AND slot=? AND kind='mod' ORDER BY share DESC LIMIT 3",
                                   (LEAGUE, asc, slot)).fetchall()
                slots.append({"slot": slot, "bases": [list(b) for b in bases], "mods": [list(m) for m in mods]})
            builds_out.append({"ascendancy": asc, "pct": pct, "sample_n": n,
                               "skills": [list(s) for s in skills], "slots": slots})
    finally:
        con.close()
    data = {
        "league": LEAGUE,
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "divine_ex": round(div[0], 2) if div and div[0] else "—",
        "snapshots": snaps,
        "sample": False,
        "economy": [{k: s[k] for k in ("name", "category", "price", "momentum_pct",
                     "volume", "vol_z", "vclass", "hot")} for s in analyze()],
        "builds": builds_out,
    }
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"[export] {path}: {len(data['economy'])} эконом-позиций, {len(builds_out)} асцендансов")


def cmd_once():
    init_db()
    n = fetch_scout()
    fetch_ninja_builds()
    sig = analyze()
    alerts = signals_to_alerts(sig)
    print(f"\n[analyze] предметов с сигналом: {len(sig)}, алертов: {len(alerts)}")
    dispatch_alerts(alerts)

def cmd_run():
    init_db()
    print(f"Автономный режим: каждые {POLL_MINUTES} мин. Ctrl+C для выхода.")
    while True:
        try:
            cmd_once()
        except KeyboardInterrupt:
            print("\nОстановлено."); break
        except Exception as e:
            print(f"[loop] {e}", file=sys.stderr)
        time.sleep(POLL_MINUTES * 60)

def cmd_report(top=25):
    init_db()
    sig = analyze()
    if not sig:
        print("Мало истории для выводов. Запусти --once несколько раз (нужно ≥"
              f"{ALERT_RULES['min_history']} снимков на предмет)."); return
    print(f"\n=== ТОП движений ({LEAGUE}) ===")
    print(f"{'name':28} {'cat':9} {'price':>9} {'mom%':>7} {'vol':>8} {'vol_z':>6} {'class':7} hot")
    for s in sig[:top]:
        print(f"{s['name'][:28]:28} {s['category']:9} {s['price']:>9} "
              f"{s['momentum_pct']:>7} {s['volume']:>8} {s['vol_z']:>6} {s['vclass']:7} "
              f"{'🔥' if s['hot'] else ''}")

# ----------------------------------------------------------------------------
# Selftest: проверяем логику анализа на синтетике (без сети)
# ----------------------------------------------------------------------------

def cmd_selftest():
    global DB_PATH
    DB_PATH = ":memory:"  # НЕ трогаем реальную БД
    # in-memory sqlite нужно держать одно соединение — упростим: временный файл
    import tempfile
    fd, DB_PATH = tempfile.mkstemp(suffix=".db"); os.close(fd)
    try:
        init_db()
        # Синтетика: 3 предмета. "Spike Orb" — объём и цена резко вверх в последнем снимке.
        import random
        rows = []
        base_ts = datetime.now(timezone.utc)
        for i in range(12):
            ts = base_ts.replace(microsecond=i).isoformat()
            rows.append((ts, LEAGUE, "currency", "stable", "Stable Orb", 10.0 + random.uniform(-0.2,0.2), 100 + random.uniform(-5,5)))
            rows.append((ts, LEAGUE, "currency", "drift",  "Drift Orb",  10.0 + i*0.4, 80 + random.uniform(-3,3)))
            vol = 90 if i < 11 else 400          # всплеск объёма в последнем
            pr  = 5.0 if i < 11 else 7.5         # +50% цена в последнем
            rows.append((ts, LEAGUE, "currency", "spike",  "Spike Orb",  pr, vol))
        # порядок ts по возрастанию -> в analyze берём DESC, последний снимок = самый свежий
        insert_snapshots(rows)
        sig = analyze()
        by = {s["item_id"]: s for s in sig}
        print("Синтетический анализ:")
        for k in ("stable", "drift", "spike"):
            s = by.get(k, {})
            print(f"  {k:7} mom%={s.get('momentum_pct'):>6}  vol_z={s.get('vol_z'):>5}  "
                  f"class={s.get('vclass'):7} hot={s.get('hot')}")
        alerts = signals_to_alerts(sig)
        print(f"\nСработавших алертов: {len(alerts)}")
        for a in alerts:
            print(f"  {a['name']}: {', '.join(a['reasons'])}")
        # Проверки
        assert by["spike"]["hot"] is True, "Spike должен быть HOT"
        assert by["spike"]["momentum_pct"] > 40, "Spike momentum ~+50%"
        assert by["stable"]["hot"] is False, "Stable не должен быть HOT"
        assert any(a["item_id"] == "spike" for a in alerts), "Spike должен дать алерт"
        assert any(a["item_id"] == "drift" for a in alerts), "Drift (+~48%) должен дать алерт по momentum"
        print("\n✅ selftest passed — логика моментума/объёма/алертов работает.")
    finally:
        try: os.remove(DB_PATH)
        except OSError: pass

def main():
    ap = argparse.ArgumentParser(description="Автономный трекер экономики/билдов PoE2")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--discover", action="store_true", help="показать реальные пути/схему API")
    g.add_argument("--selftest", action="store_true", help="прогнать логику анализа на синтетике")
    g.add_argument("--once", action="store_true", help="один цикл сбора+анализа+алертов")
    g.add_argument("--run", action="store_true", help="автономный цикл каждые POLL_MINUTES мин")
    g.add_argument("--report", action="store_true", help="вывести текущие выводы из БД")
    args = ap.parse_args()
    if args.discover: cmd_discover()
    elif args.selftest: cmd_selftest()
    elif args.once: cmd_once()
    elif args.run: cmd_run()
    elif args.report: cmd_report()

if __name__ == "__main__":
    main()
