#!/usr/bin/env python3
"""
run.py — единая точка входа poe2tracker.

Один прогон = собрать экономику (poe2scout) + при необходимости билды (poe.ninja) ->
посчитать выводы -> отправить отфильтрованные алерты. Это то, что дёргает GitHub Actions
по расписанию (см. .github/workflows/track.yml). Локально тоже работает.

Команды:
  python run.py --once           # один цикл (для cron)
  python run.py --once --builds  # тот же цикл + принудительно обновить билды
  python run.py --report         # показать выводы по экономике и билдам
  python run.py --selftest       # прогнать логику обоих модулей без сети
"""
import argparse
import tracker as eco
import builds as bld
import craft as cft


def cycle(force_builds=False):
    eco.init_db()
    eco.fetch_scout()                                  # цены + объём
    con = eco.db()
    try:
        bld.refresh_builds(con, eco.LEAGUE, eco.http_get, force=force_builds)  # билды (гейт 12ч)
    finally:
        con.close()
    sig = eco.analyze()
    alerts = eco.signals_to_alerts(sig)
    print(f"[run] сигналов: {len(sig)}, алертов: {len(alerts)}")
    eco.dispatch_alerts(alerts)
    eco.export_dashboard_json("docs/data.json")   # для веб-дашборда (GitHub Pages)


def report():
    eco.cmd_report()
    con = eco.db()
    try:
        bld.report_builds(con, eco.LEAGUE)
    finally:
        con.close()


def selftest():
    eco.cmd_selftest()
    print()
    bld._selftest()
    print()
    cft._selftest()


def craft_demo():
    """Оценка стоимости крафта популярной вещи в текущих ценах БД.
    p_hit берётся из весов модов (поиск на poe2db) — здесь демонстрационные значения."""
    import os
    plan = cft.CraftPlan(base_name="Warmonger Bow", base_cost=0,   # 0 -> цену тянем из БД
                         affix_slots=6, essence_mods=1, target_mods=3, p_hit=0.08, max_annuls=2)
    cft.craft_from_db(os.environ.get("POE2_DB", "poe2tracker.db"), eco.LEAGUE, plan)


def main():
    ap = argparse.ArgumentParser(description="poe2tracker — экономика + билды + крафт")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--once", action="store_true")
    g.add_argument("--report", action="store_true")
    g.add_argument("--craft", action="store_true", help="оценить ожидаемую стоимость крафта (демо-план)")
    g.add_argument("--selftest", action="store_true")
    ap.add_argument("--builds", action="store_true", help="принудительно обновить билды в этом прогоне")
    a = ap.parse_args()
    if a.once: cycle(force_builds=a.builds)
    elif a.report: report()
    elif a.craft: craft_demo()
    elif a.selftest: selftest()


if __name__ == "__main__":
    main()
