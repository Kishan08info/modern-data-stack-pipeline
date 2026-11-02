"""Execute the order_events_daily DAG against a local SQLite warehouse.

Task graph comes from dags/daily_pipeline.py (topological order, one
retry per task, downstream tasks skipped if an upstream task fails).
Models are plain SQL (models/*.sql) in dbt style — staging, intermediate,
marts — and the tests mirror dbt schema tests (unique, not_null, range)
plus reconciliation checks. SQLite stands in for Snowflake locally; the
SQL uses no SQLite-only features except DATE(), so the models port
directly to dbt + Snowflake.

Usage: python3 src/run_pipeline.py  (run from the project root)
"""
import csv
import json
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "dags"))
from daily_pipeline import DAG  # noqa: E402

DB = ROOT / "data" / "warehouse.sqlite"
TABLES = ["stg_events", "quarantine_events", "int_events_deduped",
          "dim_customers", "fct_orders", "mart_daily_revenue",
          "mart_revenue_by_category", "mart_orders_by_channel"]
MARTS = ["mart_daily_revenue", "mart_revenue_by_category",
         "mart_orders_by_channel"]


def q(con, sql):
    return con.execute(sql).fetchone()[0]


def run_sql_file(con, name):
    sql = (ROOT / "models" / name).read_text()
    # Idempotent rebuild: drop every table this file creates first.
    for t in TABLES:
        if f"CREATE TABLE {t} " in sql:
            con.execute(f"DROP TABLE IF EXISTS {t}")
    con.executescript(sql)
    con.commit()


def task_ingest(con):
    con.execute("DROP TABLE IF EXISTS bronze_events")
    con.execute("""CREATE TABLE bronze_events (
        event_id TEXT, event_time TEXT, received_at TEXT, order_id TEXT,
        customer_id TEXT, product_category TEXT, channel TEXT,
        status TEXT, amount REAL, currency TEXT)""")
    rows = [json.loads(line) for line in
            (ROOT / "data" / "raw_events.jsonl").read_text().splitlines()
            if line.strip()]
    con.executemany(
        "INSERT INTO bronze_events VALUES (?,?,?,?,?,?,?,?,?,?)",
        [(r["event_id"], r["event_time"], r["received_at"], r["order_id"],
          r["customer_id"], r["product_category"], r["channel"],
          r["status"], r["amount"], r["currency"]) for r in rows])
    con.commit()
    return {"bronze_rows": q(con, "SELECT COUNT(*) FROM bronze_events")}


def task_build_staging(con):
    run_sql_file(con, "stg_events.sql")
    return {"stg_rows": q(con, "SELECT COUNT(*) FROM stg_events")}


def task_build_intermediate(con):
    run_sql_file(con, "int_events_deduped.sql")
    return {
        "silver_rows": q(con, "SELECT COUNT(*) FROM int_events_deduped"),
        "quarantined_rows": q(con, "SELECT COUNT(*) FROM quarantine_events"),
        "duplicates_removed": q(
            con, "SELECT COUNT(*) FROM stg_events")
        - q(con, "SELECT COUNT(*) FROM quarantine_events")
        - q(con, "SELECT COUNT(*) FROM int_events_deduped"),
        "late_events": q(
            con, "SELECT COUNT(*) FROM int_events_deduped WHERE "
            "julianday(received_at) - julianday(event_time) > 2"),
        "quarantine_by_reason": dict(con.execute(
            "SELECT quarantine_reason, COUNT(*) FROM quarantine_events "
            "GROUP BY 1").fetchall()),
    }


def task_build_marts(con):
    run_sql_file(con, "marts.sql")
    return {
        "fct_orders": q(con, "SELECT COUNT(*) FROM fct_orders"),
        "dim_customers": q(con, "SELECT COUNT(*) FROM dim_customers"),
        "mart_days": q(con, "SELECT COUNT(*) FROM mart_daily_revenue"),
    }


def task_run_tests(con):
    checks = {
        "silver_event_id_unique": q(
            con, "SELECT COUNT(*) FROM (SELECT event_id FROM "
            "int_events_deduped GROUP BY 1 HAVING COUNT(*) > 1)") == 0,
        "fct_order_id_unique": q(
            con, "SELECT COUNT(*) FROM (SELECT order_id FROM fct_orders "
            "GROUP BY 1 HAVING COUNT(*) > 1)") == 0,
        "fct_amount_valid": q(
            con, "SELECT COUNT(*) FROM fct_orders WHERE amount IS NULL "
            "OR amount < 0") == 0,
        "bronze_reconciles": q(con, "SELECT COUNT(*) FROM bronze_events")
        == q(con, "SELECT COUNT(*) FROM int_events_deduped")
        + q(con, "SELECT COUNT(*) FROM quarantine_events")
        + (q(con, "SELECT COUNT(*) FROM stg_events")
           - q(con, "SELECT COUNT(*) FROM quarantine_events")
           - q(con, "SELECT COUNT(*) FROM int_events_deduped")),
        "mart_revenue_reconciles": abs(
            q(con, "SELECT COALESCE(SUM(revenue),0) FROM mart_daily_revenue")
            - q(con, "SELECT COALESCE(SUM(amount),0) FROM fct_orders "
                "WHERE status='paid'")) < 0.01,
        "mart_no_null_dates": q(
            con, "SELECT COUNT(*) FROM mart_daily_revenue "
            "WHERE event_date IS NULL") == 0,
    }
    failed = [k for k, v in checks.items() if not v]
    if failed:
        raise AssertionError(f"data tests failed: {failed}")
    return {"tests": checks}


def task_publish(con):
    out = ROOT / "results"
    out.mkdir(exist_ok=True)
    for mart in MARTS:
        rows = con.execute(f"SELECT * FROM {mart}").fetchall()
        cols = [d[0] for d in con.execute(
            f"SELECT * FROM {mart} LIMIT 0").description]
        with open(out / f"{mart}.csv", "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(cols)
            w.writerows(rows)
    report = {
        "paid_orders": q(con, "SELECT COUNT(*) FROM fct_orders "
                          "WHERE status='paid'"),
        "total_revenue_paid": round(q(
            con, "SELECT COALESCE(SUM(amount),0) FROM fct_orders "
            "WHERE status='paid'"), 2),
        "refunded_amount": round(q(
            con, "SELECT COALESCE(SUM(amount),0) FROM fct_orders "
            "WHERE status='refunded'"), 2),
        "status_breakdown": dict(con.execute(
            "SELECT status, COUNT(*) FROM fct_orders GROUP BY 1").fetchall()),
    }
    (out / "run_report.json").write_text(json.dumps(report, indent=2))
    return report


TASKS = {"ingest": task_ingest, "build_staging": task_build_staging,
         "build_intermediate": task_build_intermediate,
         "build_marts": task_build_marts, "run_tests": task_run_tests,
         "publish": task_publish}


def topo_order():
    tasks = {t["task_id"]: t["depends_on"] for t in DAG["tasks"]}
    done, order = set(), []
    while len(order) < len(tasks):
        ready = [t for t, deps in tasks.items()
                 if t not in done and all(d in done for d in deps)]
        if not ready:
            raise ValueError("circular dependency in DAG")
        for t in sorted(ready):
            order.append(t)
            done.add(t)
    return order


def main():
    con = sqlite3.connect(DB)
    summary = {}
    for task_id in topo_order():
        for attempt in (1, 2):
            try:
                summary[task_id] = TASKS[task_id](con)
                print(f"OK   {task_id}: {summary[task_id]}")
                break
            except Exception as e:  # noqa: BLE001
                print(f"FAIL {task_id} attempt {attempt}: {e}")
                if attempt == 2:
                    raise
    (ROOT / "results" / "dag_run.json").write_text(
        json.dumps({"dag_id": DAG["dag_id"], "tasks": summary}, indent=2))
    con.close()
    print("DAG complete.")


if __name__ == "__main__":
    main()
