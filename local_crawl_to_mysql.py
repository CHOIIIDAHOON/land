#!/usr/bin/env python3
"""Run crawler locally, extract RE_* INSERT SQL, and apply to MySQL.

Usage:
  python local_crawl_to_mysql.py --regions "서울시 강남구 역삼동"
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta

import pymysql

BASE_DIR = Path(__file__).resolve().parent
CRAWLER_PATH = BASE_DIR / "crawler.py"


def _kst_now_str() -> str:
    return datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d %H:%M:%S")


def resolve_sql_log_path(cli_value: str | None) -> Path:
    if cli_value:
        return Path(cli_value).expanduser().resolve()
    env_value = os.getenv("SQL_LOG_PATH", "").strip()
    if env_value:
        return Path(env_value).expanduser().resolve()
    return (BASE_DIR / "applied_sql.log").resolve()


def append_sql_log(log_path: Path, line: str) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as f:
        f.write(line)
        if not line.endswith("\n"):
            f.write("\n")


def mysql_conn_kwargs(database: str | None) -> dict:
    return {
        "host": os.getenv("MYSQL_HOST", "127.0.0.1"),
        "port": int(os.getenv("MYSQL_PORT", "3306")),
        "user": os.getenv("MYSQL_USER", "root"),
        "password": os.getenv("MYSQL_PASSWORD", ""),
        "database": database or os.getenv("MYSQL_DATABASE", "brain"),
        "charset": "utf8mb4",
        "autocommit": False,
    }


def extract_sql_blocks(stdout_text: str) -> list[str]:
    stmts: list[str] = []
    buf: list[str] = []
    in_stmt = False

    for line in stdout_text.splitlines():
        stripped = line.strip()
        if not in_stmt:
            if stripped.startswith("INSERT INTO RE_"):
                in_stmt = True
                buf = [line]
            continue

        buf.append(line)
        if stripped.endswith(";"):
            stmt = "\n".join(buf).strip()
            if stmt:
                stmts.append(stmt)
            buf = []
            in_stmt = False

    return stmts


def run_crawler_and_collect_sql(args: argparse.Namespace) -> list[str]:
    cmd = [sys.executable, str(CRAWLER_PATH), "--print-sql-only"]

    if args.regions:
        cmd.extend(["--regions", *args.regions])
    if args.trade_types:
        cmd.extend(["--trade-types", *args.trade_types])
    if args.property_type:
        cmd.extend(["--property-type", args.property_type])
    if args.min_households is not None:
        cmd.extend(["--min-households", str(args.min_households)])
    if args.complex_nos:
        cmd.extend(["--complex-nos", *[str(v) for v in args.complex_nos]])
    if args.raw_output:
        cmd.extend(["--raw-output", args.raw_output])

    print("[run]", " ".join(cmd))
    proc = subprocess.run(cmd, cwd=str(BASE_DIR), text=True, capture_output=True)

    if proc.stderr:
        # crawler logging goes to stderr by default
        print(proc.stderr, file=sys.stderr, end="")

    if proc.returncode != 0:
        if proc.stdout:
            print(proc.stdout)
        raise RuntimeError(f"crawler failed (exit={proc.returncode})")

    return extract_sql_blocks(proc.stdout)


def apply_sql(stmts: list[str], database: str | None, sql_log_path: Path | None = None) -> None:
    if not stmts:
        print("[warn] extracted SQL statements = 0")
        return

    if sql_log_path is not None:
        append_sql_log(
            sql_log_path,
            f"\n=== run_start { _kst_now_str() } statements={len(stmts)} ===",
        )

    conn = pymysql.connect(**mysql_conn_kwargs(database))
    listing_count = 0
    snap_count = 0
    try:
        with conn.cursor() as cur:
            for idx, stmt in enumerate(stmts, start=1):
                if sql_log_path is not None:
                    append_sql_log(sql_log_path, f"\n-- stmt #{idx}\n{stmt}\n")
                cur.execute(stmt)
                head = stmt.lstrip().upper()
                if "INSERT INTO RE_LISTING" in head:
                    listing_count += 1
                elif "INSERT INTO RE_LIST_SNAP" in head:
                    snap_count += 1
                if idx % 200 == 0:
                    print(f"[apply] {idx}/{len(stmts)}")
        conn.commit()
        if sql_log_path is not None:
            append_sql_log(
                sql_log_path,
                (
                    f"=== run_commit { _kst_now_str() } "
                    f"total={len(stmts)} re_listing={listing_count} re_list_snap={snap_count} ==="
                ),
            )
    except Exception:
        conn.rollback()
        if sql_log_path is not None:
            append_sql_log(
                sql_log_path,
                f"=== run_rollback { _kst_now_str() } ===",
            )
        raise
    finally:
        conn.close()

    print(f"[done] applied SQL statements: {len(stmts)}")
    print(f"[done] RE_LISTING inserts: {listing_count}, RE_LIST_SNAP inserts: {snap_count}")
    if sql_log_path is not None:
        print(f"[done] SQL log: {sql_log_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Local crawl -> MySQL importer")
    parser.add_argument("--regions", nargs="+", required=True)
    parser.add_argument("--trade-types", nargs="+", choices=["A1", "B1", "B2"])
    parser.add_argument("--property-type", choices=["apt", "officetel", "all"], default="all")
    parser.add_argument("--min-households", type=int, default=0)
    parser.add_argument("--complex-nos", nargs="+", type=int)
    parser.add_argument("--raw-output")
    parser.add_argument("--database", help="Override MYSQL_DATABASE")
    parser.add_argument(
        "--sql-log",
        help="Path to write executed SQL log (default: ./applied_sql.log or $SQL_LOG_PATH)",
    )
    args = parser.parse_args()

    stmts = run_crawler_and_collect_sql(args)
    print(f"[info] extracted SQL statements: {len(stmts)}")
    apply_sql(stmts, args.database, resolve_sql_log_path(args.sql_log))


if __name__ == "__main__":
    main()
