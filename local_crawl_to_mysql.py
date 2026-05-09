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

import pymysql

BASE_DIR = Path(__file__).resolve().parent
CRAWLER_PATH = BASE_DIR / "crawler.py"


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
    cmd = [sys.executable, str(CRAWLER_PATH), "--print-sql-only", "--no-db-save"]

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


def apply_sql(stmts: list[str], database: str | None) -> None:
    if not stmts:
        print("[warn] extracted SQL statements = 0")
        return

    conn = pymysql.connect(**mysql_conn_kwargs(database))
    try:
        with conn.cursor() as cur:
            for idx, stmt in enumerate(stmts, start=1):
                cur.execute(stmt)
                if idx % 200 == 0:
                    print(f"[apply] {idx}/{len(stmts)}")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print(f"[done] applied SQL statements: {len(stmts)}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Local crawl -> MySQL importer")
    parser.add_argument("--regions", nargs="+", required=True)
    parser.add_argument("--trade-types", nargs="+", choices=["A1", "B1", "B2"])
    parser.add_argument("--property-type", choices=["apt", "officetel", "all"], default="all")
    parser.add_argument("--min-households", type=int, default=0)
    parser.add_argument("--complex-nos", nargs="+", type=int)
    parser.add_argument("--raw-output")
    parser.add_argument("--database", help="Override MYSQL_DATABASE")
    args = parser.parse_args()

    stmts = run_crawler_and_collect_sql(args)
    print(f"[info] extracted SQL statements: {len(stmts)}")
    apply_sql(stmts, args.database)


if __name__ == "__main__":
    main()
