"""SCHEMA.md (RE_* / MySQL) 기준 시세 조회 헬퍼.

연결 정보는 환경변수 또는 인자로 받습니다.
- MYSQL_HOST     (default: 127.0.0.1)   # 리눅스 로컬 MySQL
- MYSQL_PORT     (default: 3306)
- MYSQL_USER     (default: root)
- MYSQL_PASSWORD (default: '')
- MYSQL_DATABASE (default: brain)
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterable, Optional

import pymysql
from pymysql.cursors import DictCursor


TRADE_LABELS = {"A1": "매매", "B1": "전세", "B2": "월세"}
RENT_TRADE_TYPES = ("B1", "B2")


def _conn_kwargs(database: Optional[str] = None) -> dict:
    return {
        "host": os.getenv("MYSQL_HOST", "127.0.0.1"),
        "port": int(os.getenv("MYSQL_PORT", "3306")),
        "user": os.getenv("MYSQL_USER", "root"),
        "password": os.getenv("MYSQL_PASSWORD", ""),
        "database": database or os.getenv("MYSQL_DATABASE", "brain"),
        "charset": "utf8mb4",
        "autocommit": True,
        "cursorclass": DictCursor,
    }


@contextmanager
def _connect(database: Optional[str] = None):
    conn = pymysql.connect(**_conn_kwargs(database))
    try:
        yield conn
    finally:
        conn.close()


def _normalize_trade_types(trade_types: Optional[Iterable[str]]) -> list[str]:
    if not trade_types:
        return []
    return [t for t in trade_types if t in TRADE_LABELS]


def _trade_filter_clause(trade_types: list[str]) -> tuple[str, list]:
    """RE_LISTING.TRADE_TYPE 필터 SQL 조각."""
    if not trade_types:
        return "", []
    placeholders = ",".join(["%s"] * len(trade_types))
    return f"L.TRADE_TYPE IN ({placeholders})", list(trade_types)


def search_market(
    region_depth1: Optional[str] = None,
    region_depth2: Optional[str] = None,
    region_depth3: Optional[str] = None,
    trade_types: Optional[Iterable[str]] = None,
    property_type: Optional[str] = None,
    min_households: Optional[int] = None,
    price_min: Optional[int] = None,
    price_max: Optional[int] = None,
    complex_nos: Optional[Iterable[int]] = None,
    limit: int = 30,
    database: Optional[str] = None,
) -> list[dict]:
    """단지 단위로 최근 스냅샷의 가격 요약(매매/전세/월세)을 반환.

    가격 단위는 원(KRW) 정수입니다.
    """

    where: list[str] = ["S.SNAPSHOT_DATE = (SELECT MAX(SNAPSHOT_DATE) FROM RE_LIST_SNAP)"]
    params: list = []

    if region_depth1:
        where.append("C.REGION_DEPTH1 = %s")
        params.append(region_depth1)
    if region_depth2:
        where.append("C.REGION_DEPTH2 = %s")
        params.append(region_depth2)
    if region_depth3:
        where.append("C.REGION_DEPTH3 = %s")
        params.append(region_depth3)
    if property_type and property_type != "all":
        where.append("C.TYPE = %s")
        params.append(property_type)
    if min_households is not None:
        where.append("IFNULL(C.TOTAL_HOUSEHOLDS, 0) >= %s")
        params.append(int(min_households))

    if complex_nos:
        cn_list = [int(v) for v in complex_nos]
        if cn_list:
            placeholders = ",".join(["%s"] * len(cn_list))
            where.append(f"C.COMPLEX_NO IN ({placeholders})")
            params.extend(cn_list)

    tt_list = _normalize_trade_types(trade_types)
    tt_clause, tt_params = _trade_filter_clause(tt_list)
    if tt_clause:
        where.append(tt_clause)
        params.extend(tt_params)

    where_sql = " AND ".join(where)

    base_sql = f"""
        SELECT
          C.COMPLEX_NO,
          C.NAME,
          C.TYPE                AS PROPERTY_TYPE,
          C.REGION_DEPTH1,
          C.REGION_DEPTH2,
          C.REGION_DEPTH3,
          C.TOTAL_HOUSEHOLDS,
          MAX(S.SNAPSHOT_DATE)  AS SNAPSHOT_DATE,
          MIN(CASE WHEN L.TRADE_TYPE = 'A1' THEN S.PRICE END)        AS TRADE_MIN,
          AVG(CASE WHEN L.TRADE_TYPE = 'A1' THEN S.PRICE END)        AS TRADE_AVG,
          MAX(CASE WHEN L.TRADE_TYPE = 'A1' THEN S.PRICE END)        AS TRADE_MAX,
          SUM(CASE WHEN L.TRADE_TYPE = 'A1' THEN 1 ELSE 0 END)       AS TRADE_COUNT,
          MIN(CASE WHEN L.TRADE_TYPE = 'B1' THEN S.PRICE END)        AS RENT_MIN,
          AVG(CASE WHEN L.TRADE_TYPE = 'B1' THEN S.PRICE END)        AS RENT_AVG,
          MAX(CASE WHEN L.TRADE_TYPE = 'B1' THEN S.PRICE END)        AS RENT_MAX,
          SUM(CASE WHEN L.TRADE_TYPE = 'B1' THEN 1 ELSE 0 END)       AS RENT_COUNT,
          MIN(CASE WHEN L.TRADE_TYPE = 'B2' THEN S.PRICE END)        AS MONTHLY_DEPOSIT_MIN,
          AVG(CASE WHEN L.TRADE_TYPE = 'B2' THEN S.PRICE END)        AS MONTHLY_DEPOSIT_AVG,
          MIN(CASE WHEN L.TRADE_TYPE = 'B2' THEN S.MONTHLY_RENT END) AS MONTHLY_RENT_MIN,
          AVG(CASE WHEN L.TRADE_TYPE = 'B2' THEN S.MONTHLY_RENT END) AS MONTHLY_RENT_AVG,
          SUM(CASE WHEN L.TRADE_TYPE = 'B2' THEN 1 ELSE 0 END)       AS MONTHLY_COUNT
        FROM RE_LIST_SNAP S
        JOIN RE_LISTING   L ON L.ARTICLE_NUMBER = S.ARTICLE_NUMBER
        JOIN RE_COMPLEX   C ON C.COMPLEX_NO     = L.COMPLEX_NO
        WHERE {where_sql}
        GROUP BY
          C.COMPLEX_NO, C.NAME, C.TYPE,
          C.REGION_DEPTH1, C.REGION_DEPTH2, C.REGION_DEPTH3,
          C.TOTAL_HOUSEHOLDS
    """

    having: list[str] = []
    if price_min is not None:
        having.append(
            "(IFNULL(TRADE_MIN, 0) >= %s OR IFNULL(RENT_MIN, 0) >= %s "
            "OR IFNULL(MONTHLY_DEPOSIT_MIN, 0) >= %s)"
        )
        params.extend([int(price_min)] * 3)
    if price_max is not None:
        having.append(
            "((TRADE_MIN IS NOT NULL AND TRADE_MIN <= %s) "
            "OR (RENT_MIN IS NOT NULL AND RENT_MIN <= %s) "
            "OR (MONTHLY_DEPOSIT_MIN IS NOT NULL AND MONTHLY_DEPOSIT_MIN <= %s))"
        )
        params.extend([int(price_max)] * 3)

    sql = base_sql
    if having:
        sql += " HAVING " + " AND ".join(having)

    sql += """
        ORDER BY
          GREATEST(IFNULL(TRADE_COUNT, 0),
                   IFNULL(RENT_COUNT, 0),
                   IFNULL(MONTHLY_COUNT, 0)) DESC,
          IFNULL(TOTAL_HOUSEHOLDS, 0) DESC
        LIMIT %s
    """
    params.append(int(limit))

    with _connect(database) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    for r in rows:
        trade_avg = float(r.get("TRADE_AVG") or 0)
        rent_avg = float(r.get("RENT_AVG") or 0)
        r["JEONSE_RATIO"] = (
            round(rent_avg / trade_avg * 100, 2)
            if trade_avg > 0 and rent_avg > 0
            else None
        )
        if isinstance(r.get("SNAPSHOT_DATE"), object) and r.get("SNAPSHOT_DATE") is not None:
            r["SNAPSHOT_DATE"] = str(r["SNAPSHOT_DATE"])

    return rows


def market_trend(
    region_depth2: str,
    property_type: str = "all",
    database: Optional[str] = None,
) -> dict:
    """구/군 단위로 최근 두 스냅샷 일자의 매물수/평균가 변화를 비교."""

    type_clause = ""
    type_params: list = []
    if property_type and property_type != "all":
        type_clause = " AND C.TYPE = %s"
        type_params = [property_type]

    with _connect(database) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT SNAPSHOT_DATE
                FROM RE_LIST_SNAP
                ORDER BY SNAPSHOT_DATE DESC
                LIMIT 2
                """
            )
            dates = [row["SNAPSHOT_DATE"] for row in cur.fetchall()]
            if len(dates) < 2:
                return {"message": "추세 계산을 위해 최소 2회 수집 데이터가 필요합니다."}

            latest, prev = dates[0], dates[1]

            sql = f"""
                SELECT
                  SUM(CASE WHEN S.SNAPSHOT_DATE = %s THEN 1 ELSE 0 END) AS LATEST_CNT,
                  SUM(CASE WHEN S.SNAPSHOT_DATE = %s THEN 1 ELSE 0 END) AS PREV_CNT,
                  AVG(CASE WHEN S.SNAPSHOT_DATE = %s AND L.TRADE_TYPE = 'A1' THEN S.PRICE END) AS LATEST_TRADE_AVG,
                  AVG(CASE WHEN S.SNAPSHOT_DATE = %s AND L.TRADE_TYPE = 'A1' THEN S.PRICE END) AS PREV_TRADE_AVG,
                  AVG(CASE WHEN S.SNAPSHOT_DATE = %s AND L.TRADE_TYPE = 'B1' THEN S.PRICE END) AS LATEST_RENT_AVG,
                  AVG(CASE WHEN S.SNAPSHOT_DATE = %s AND L.TRADE_TYPE = 'B1' THEN S.PRICE END) AS PREV_RENT_AVG,
                  AVG(CASE WHEN S.SNAPSHOT_DATE = %s AND L.TRADE_TYPE = 'B2' THEN S.MONTHLY_RENT END) AS LATEST_MONTHLY_RENT_AVG,
                  AVG(CASE WHEN S.SNAPSHOT_DATE = %s AND L.TRADE_TYPE = 'B2' THEN S.MONTHLY_RENT END) AS PREV_MONTHLY_RENT_AVG
                FROM RE_LIST_SNAP S
                JOIN RE_LISTING L ON L.ARTICLE_NUMBER = S.ARTICLE_NUMBER
                JOIN RE_COMPLEX C ON C.COMPLEX_NO     = L.COMPLEX_NO
                WHERE C.REGION_DEPTH2 = %s
                  AND S.SNAPSHOT_DATE IN (%s, %s){type_clause}
                """

            params = [
                latest, prev,
                latest, prev,
                latest, prev,
                latest, prev,
                region_depth2,
                latest, prev,
                *type_params,
            ]
            cur.execute(sql, params)
            row = cur.fetchone() or {}

    latest_cnt = int(row.get("LATEST_CNT") or 0)
    prev_cnt = int(row.get("PREV_CNT") or 0)
    return {
        "latest_date": str(latest),
        "previous_date": str(prev),
        "latest_count": latest_cnt,
        "previous_count": prev_cnt,
        "count_delta": latest_cnt - prev_cnt,
        "latest_trade_avg_won": int(row.get("LATEST_TRADE_AVG") or 0),
        "previous_trade_avg_won": int(row.get("PREV_TRADE_AVG") or 0),
        "latest_rent_avg_won": int(row.get("LATEST_RENT_AVG") or 0),
        "previous_rent_avg_won": int(row.get("PREV_RENT_AVG") or 0),
        "latest_monthly_rent_avg_won": int(row.get("LATEST_MONTHLY_RENT_AVG") or 0),
        "previous_monthly_rent_avg_won": int(row.get("PREV_MONTHLY_RENT_AVG") or 0),
    }


def recommend_by_budget(
    region_depth1: str,
    property_type: str,
    budget_won: int,
    trade_types: Optional[Iterable[str]] = None,
    min_households: int = 0,
    limit: int = 10,
    database: Optional[str] = None,
) -> list[dict]:
    """예산(보증금) 기준으로 가장 가까운 단지 후보를 반환.

    기본 거래유형은 전세/월세(B1, B2). PRICE(보증금)가 budget_won과 가장 가까운 매물을 가진 단지를
    근접도 순으로 정렬한다.
    """

    tt_list = _normalize_trade_types(trade_types) or list(RENT_TRADE_TYPES)
    placeholders = ",".join(["%s"] * len(tt_list))

    sql = f"""
        SELECT
          C.COMPLEX_NO,
          C.NAME,
          C.TYPE              AS PROPERTY_TYPE,
          C.REGION_DEPTH1,
          C.REGION_DEPTH2,
          C.REGION_DEPTH3,
          C.TOTAL_HOUSEHOLDS,
          MAX(S.SNAPSHOT_DATE) AS SNAPSHOT_DATE,
          MIN(S.PRICE)         AS DEPOSIT_MIN,
          AVG(S.PRICE)         AS DEPOSIT_AVG,
          MAX(S.PRICE)         AS DEPOSIT_MAX,
          AVG(S.MONTHLY_RENT)  AS MONTHLY_RENT_AVG,
          COUNT(*)             AS LISTING_COUNT,
          MIN(ABS(S.PRICE - %s)) AS BUDGET_GAP
        FROM RE_LIST_SNAP S
        JOIN RE_LISTING   L ON L.ARTICLE_NUMBER = S.ARTICLE_NUMBER
        JOIN RE_COMPLEX   C ON C.COMPLEX_NO     = L.COMPLEX_NO
        WHERE S.SNAPSHOT_DATE = (SELECT MAX(SNAPSHOT_DATE) FROM RE_LIST_SNAP)
          AND L.TRADE_TYPE IN ({placeholders})
          AND C.REGION_DEPTH1 = %s
          AND (%s = 'all' OR C.TYPE = %s)
          AND IFNULL(C.TOTAL_HOUSEHOLDS, 0) >= %s
          AND S.PRICE > 0
        GROUP BY
          C.COMPLEX_NO, C.NAME, C.TYPE,
          C.REGION_DEPTH1, C.REGION_DEPTH2, C.REGION_DEPTH3,
          C.TOTAL_HOUSEHOLDS
        ORDER BY BUDGET_GAP ASC, LISTING_COUNT DESC
        LIMIT %s
    """

    params: list = [int(budget_won)]
    params.extend(tt_list)
    params.extend([region_depth1, property_type, property_type, int(min_households), int(limit)])

    with _connect(database) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    for r in rows:
        if r.get("SNAPSHOT_DATE") is not None:
            r["SNAPSHOT_DATE"] = str(r["SNAPSHOT_DATE"])
    return rows


def listings_for_complex(
    complex_no: int,
    snapshot_date: Optional[str] = None,
    trade_types: Optional[Iterable[str]] = None,
    database: Optional[str] = None,
) -> list[dict]:
    """단지 ID 기준으로 (기본은 최신) 스냅샷의 매물 목록을 반환.

    SCHEMA.md 6.1 예시와 동일한 JOIN 구조.
    """

    where = ["L.COMPLEX_NO = %s"]
    params: list = [int(complex_no)]

    if snapshot_date:
        where.append("S.SNAPSHOT_DATE = %s")
        params.append(snapshot_date)
    else:
        where.append(
            "S.SNAPSHOT_DATE = ("
            " SELECT MAX(SNAPSHOT_DATE) FROM RE_LIST_SNAP S2"
            " JOIN RE_LISTING L2 ON L2.ARTICLE_NUMBER = S2.ARTICLE_NUMBER"
            " WHERE L2.COMPLEX_NO = %s)"
        )
        params.append(int(complex_no))

    tt_list = _normalize_trade_types(trade_types)
    if tt_list:
        placeholders = ",".join(["%s"] * len(tt_list))
        where.append(f"L.TRADE_TYPE IN ({placeholders})")
        params.extend(tt_list)

    sql = f"""
        SELECT
          C.COMPLEX_NO,
          C.NAME            AS COMPLEX_NAME,
          P.NAME            AS PYEONG_NAME,
          P.SUPPLY_AREA,
          P.EXCLUSIVE_AREA,
          L.ARTICLE_NUMBER,
          L.TRADE_TYPE,
          L.DONG_NAME,
          L.FLOOR_TEXT,
          L.DIRECTION,
          S.SNAPSHOT_DATE,
          S.PRICE,
          S.MONTHLY_RENT,
          S.NOTE
        FROM RE_LIST_SNAP S
        JOIN RE_LISTING   L ON L.ARTICLE_NUMBER = S.ARTICLE_NUMBER
        LEFT JOIN RE_PYEONG P ON P.PYEONG_ID    = L.PYEONG_ID
        JOIN RE_COMPLEX   C ON C.COMPLEX_NO     = L.COMPLEX_NO
        WHERE {' AND '.join(where)}
        ORDER BY P.SUPPLY_AREA, L.TRADE_TYPE, S.PRICE
    """

    with _connect(database) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    for r in rows:
        if r.get("SNAPSHOT_DATE") is not None:
            r["SNAPSHOT_DATE"] = str(r["SNAPSHOT_DATE"])
    return rows


def price_history(
    article_number: str,
    database: Optional[str] = None,
) -> list[dict]:
    """매물 단위 가격 변동 이력 (SCHEMA.md 6.2 예시)."""

    sql = """
        SELECT SNAPSHOT_DATE, PRICE, MONTHLY_RENT, NOTE
        FROM RE_LIST_SNAP
        WHERE ARTICLE_NUMBER = %s
        ORDER BY SNAPSHOT_DATE
    """
    with _connect(database) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, [str(article_number)])
            rows = cur.fetchall()

    for r in rows:
        if r.get("SNAPSHOT_DATE") is not None:
            r["SNAPSHOT_DATE"] = str(r["SNAPSHOT_DATE"])
    return rows
