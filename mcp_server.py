#!/usr/bin/env python3
import asyncio
import os
from pathlib import Path
from tempfile import NamedTemporaryFile

from mcp.server.fastmcp import FastMCP

from crawler import run_crawler
from market_query import (
    listings_for_complex,
    market_trend,
    price_history,
    recommend_by_budget,
    search_market,
)


mcp = FastMCP("naver-land-market")
BASE_DIR = Path(__file__).resolve().parent
DEFAULT_CRAWLER_DB_PATH = str(BASE_DIR / "real_estate.db")


@mcp.tool()
def crawl_market(
    regions: list[str],
    trade_types: list[str] | None = None,
    property_type: str = "all",
    min_households: int = 0,
    complex_nos: list[int] | None = None,
    db_path: str = DEFAULT_CRAWLER_DB_PATH,
    keep_raw_output: bool = False,
) -> dict:
    """지역/필터 조건으로 시장 데이터를 수집한다 (크롤러는 SQLite + RE_* INSERT SQL 출력)."""
    raw_file = NamedTemporaryFile(
        prefix="raw_naver_land_",
        suffix=".json",
        delete=False,
        dir=str(BASE_DIR),
    )
    raw_file_path = raw_file.name
    raw_file.close()

    asyncio.run(
        run_crawler(
            regions=regions,
            trade_types=trade_types or ["A1", "B1", "B2"],
            property_type=property_type,
            min_households=min_households,
            complex_nos=complex_nos,
            db_path=db_path,
            raw_output=raw_file_path,
            no_db_save=False,
        )
    )

    result = {
        "status": "ok",
        "db_path": db_path,
        "raw_output_path": raw_file_path,
        "note": "raw 파일은 임시 산출물입니다.",
    }
    if not keep_raw_output and os.path.exists(raw_file_path):
        os.remove(raw_file_path)
        result["raw_output_deleted"] = True
    else:
        result["raw_output_deleted"] = False
    return result


@mcp.tool()
def search_listings(
    region_depth1: str | None = None,
    region_depth2: str | None = None,
    region_depth3: str | None = None,
    trade_types: list[str] | None = None,
    property_type: str = "all",
    min_households: int | None = None,
    price_min: int | None = None,
    price_max: int | None = None,
    complex_nos: list[int] | None = None,
    limit: int = 30,
    database: str | None = None,
) -> dict:
    """RE_* (MySQL) 스키마에서 단지 단위 가격 요약을 조건별로 조회한다."""
    rows = search_market(
        region_depth1=region_depth1,
        region_depth2=region_depth2,
        region_depth3=region_depth3,
        trade_types=trade_types,
        property_type=property_type,
        min_households=min_households,
        price_min=price_min,
        price_max=price_max,
        complex_nos=complex_nos,
        limit=limit,
        database=database,
    )
    return {"count": len(rows), "items": rows}


@mcp.tool()
def get_market_trend(
    region_depth2: str,
    property_type: str = "all",
    database: str | None = None,
) -> dict:
    """구/군 단위로 최근 두 스냅샷 일자의 매물수/평균가 변동을 비교한다."""
    return market_trend(
        region_depth2=region_depth2,
        property_type=property_type,
        database=database,
    )


@mcp.tool()
def recommend_for_budget(
    budget_won: int,
    region_depth1: str,
    property_type: str = "officetel",
    trade_types: list[str] | None = None,
    min_households: int = 0,
    limit: int = 10,
    database: str | None = None,
) -> dict:
    """예산(보증금) 기준으로 가까운 후보 단지를 추천한다."""
    items = recommend_by_budget(
        region_depth1=region_depth1,
        property_type=property_type,
        budget_won=budget_won,
        trade_types=trade_types,
        min_households=min_households,
        limit=limit,
        database=database,
    )
    return {"count": len(items), "items": items}


@mcp.tool()
def get_complex_listings(
    complex_no: int,
    snapshot_date: str | None = None,
    trade_types: list[str] | None = None,
    database: str | None = None,
) -> dict:
    """단지 ID 기준으로 (기본은 최신) 스냅샷의 매물 목록을 반환한다."""
    items = listings_for_complex(
        complex_no=complex_no,
        snapshot_date=snapshot_date,
        trade_types=trade_types,
        database=database,
    )
    return {"count": len(items), "items": items}


@mcp.tool()
def get_price_history(
    article_number: str,
    database: str | None = None,
) -> dict:
    """매물(article_number) 단위 가격 변동 이력을 반환한다."""
    items = price_history(
        article_number=article_number,
        database=database,
    )
    return {"count": len(items), "items": items}


if __name__ == "__main__":
    mcp.run()
