#!/usr/bin/env python3
from mcp.server.fastmcp import FastMCP

from market_query import (
    listings_for_complex,
    market_trend,
    price_history,
    recommend_by_budget,
    search_market,
)


mcp = FastMCP("naver-land-market")


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
