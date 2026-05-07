# crawl_naver_land

네이버 부동산(land.naver.com) 정보를 조회해 SQLite DB로 저장하는 크롤러입니다.

## 설치

```bash
python -m pip install -r requirements.txt
python -m playwright install chromium
```

## 실행

기본 실행(코드 내부 기본 지역 사용):

```bash
./crawler.py
```

특정 지역 실행:

```bash
./crawler.py --regions "서울시 강남구"
```

DB 경로 지정:

```bash
./crawler.py --regions "서울시 강남구" --db-path ./real_estate.db
```

원본(raw)만 저장하고 DB 저장은 건너뛰기:

```bash
./crawler.py --regions "서울시 강남구" --no-db-save --raw-output ./raw_test.json
```

세대수/거래유형 조건 지정:

```bash
# 500세대 이상 + 매매(A1)만
./crawler.py --regions "서울시 강남구" --min-households 500 --trade-types A1

# 전세(B1)만 raw 확인
./crawler.py --regions "서울시 강남구 신사동" --no-db-save --raw-output ./raw_test.json --trade-types B1

# 오피스텔만 + 월세(B2)만
./crawler.py --regions "서울시 강남구" --property-type officetel --trade-types B2

# 특정 단지 코드만 수집
./crawler.py --regions "서울시 강남구" --complex-nos 12345 67890

# DB 저장 없이 SCHEMA.md(RE_*) 기준 INSERT SQL만 출력
./crawler.py --regions "서울시 강남구" --trade-types B1 B2 --print-sql-only
```

## 결과

실행 후 SQLite DB 파일에 아래 테이블이 생성/갱신됩니다.

- `complexes`
- `prices`

## MCP 서버

MCP 서버 파일: `mcp_server.py`

```bash
python -m pip install -r requirements.txt
./mcp_server.py
```

제공 도구:

- `crawl_market`: 수집 실행 (임시 raw JSON 생성 후 기본은 자동 삭제)
- `search_listings`: 조건 검색 (거래유형/가격대/세대수/단지코드/유형) — MySQL `RE_*` 스키마 기반
- `get_market_trend`: 구/군 단위 매물 흐름 요약
- `recommend_for_budget`: 예산 기반 추천
- `get_complex_listings`: 단지 ID 기준 (기본 최신) 스냅샷 매물 목록
- `get_price_history`: 매물(article_number) 가격 변동 이력

> 조회 도구는 `MYSQL_HOST/PORT/USER/PASSWORD/DATABASE` 환경변수로 MySQL에 접속합니다.
> 기본값: 리눅스 로컬 MySQL(`127.0.0.1:3306`), 데이터베이스명 `brain`.
> 스키마는 [`SCHEMA.md`](./SCHEMA.md) 참고.

```bash
# 리눅스 로컬 MySQL 사용 예 (DB 이름: brain)
mysql -u root -p -e "CREATE DATABASE IF NOT EXISTS brain CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
mysql -u root -p brain < /path/to/schema.sql   # SCHEMA.md DDL 적용

export MYSQL_USER=root
export MYSQL_PASSWORD=*****
# MYSQL_HOST/PORT/DATABASE 는 기본값(127.0.0.1/3306/brain) 사용
```