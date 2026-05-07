# 부동산 크롤 데이터 스키마 (RE_*)

네이버 부동산 크롤 결과를 적재하기 위한 MySQL 8 기준 스키마 문서입니다.

- **접두어**: `RE_` (Real Estate)
- **식별자 규칙**: 테이블/컬럼 모두 대문자
- **엔진/캐릭터셋**: `InnoDB` / `utf8mb4` / `utf8mb4_0900_ai_ci`
- **설계 컨셉**
  - `RE_COMPLEX`, `RE_PYEONG` = 거의 변하지 않는 **마스터**
  - `RE_LISTING` = **매물 마스터** (article 단위, SCD-2 방식 추적)
  - `RE_LIST_SNAP` = 매물의 **일별 가격/메모 스냅샷** (가격 변동 추적)
  - `RE_META` = 스키마 메타 정보(선택)

> MySQL 키워드 충돌 방지를 위해 매물 메모 컬럼은 `COMMENT` 대신 **`NOTE`** 로 명명했습니다.

---

## 1. 테이블 개요

| 테이블 | 설명 | PK | 주요 FK |
|---|---|---|---|
| `RE_COMPLEX` | 단지 마스터 | `COMPLEX_NO` | - |
| `RE_PYEONG` | 단지의 평형 마스터 | `PYEONG_ID` | `COMPLEX_NO → RE_COMPLEX` |
| `RE_LISTING` | 매물(article) 마스터 | `ARTICLE_NUMBER` | `COMPLEX_NO → RE_COMPLEX`, `PYEONG_ID → RE_PYEONG` |
| `RE_LIST_SNAP` | 매물 일별 가격/메모 스냅샷 | `ID` | `ARTICLE_NUMBER → RE_LISTING` |
| `RE_META` | 스키마 메타 정보 | `KEY` | - |

### 거래유형(`TRADE_TYPE`)

| 코드 | 의미 |
|---|---|
| `A1` | 매매 |
| `B1` | 전세 |
| `B2` | 월세 |

### 가격 컬럼 의미 (`RE_LIST_SNAP`)

| TRADE_TYPE | `PRICE` | `MONTHLY_RENT` |
|---|---|---|
| A1 (매매) | 매매가 (원) | NULL |
| B1 (전세) | 전세 보증금 (원) | NULL |
| B2 (월세) | 월세 보증금 (원) | 월세 (원) |

---

## 2. ERD (요약)

```
RE_COMPLEX (1) ───── (N) RE_PYEONG
     │                     │
     │ (1)                 │ (1)
     │                     │
     └───── (N) RE_LISTING ┘
                  │ (1)
                  │
                  └───── (N) RE_LIST_SNAP
```

---

## 3. DDL

```sql
-- =====================================================
-- RE_* : Real-Estate (Naver Land) schema
-- 단지/평형 = 마스터, 매물 = 일별 스냅샷
-- =====================================================
SET NAMES utf8mb4;

-- 1) 단지 마스터 ----------------------------------------------------
CREATE TABLE IF NOT EXISTS RE_COMPLEX (
  COMPLEX_NO            BIGINT        NOT NULL                 COMMENT '네이버 단지번호 (PK)',
  NAME                  VARCHAR(255)                           COMMENT '단지명',
  TYPE                  VARCHAR(10)                            COMMENT '단지 타입 코드 (예: A01)',
  REGION_DEPTH1         VARCHAR(50)                            COMMENT '시/도',
  REGION_DEPTH2         VARCHAR(50)                            COMMENT '시/군/구',
  REGION_DEPTH3         VARCHAR(50)                            COMMENT '읍/면/동',
  ROAD_ADDRESS          VARCHAR(255)                           COMMENT '도로명주소',
  JIBUN                 VARCHAR(50)                            COMMENT '지번',
  ZIP_CODE              VARCHAR(10)                            COMMENT '우편번호',
  TOTAL_HOUSEHOLDS      INT                                    COMMENT '총 세대수',
  TOTAL_DONGS           INT                                    COMMENT '총 동수',
  COMPLETION_DATE       VARCHAR(8)                             COMMENT '사용승인일 YYYYMMDD',
  CONSTRUCTION_COMPANY  VARCHAR(100)                           COMMENT '시공사',
  HEATING_METHOD        VARCHAR(20)                            COMMENT '난방 방식 코드',
  HEATING_FUEL          VARCHAR(20)                            COMMENT '난방 연료 코드',
  PARKING_PER_HOUSEHOLD DECIMAL(5,2)                           COMMENT '세대당 주차대수',
  FAR                   DECIMAL(8,2)                           COMMENT '용적률(%)',
  BCR                   DECIMAL(8,2)                           COMMENT '건폐율(%)',
  HIGHEST_FLOOR         INT                                    COMMENT '최고층',
  LOWEST_FLOOR          INT                                    COMMENT '최저층',
  LATITUDE              DECIMAL(10,7)                          COMMENT '위도',
  LONGITUDE             DECIMAL(10,7)                          COMMENT '경도',
  FIRST_SEEN            DATE                                   COMMENT '최초 수집일',
  LAST_UPDATED          DATETIME      DEFAULT CURRENT_TIMESTAMP
                                      ON UPDATE CURRENT_TIMESTAMP COMMENT '최근 갱신 시각',
  PRIMARY KEY (COMPLEX_NO),
  KEY IDX_RE_COMPLEX_REGION (REGION_DEPTH1, REGION_DEPTH2, REGION_DEPTH3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='[부동산] 네이버 단지 마스터';

-- 2) 단지 평형 마스터 -----------------------------------------------
CREATE TABLE IF NOT EXISTS RE_PYEONG (
  PYEONG_ID         BIGINT        NOT NULL AUTO_INCREMENT      COMMENT '평형 PK',
  COMPLEX_NO        BIGINT        NOT NULL                     COMMENT '단지 FK (RE_COMPLEX.COMPLEX_NO)',
  PYEONG_NUMBER     INT                                        COMMENT '단지 내 평형 순번 (pyeongs[].number)',
  NAME              VARCHAR(50)                                COMMENT '평형 표시명 (예: 80A)',
  NAME_TYPE         VARCHAR(10)                                COMMENT '평형 분류 코드 (A,B,C 등)',
  SUPPLY_AREA       DECIMAL(7,2)                               COMMENT '공급면적(㎡)',
  CONTRACT_AREA     DECIMAL(7,2)                               COMMENT '계약면적(㎡)',
  EXCLUSIVE_AREA    DECIMAL(7,2)                               COMMENT '전용면적(㎡)',
  ROOM_COUNT        INT                                        COMMENT '방 개수',
  BATH_COUNT        INT                                        COMMENT '욕실 개수',
  HOUSEHOLD_COUNT   INT                                        COMMENT '해당 평형 세대수',
  ENTRANCE_TYPE     VARCHAR(10)                                COMMENT '현관구조 코드 (10/20/30)',
  HALLWAY_LABEL     VARCHAR(20)                                COMMENT '현관구조 표시 (계단식/복도식/복합식)',
  DEFAULT_DIRECTION VARCHAR(10)                                COMMENT '평형 기본 방향',
  LAST_UPDATED      DATETIME      DEFAULT CURRENT_TIMESTAMP
                                  ON UPDATE CURRENT_TIMESTAMP  COMMENT '최근 갱신 시각',
  PRIMARY KEY (PYEONG_ID),
  UNIQUE KEY UQ_RE_PYEONG (COMPLEX_NO, PYEONG_NUMBER),
  KEY IDX_RE_PYEONG_COMPLEX (COMPLEX_NO),
  CONSTRAINT FK_RE_PYEONG_COMPLEX
    FOREIGN KEY (COMPLEX_NO) REFERENCES RE_COMPLEX(COMPLEX_NO)
    ON UPDATE CASCADE ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='[부동산] 단지 평형 마스터';

-- 3) 매물(article) 마스터 -------------------------------------------
CREATE TABLE IF NOT EXISTS RE_LISTING (
  ARTICLE_NUMBER   VARCHAR(20)   NOT NULL                      COMMENT '네이버 매물번호 (PK)',
  COMPLEX_NO       BIGINT        NOT NULL                      COMMENT '단지 FK',
  PYEONG_ID        BIGINT                                      COMMENT '평형 FK',
  TRADE_TYPE       VARCHAR(5)    NOT NULL                      COMMENT '거래유형 A1=매매,B1=전세,B2=월세',
  DONG_NAME        VARCHAR(50)                                 COMMENT '동 명 (예: 106동)',
  FLOOR_TEXT       VARCHAR(20)                                 COMMENT '층 표시 (예: 고/22, 5/25)',
  DIRECTION        VARCHAR(10)                                 COMMENT '방향 (남/동/WS 등)',
  FIRST_SEEN_DATE  DATE                                        COMMENT '최초 노출일',
  LAST_SEEN_DATE   DATE                                        COMMENT '마지막 노출일',
  IS_ACTIVE        TINYINT(1)    NOT NULL DEFAULT 1            COMMENT '최근 크롤 노출 여부 (1=활성)',
  PRIMARY KEY (ARTICLE_NUMBER),
  KEY IDX_RE_LISTING_COMPLEX (COMPLEX_NO),
  KEY IDX_RE_LISTING_PYEONG  (PYEONG_ID),
  KEY IDX_RE_LISTING_SEEN    (LAST_SEEN_DATE),
  KEY IDX_RE_LISTING_TRADE   (TRADE_TYPE),
  KEY IDX_RE_LISTING_ACTIVE  (IS_ACTIVE),
  CONSTRAINT FK_RE_LISTING_COMPLEX
    FOREIGN KEY (COMPLEX_NO) REFERENCES RE_COMPLEX(COMPLEX_NO)
    ON UPDATE CASCADE ON DELETE RESTRICT,
  CONSTRAINT FK_RE_LISTING_PYEONG
    FOREIGN KEY (PYEONG_ID)  REFERENCES RE_PYEONG(PYEONG_ID)
    ON UPDATE CASCADE ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='[부동산] 매물 마스터 (article 단위, SCD-2 추적)';

-- 4) 일별 가격/메모 스냅샷 -----------------------------------------
CREATE TABLE IF NOT EXISTS RE_LIST_SNAP (
  ID              BIGINT        NOT NULL AUTO_INCREMENT        COMMENT '스냅샷 PK',
  ARTICLE_NUMBER  VARCHAR(20)   NOT NULL                       COMMENT '매물 FK (RE_LISTING.ARTICLE_NUMBER)',
  SNAPSHOT_DATE   DATE          NOT NULL                       COMMENT '수집일 YYYY-MM-DD',
  PRICE           BIGINT                                       COMMENT '매매가 또는 전/월세 보증금 (원)',
  MONTHLY_RENT    BIGINT                                       COMMENT '월세 (원, B2 매물만)',
  NOTE            VARCHAR(500)                                 COMMENT '매물 설명/특이사항',
  PRIMARY KEY (ID),
  UNIQUE KEY UQ_RE_LIST_SNAP (ARTICLE_NUMBER, SNAPSHOT_DATE),
  KEY IDX_RE_LIST_SNAP_DATE (SNAPSHOT_DATE),
  CONSTRAINT FK_RE_LIST_SNAP_ARTICLE
    FOREIGN KEY (ARTICLE_NUMBER) REFERENCES RE_LISTING(ARTICLE_NUMBER)
    ON UPDATE CASCADE ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='[부동산] 매물 일별 가격/메모 스냅샷';

-- 5) (선택) 스키마 메타 ---------------------------------------------
CREATE TABLE IF NOT EXISTS RE_META (
  `KEY`   VARCHAR(50)  NOT NULL                                COMMENT '메타 키',
  VALUE   VARCHAR(255)                                         COMMENT '메타 값',
  PRIMARY KEY (`KEY`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='[부동산] 스키마 메타 정보';

INSERT INTO RE_META(`KEY`, VALUE) VALUES
  ('DOMAIN', 'real_estate'),
  ('SCHEMA_VERSION', '1')
ON DUPLICATE KEY UPDATE VALUE = VALUES(VALUE);
```

---

## 4. 컬럼 사전

### 4.1 `RE_COMPLEX`

| 컬럼 | 타입 | NULL | 기본 | 설명 |
|---|---|---|---|---|
| COMPLEX_NO | BIGINT | NO | - | 네이버 단지번호 (PK) |
| NAME | VARCHAR(255) | YES | - | 단지명 |
| TYPE | VARCHAR(10) | YES | - | 단지 타입 코드 (예: A01) |
| REGION_DEPTH1 | VARCHAR(50) | YES | - | 시/도 |
| REGION_DEPTH2 | VARCHAR(50) | YES | - | 시/군/구 |
| REGION_DEPTH3 | VARCHAR(50) | YES | - | 읍/면/동 |
| ROAD_ADDRESS | VARCHAR(255) | YES | - | 도로명주소 |
| JIBUN | VARCHAR(50) | YES | - | 지번 |
| ZIP_CODE | VARCHAR(10) | YES | - | 우편번호 |
| TOTAL_HOUSEHOLDS | INT | YES | - | 총 세대수 |
| TOTAL_DONGS | INT | YES | - | 총 동수 |
| COMPLETION_DATE | VARCHAR(8) | YES | - | 사용승인일 YYYYMMDD |
| CONSTRUCTION_COMPANY | VARCHAR(100) | YES | - | 시공사 |
| HEATING_METHOD | VARCHAR(20) | YES | - | 난방 방식 코드 |
| HEATING_FUEL | VARCHAR(20) | YES | - | 난방 연료 코드 |
| PARKING_PER_HOUSEHOLD | DECIMAL(5,2) | YES | - | 세대당 주차대수 |
| FAR | DECIMAL(8,2) | YES | - | 용적률(%) |
| BCR | DECIMAL(8,2) | YES | - | 건폐율(%) |
| HIGHEST_FLOOR | INT | YES | - | 최고층 |
| LOWEST_FLOOR | INT | YES | - | 최저층 |
| LATITUDE | DECIMAL(10,7) | YES | - | 위도 |
| LONGITUDE | DECIMAL(10,7) | YES | - | 경도 |
| FIRST_SEEN | DATE | YES | - | 최초 수집일 |
| LAST_UPDATED | DATETIME | YES | CURRENT_TIMESTAMP | 최근 갱신 시각 |

### 4.2 `RE_PYEONG`

| 컬럼 | 타입 | NULL | 기본 | 설명 |
|---|---|---|---|---|
| PYEONG_ID | BIGINT | NO | AUTO_INCREMENT | 평형 PK |
| COMPLEX_NO | BIGINT | NO | - | 단지 FK |
| PYEONG_NUMBER | INT | YES | - | 단지 내 평형 순번 |
| NAME | VARCHAR(50) | YES | - | 평형 표시명 (예: 80A) |
| NAME_TYPE | VARCHAR(10) | YES | - | 평형 분류 코드 (A, B, C 등) |
| SUPPLY_AREA | DECIMAL(7,2) | YES | - | 공급면적(㎡) |
| CONTRACT_AREA | DECIMAL(7,2) | YES | - | 계약면적(㎡) |
| EXCLUSIVE_AREA | DECIMAL(7,2) | YES | - | 전용면적(㎡) |
| ROOM_COUNT | INT | YES | - | 방 개수 |
| BATH_COUNT | INT | YES | - | 욕실 개수 |
| HOUSEHOLD_COUNT | INT | YES | - | 해당 평형 세대수 |
| ENTRANCE_TYPE | VARCHAR(10) | YES | - | 현관구조 코드 (10/20/30) |
| HALLWAY_LABEL | VARCHAR(20) | YES | - | 현관구조 표시 (계단식/복도식/복합식) |
| DEFAULT_DIRECTION | VARCHAR(10) | YES | - | 평형 기본 방향 |
| LAST_UPDATED | DATETIME | YES | CURRENT_TIMESTAMP | 최근 갱신 시각 |

### 4.3 `RE_LISTING`

| 컬럼 | 타입 | NULL | 기본 | 설명 |
|---|---|---|---|---|
| ARTICLE_NUMBER | VARCHAR(20) | NO | - | 네이버 매물번호 (PK) |
| COMPLEX_NO | BIGINT | NO | - | 단지 FK |
| PYEONG_ID | BIGINT | YES | - | 평형 FK |
| TRADE_TYPE | VARCHAR(5) | NO | - | 거래유형 A1/B1/B2 |
| DONG_NAME | VARCHAR(50) | YES | - | 동 명 |
| FLOOR_TEXT | VARCHAR(20) | YES | - | 층 표시 (예: 고/22) |
| DIRECTION | VARCHAR(10) | YES | - | 방향 |
| FIRST_SEEN_DATE | DATE | YES | - | 최초 노출일 |
| LAST_SEEN_DATE | DATE | YES | - | 마지막 노출일 |
| IS_ACTIVE | TINYINT(1) | NO | 1 | 최근 크롤 노출 여부 (1=활성) |

### 4.4 `RE_LIST_SNAP`

| 컬럼 | 타입 | NULL | 기본 | 설명 |
|---|---|---|---|---|
| ID | BIGINT | NO | AUTO_INCREMENT | 스냅샷 PK |
| ARTICLE_NUMBER | VARCHAR(20) | NO | - | 매물 FK |
| SNAPSHOT_DATE | DATE | NO | - | 수집일 |
| PRICE | BIGINT | YES | - | 매매가 또는 전/월세 보증금 (원) |
| MONTHLY_RENT | BIGINT | YES | - | 월세 (원, B2만) |
| NOTE | VARCHAR(500) | YES | - | 매물 설명/특이사항 |

> 유니크: `(ARTICLE_NUMBER, SNAPSHOT_DATE)` — 같은 매물의 같은 날짜 스냅샷은 1건.

### 4.5 `RE_META`

| 컬럼 | 타입 | NULL | 기본 | 설명 |
|---|---|---|---|---|
| KEY | VARCHAR(50) | NO | - | 메타 키 |
| VALUE | VARCHAR(255) | YES | - | 메타 값 |

---

## 5. 일일 적재 로직 (요약)

크롤이 끝난 시점, `TODAY = CURDATE()` 기준:

1. **`RE_COMPLEX` / `RE_PYEONG` UPSERT**
   - 거의 변하지 않으므로 `INSERT ... ON DUPLICATE KEY UPDATE`로 갱신.
2. **`RE_LISTING` UPSERT**
   - 새 매물 → INSERT (`FIRST_SEEN_DATE = TODAY`, `LAST_SEEN_DATE = TODAY`, `IS_ACTIVE = 1`).
   - 기존 매물 → `LAST_SEEN_DATE = TODAY`, `IS_ACTIVE = 1`, 메타(`FLOOR_TEXT`, `DIRECTION` 등) 갱신.
   - 이번 크롤에 안 잡힌 기존 매물 → 별도 단계에서 `IS_ACTIVE = 0` 처리.
3. **`RE_LIST_SNAP` INSERT**
   - 매물 1건 = 그날 1행. `UNIQUE(ARTICLE_NUMBER, SNAPSHOT_DATE)`로 중복 방지.

### 비활성화 처리 예시

```sql
UPDATE RE_LISTING
SET IS_ACTIVE = 0
WHERE LAST_SEEN_DATE < CURDATE()
  AND IS_ACTIVE = 1;
```

---

## 6. 자주 쓰는 JOIN 예시

### 6.1 특정 단지의 오늘 매물 목록

```sql
SELECT C.NAME    AS 단지,
       P.NAME    AS 평형,
       L.TRADE_TYPE,
       S.PRICE,
       S.MONTHLY_RENT,
       L.FLOOR_TEXT,
       L.DIRECTION,
       S.NOTE
FROM RE_LIST_SNAP S
JOIN RE_LISTING   L ON L.ARTICLE_NUMBER = S.ARTICLE_NUMBER
JOIN RE_PYEONG    P ON P.PYEONG_ID      = L.PYEONG_ID
JOIN RE_COMPLEX   C ON C.COMPLEX_NO     = L.COMPLEX_NO
WHERE S.SNAPSHOT_DATE = CURDATE()
  AND C.COMPLEX_NO = ?
ORDER BY P.SUPPLY_AREA, L.TRADE_TYPE, S.PRICE;
```

### 6.2 특정 매물의 가격 변동 이력

```sql
SELECT SNAPSHOT_DATE, PRICE, MONTHLY_RENT, NOTE
FROM RE_LIST_SNAP
WHERE ARTICLE_NUMBER = ?
ORDER BY SNAPSHOT_DATE;
```

### 6.3 평형별 오늘 매매 최저/평균/최고 (집계 테이블 없이)

```sql
SELECT C.NAME AS 단지, P.NAME AS 평형,
       MIN(S.PRICE) AS 최저,
       AVG(S.PRICE) AS 평균,
       MAX(S.PRICE) AS 최고,
       COUNT(*)     AS 매물수
FROM RE_LIST_SNAP S
JOIN RE_LISTING   L ON L.ARTICLE_NUMBER = S.ARTICLE_NUMBER
JOIN RE_PYEONG    P ON P.PYEONG_ID      = L.PYEONG_ID
JOIN RE_COMPLEX   C ON C.COMPLEX_NO     = L.COMPLEX_NO
WHERE S.SNAPSHOT_DATE = CURDATE()
  AND L.TRADE_TYPE    = 'A1'
GROUP BY C.NAME, P.NAME
ORDER BY C.NAME, P.SUPPLY_AREA;
```

### 6.4 어제 대비 가격이 변한 매물

```sql
SELECT T.ARTICLE_NUMBER, T.PRICE AS 오늘가, Y.PRICE AS 어제가,
       (T.PRICE - Y.PRICE) AS 변동
FROM RE_LIST_SNAP T
JOIN RE_LIST_SNAP Y
  ON Y.ARTICLE_NUMBER = T.ARTICLE_NUMBER
 AND Y.SNAPSHOT_DATE  = DATE_SUB(T.SNAPSHOT_DATE, INTERVAL 1 DAY)
WHERE T.SNAPSHOT_DATE = CURDATE()
  AND T.PRICE <> Y.PRICE;
```

---

## 7. 운영 메모

- 컬럼 코멘트 변경
  ```sql
  ALTER TABLE RE_COMPLEX
    MODIFY COLUMN TOTAL_HOUSEHOLDS INT COMMENT '총 세대수 (집계)';
  ```
- 테이블 코멘트 변경
  ```sql
  ALTER TABLE RE_COMPLEX COMMENT = '[부동산] 네이버 단지 마스터 v2';
  ```
- MySQL 5.7 환경이라면 콜레이션은 `utf8mb4_unicode_ci` 권장.
- 단지번호 자릿수가 더 길어질 가능성이 적다면 `BIGINT` → `INT UNSIGNED` 로도 충분.
- 매물/스냅샷 적재는 `INSERT ... ON DUPLICATE KEY UPDATE` 패턴으로 멱등성 유지 권장.
