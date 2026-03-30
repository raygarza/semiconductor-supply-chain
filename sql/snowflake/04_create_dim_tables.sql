-- =============================================================
-- 04_create_dim_tables.sql
-- Creates all dimension tables in SEMICON_DB.GOLD schema
-- Run after: 01_create_warehouse.sql, 02_create_db.sql, 03_create_schema.sql
-- Author: Ray Garza | March 2026
-- =============================================================

USE DATABASE SEMICON_DB;
USE SCHEMA GOLD;
USE WAREHOUSE SEMICON_WH;

-- ── DIM_COUNTRY ───────────────────────────────────────────────
-- Country lookup — maps BACI numeric codes to ISO3 and full names
-- Source: BACI country code file shipped with dataset
CREATE TABLE IF NOT EXISTS DIM_COUNTRY (
    country_code        INTEGER,        -- BACI numeric code (matches i/j columns)
    iso3                VARCHAR(3),     -- ISO 3166-1 alpha-3 e.g. USA, TWN, CHN
    country_name        VARCHAR(100),   -- Full English name
    region              VARCHAR(50),    -- Geographic region for map grouping
    is_key_player       BOOLEAN,        -- Flag for Taiwan, China, US, NL, JP, KR
    PRIMARY KEY (country_code)
);

-- ── DIM_COMPANY ───────────────────────────────────────────────
-- Company metadata for financial analysis
-- Source: Manually compiled, verified against company IR pages
CREATE TABLE IF NOT EXISTS DIM_COMPANY (
    ticker              VARCHAR(15),    -- Stock ticker e.g. AMAT, TSM, 005930.KS
    company_name        VARCHAR(100),
    company_type        VARCHAR(20),    -- equipment/foundry/fabless/memory/sub_tier
    hq_country          VARCHAR(3),     -- ISO3 country code of headquarters
    hq_city             VARCHAR(50),
    founded_year        INTEGER,
    chips_act_recipient BOOLEAN,        -- Does this company have a CHIPS award?
    PRIMARY KEY (ticker)
);

-- ── DIM_HS_CODE ───────────────────────────────────────────────
-- Harmonized System product code descriptions
-- Source: World Customs Organization HS 2017 nomenclature
CREATE TABLE IF NOT EXISTS DIM_HS_CODE (
    hs6_code            VARCHAR(10),    -- 6-digit HS code as string e.g. 854231
    hs4_chapter         VARCHAR(4),     -- First 4 digits e.g. 8542
    hs2_section         VARCHAR(2),     -- First 2 digits e.g. 85
    description         VARCHAR(500),   -- Full WCO description
    semiconductor_category VARCHAR(30), -- Integrated Circuits / Discrete Semiconductors
    PRIMARY KEY (hs6_code)
);

-- ── DIM_YEAR ──────────────────────────────────────────────────
-- Year dimension for time intelligence in Power BI
CREATE TABLE IF NOT EXISTS DIM_YEAR (
    year                INTEGER,
    chips_act_passed    BOOLEAN,        -- 2022 = TRUE
    covid_impact        BOOLEAN,        -- 2020/2021 = TRUE
    chip_shortage       BOOLEAN,        -- 2021/2022 = TRUE
    china_export_controls BOOLEAN,      -- 2023+ = TRUE
    PRIMARY KEY (year)
);

-- Populate DIM_YEAR immediately — it's small and fully known
INSERT INTO DIM_YEAR VALUES
    (2018, FALSE, FALSE, FALSE, FALSE),
    (2019, FALSE, FALSE, FALSE, FALSE),
    (2020, FALSE, TRUE,  FALSE, FALSE),
    (2021, FALSE, TRUE,  TRUE,  FALSE),
    (2022, TRUE,  FALSE, TRUE,  FALSE),
    (2023, TRUE,  FALSE, FALSE, TRUE),
    (2024, TRUE,  FALSE, FALSE, TRUE),
    (2025, TRUE,  FALSE, FALSE, TRUE),
    (2026, TRUE,  FALSE, FALSE, TRUE);

-- Verify
SELECT * FROM DIM_YEAR ORDER BY year;