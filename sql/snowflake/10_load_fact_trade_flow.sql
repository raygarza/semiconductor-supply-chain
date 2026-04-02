-- 10_load_fact_trade_flow.sql
-- Verifies FACT_TRADE_FLOW loaded correctly after CSV upload
-- Source: CEPII BACI HS17 V202601 processed via Spark/Databricks
-- See source_registry.csv SRC001

-- Verify row count
SELECT COUNT(*) as row_count FROM SEMICON_DB.GOLD.FACT_TRADE_FLOW;

-- Verify year coverage
SELECT year, COUNT(*) as rows
FROM SEMICON_DB.GOLD.FACT_TRADE_FLOW
GROUP BY year
ORDER BY year;

-- Verify top exporters
SELECT exporter_name, 
       ROUND(SUM(trade_value_usd)/1e12, 2) as total_trillion_usd
FROM SEMICON_DB.GOLD.FACT_TRADE_FLOW
WHERE exporter_name IS NOT NULL
GROUP BY exporter_name
ORDER BY total_trillion_usd DESC
LIMIT 10;

-- Verify category split
SELECT semiconductor_category, COUNT(*) as rows
FROM SEMICON_DB.GOLD.FACT_TRADE_FLOW
GROUP BY semiconductor_category;