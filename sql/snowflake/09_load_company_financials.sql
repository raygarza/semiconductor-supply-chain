
CREATE TABLE IF NOT EXISTS SEMICON_DB.GOLD.COMPANY_FINANCIALS (
    ticker              VARCHAR(15),
    company_name        VARCHAR(100),
    company_type        VARCHAR(20),
    fiscal_year         INTEGER,
    revenue_b           FLOAT,
    rd_spend_b          FLOAT,
    capex_b             FLOAT,
    currency_note       VARCHAR(100)
);