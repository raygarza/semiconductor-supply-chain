-- 07_load_chips_act.sql
-- Loads CHIPS Act awards data into Snowflake
-- Source: Manually compiled from SIA tracker + GAO Report GAO-26-107882
-- See source_registry.csv SRC005, SRC006, SRC007, SRC008

CREATE TABLE IF NOT EXISTS SEMICON_DB.GOLD.CHIPS_ACT_AWARDS (
    company                 VARCHAR(100),
    state                   VARCHAR(20),
    city                    VARCHAR(100),
    chips_type              VARCHAR(30),
    announced_amount_b      FLOAT,
    finalized_amount_b      FLOAT,
    status                  VARCHAR(20),
    date_proposed           DATE,
    date_finalized          DATE,
    expected_completion     INTEGER,
    notes                   VARCHAR(1000)
);