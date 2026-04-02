-- 11_load_dim_country.sql
-- Country dimension table
-- Source: BACI country code documentation + UN M49 standard
-- Note: BACI code 490 = Taiwan (UN "Other Asia NES" classification)

CREATE TABLE IF NOT EXISTS SEMICON_DB.GOLD.DIM_COUNTRY (
    country_code    INTEGER,
    iso3            VARCHAR(3),
    country_name    VARCHAR(100),
    region          VARCHAR(50),
    is_key_player   BOOLEAN,
    PRIMARY KEY (country_code)
);

INSERT INTO SEMICON_DB.GOLD.DIM_COUNTRY VALUES
(490, 'TWN', 'Taiwan',        'East Asia',        TRUE),
(156, 'CHN', 'China',         'East Asia',        TRUE),
(410, 'KOR', 'South Korea',   'East Asia',        TRUE),
(458, 'MYS', 'Malaysia',      'Southeast Asia',   FALSE),
(702, 'SGP', 'Singapore',     'Southeast Asia',   FALSE),
(392, 'JPN', 'Japan',         'East Asia',        TRUE),
(842, 'USA', 'United States', 'North America',    TRUE),
(704, 'VNM', 'Vietnam',       'Southeast Asia',   FALSE),
(608, 'PHL', 'Philippines',   'Southeast Asia',   FALSE),
(276, 'DEU', 'Germany',       'Europe',           FALSE),
(528, 'NLD', 'Netherlands',   'Europe',           TRUE),
(764, 'THA', 'Thailand',      'Southeast Asia',   FALSE),
(356, 'IND', 'India',         'South Asia',       FALSE),
(348, 'HUN', 'Hungary',       'Europe',           FALSE),
(203, 'CZE', 'Czech Republic','Europe',           FALSE),
(372, 'IRL', 'Ireland',       'Europe',           FALSE),
(826, 'GBR', 'United Kingdom','Europe',           FALSE),
(250, 'FRA', 'France',        'Europe',           FALSE),
(724, 'ESP', 'Spain',         'Europe',           FALSE),
(380, 'ITA', 'Italy',         'Europe',           FALSE),
(36,  'AUS', 'Australia',     'Oceania',          FALSE),
(124, 'CAN', 'Canada',        'North America',    FALSE),
(76,  'BRA', 'Brazil',        'South America',    FALSE),
(484, 'MEX', 'Mexico',        'North America',    FALSE),
(682, 'SAU', 'Saudi Arabia',  'Middle East',      FALSE),
(360, 'IDN', 'Indonesia',     'Southeast Asia',   FALSE),
(116, 'KHM', 'Cambodia',      'Southeast Asia',   FALSE),
(40,  'AUT', 'Austria',       'Europe',           FALSE),
(56,  'BEL', 'Belgium',       'Europe',           FALSE);

SELECT * FROM SEMICON_DB.GOLD.DIM_COUNTRY ORDER BY is_key_player DESC, country_name;
