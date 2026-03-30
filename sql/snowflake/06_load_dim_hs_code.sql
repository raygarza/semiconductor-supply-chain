-- 06_load_dim_hs_code.sql
-- Populates HS code dimension with semiconductor-specific codes
-- Source: World Customs Organization HS 2017 nomenclature
-- Only includes codes relevant to this project (8541/8542 chapters)

CREATE TABLE IF NOT EXISTS SEMICON_DB.GOLD.DIM_HS_CODE (
    hs6_code                VARCHAR(10),
    hs4_chapter             VARCHAR(4),
    hs2_section             VARCHAR(2),
    description             VARCHAR(500),
    semiconductor_category  VARCHAR(30),
    PRIMARY KEY (hs6_code)
);

INSERT INTO SEMICON_DB.GOLD.DIM_HS_CODE VALUES
-- HS 8541 — Discrete Semiconductors
('854110', '8541', '85', 'Diodes',                                          'Discrete Semiconductors'),
('854121', '8541', '85', 'Transistors <1W',                                 'Discrete Semiconductors'),
('854129', '8541', '85', 'Transistors other',                               'Discrete Semiconductors'),
('854130', '8541', '85', 'Thyristors, diacs and triacs',                    'Discrete Semiconductors'),
('854140', '8541', '85', 'Photosensitive devices incl solar cells',         'Discrete Semiconductors'),
('854150', '8541', '85', 'LED devices',                                     'Discrete Semiconductors'),
('854160', '8541', '85', 'Mounted piezoelectric crystals',                  'Discrete Semiconductors'),
('854190', '8541', '85', 'Other discrete semiconductor devices',            'Discrete Semiconductors'),
-- HS 8542 — Integrated Circuits (THE chips)
('854231', '8542', '85', 'Processors and controllers',                      'Integrated Circuits'),
('854232', '8542', '85', 'Memories',                                        'Integrated Circuits'),
('854233', '8542', '85', 'Amplifiers',                                      'Integrated Circuits'),
('854239', '8542', '85', 'Other electronic integrated circuits',            'Integrated Circuits'),
('854290', '8542', '85', 'Parts of electronic integrated circuits',         'Integrated Circuits');

SELECT * FROM SEMICON_DB.GOLD.DIM_HS_CODE ORDER BY hs6_code;