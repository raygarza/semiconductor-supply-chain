CREATE WAREHOUSE IF NOT EXISTS semicon_wh
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  COMMENT = 'Semiconductor supply chain project warehouse';