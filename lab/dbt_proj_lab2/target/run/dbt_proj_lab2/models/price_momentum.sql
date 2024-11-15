
  create or replace   view dev.analytics.price_momentum
  
   as (
    -- models/price_momentum.sql

WITH base_data AS (
    SELECT
        date,
        symbol,
        close,
        LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS previous_close
    FROM dev.raw_data.stockprice
)

SELECT
    date,
    symbol,
    close,
    previous_close,
    close - previous_close AS price_momentum
FROM base_data
WHERE previous_close IS NOT NULL
ORDER BY symbol, date
  );

