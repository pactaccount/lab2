
  create or replace   view dev.analytics.moving_averages
  
   as (
    WITH base_data AS (
    SELECT
        date,
        symbol,
        close
    FROM dev.raw_data.stockprice
)

SELECT
    date,
    symbol,
    close,
    AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS moving_avg_7d
FROM base_data
ORDER BY symbol, date
  );

