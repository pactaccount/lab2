
  
    

        create or replace transient table dev.analytics.analysis
         as
        (WITH base_data AS (
    SELECT
        date,
        symbol,
        close,
        LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS previous_close
    FROM dev.raw_data.stockprice
),

-- Calculate price change (used for RSI calculation)
price_changes AS (
    SELECT
        date,
        symbol,
        close,
        close - previous_close AS price_change
    FROM base_data
    WHERE previous_close IS NOT NULL
),

-- Calculate gains and losses
gains_and_losses AS (
    SELECT
        date,
        symbol,
        CASE WHEN price_change > 0 THEN price_change ELSE 0 END AS gain,
        CASE WHEN price_change < 0 THEN ABS(price_change) ELSE 0 END AS loss
    FROM price_changes
),

-- Calculate the average gains and losses for RSI calculation
averages AS (
    SELECT
        date,
        symbol,
        AVG(gain) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_gain,
        AVG(loss) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_loss
    FROM gains_and_losses
),

-- Calculate RSI based on average gains and losses
rsi_data AS (
    SELECT
        date,
        symbol,
        CASE 
            WHEN avg_loss = 0 THEN 100
            ELSE 100 - (100 / (1 + (avg_gain / avg_loss)))
        END AS rsi
    FROM averages
),

-- Calculate the 7-day moving average
moving_avg_data AS (
    SELECT
        date,
        symbol,
        AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS moving_avg_7d
    FROM base_data
),

-- Use ROW_NUMBER to ensure we only take the first row per symbol and date
ranked_data AS (
    SELECT
        rsi_data.date,
        rsi_data.symbol,
        rsi_data.rsi,
        moving_avg_data.moving_avg_7d,
        ROW_NUMBER() OVER (PARTITION BY rsi_data.symbol, rsi_data.date ORDER BY rsi_data.date) AS rn
    FROM rsi_data
    JOIN moving_avg_data
        ON rsi_data.date = moving_avg_data.date AND rsi_data.symbol = moving_avg_data.symbol
)

-- Select only the rows with rank 1 (the first occurrence per symbol and date)
SELECT 
    date,
    symbol,
    rsi,
    moving_avg_7d
FROM ranked_data
WHERE rn = 1
ORDER BY symbol, date
        );
      
  