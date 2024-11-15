WITH base_data AS (
    SELECT
        date,
        symbol,
        close,
        LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS previous_close
    FROM dev.raw_data.stockprice
),

price_changes AS (
    SELECT
        date,
        symbol,
        close,
        close - previous_close AS price_change
    FROM base_data
    WHERE previous_close IS NOT NULL
),

gains_and_losses AS (
    SELECT
        date,
        symbol,
        close,
        CASE WHEN price_change > 0 THEN price_change ELSE 0 END AS gain,
        CASE WHEN price_change < 0 THEN ABS(price_change) ELSE 0 END AS loss
    FROM price_changes
),

averages AS (
    SELECT
        date,
        symbol,
        AVG(gain) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_gain,
        AVG(loss) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_loss
    FROM gains_and_losses
)

SELECT
    date,
    symbol,
    avg_gain,
    avg_loss,
    CASE 
        WHEN avg_loss = 0 THEN 100
        ELSE 100 - (100 / (1 + (avg_gain / avg_loss)))
    END AS rsi
FROM averages
ORDER BY symbol, date