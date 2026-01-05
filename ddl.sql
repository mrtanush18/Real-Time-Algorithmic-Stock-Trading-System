CREATE OR REPLACE TABLE market_news (
    id BIGINT,
    author VARCHAR,
    headline VARCHAR,
    source VARCHAR,
    summary VARCHAR,
    data_provider VARCHAR,
    `url` VARCHAR,
    symbol VARCHAR,
    sentiment DECIMAL,
    timestamp_ms BIGINT,
    event_time AS TO_TIMESTAMP_LTZ(timestamp_ms, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'market-news',
    'properties.bootstrap.servers' = 'redpanda-1:29092,redpanda-2:29092',
    'properties.group.id' = 'test-group',
    'properties.auto.offset.reset' = 'earliest',
    'format' = 'json'
);


CREATE OR REPLACE TABLE stock_prices (
    symbol VARCHAR,
    `open` FLOAT,
    high FLOAT,
    low FLOAT,
    `close` FLOAT,
    volume DECIMAL,
    trade_count FLOAT,
    vwap DECIMAL,
    provider VARCHAR,
    `timestamp` BIGINT,
    event_time AS TO_TIMESTAMP_LTZ(`timestamp`, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'stock-prices',
    'properties.bootstrap.servers' = 'redpanda-1:29092,redpanda-2:29093',
    'properties.group.id' = 'test-group',
    'properties.auto.offset.reset' = 'earliest',
    'format' = 'json'
);



CREATE VIEW price_with_prev AS
SELECT
    symbol,
    event_time,  
    `close`,
    LAG(`close`) OVER (PARTITION BY symbol ORDER BY event_time) AS prev_close
FROM stock_prices;

CREATE OR REPLACE VIEW windowed_news AS
SELECT
    symbol,
    CAST(TUMBLE_START(event_time, INTERVAL '1' MINUTE) AS TIMESTAMP_LTZ(3)) AS window_start,
    CAST(TUMBLE_END(event_time, INTERVAL '1' MINUTE) AS TIMESTAMP_LTZ(3)) AS window_end,
    AVG(sentiment) AS avg_sentiment
FROM market_news
GROUP BY
    symbol,
    TUMBLE(event_time, INTERVAL '1' MINUTE);

CREATE VIEW trading_signal AS
SELECT
    pw.symbol,
    pw.event_time,
    CASE
        WHEN wn.avg_sentiment > 0.3 AND pw.`close` > pw.prev_close THEN 'BUY'
        WHEN wn.avg_sentiment < -0.3 AND pw.`close` < pw.prev_close THEN 'SELL'
        ELSE 'HOLD'
    END AS signal
FROM price_with_prev pw
JOIN windowed_news wn
ON pw.symbol = wn.symbol
AND pw.event_time BETWEEN wn.window_start AND wn.window_end;


CREATE TABLE trading_signals_sink (
symbol STRING,
event_time TIMESTAMP_LTZ(3),
signal STRING
) WITH (
'connector' = 'kafka',
'topic' = 'trading-signals',
'properties.bootstrap.servers' = 'redpanda-1:29092,redpanda-2:29092',
'properties.group.id' = 'trading-signals-group',
'format' = 'json'
);

INSERT INTO trading_signals_sink
SELECT
symbol,
event_time,
signal
FROM trading_signal;


CREATE TABLE sentiment_sink (
    symbol STRING,
    window_start TIMESTAMP_LTZ(3),
    avg_sentiment DOUBLE
) WITH (
    'connector' = 'kafka',
    'topic' = 'sentiment-trend',
    'properties.bootstrap.servers' = 'redpanda-1:29092,redpanda-2:29092',
    'format' = 'json'
);

INSERT INTO sentiment_sink
SELECT symbol, window_start, avg_sentiment
FROM windowed_news;