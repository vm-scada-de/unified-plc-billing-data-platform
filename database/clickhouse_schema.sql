-- ==============================================================================
-- ⚡️ HIGH-PERFORMANCE STREAMING LAYER: TIME-SERIES TELEMETRY LOGS
-- SYSTEM ENGINE: ClickHouse (Production-Ready Industrial Layout)
-- ==============================================================================

CREATE TABLE tags_realtime_archive
(
    `value_id` UInt32,
    `time_stmp` DateTime64(3, 'Europe/Moscow') CODEC(DoubleDelta, LZ4),
    `val` Float32 CODEC(Gorilla, LZ4),
    `quality` UInt8 CODEC(LZ4)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(time_stmp)
PRIMARY KEY (value_id, time_stmp)
ORDER BY (value_id, time_stmp)
SETTINGS index_granularity = 2048;

-- ------------------------------------------------------------------------------
-- 💡 INDUSTRIAL TSDB ARCHITECTURE METRICS & CODECS EXPLANATION:
-- 1. `time_stmp`: 3-digit millisecond precision tracking synced with Europe/Moscow.
-- 2. `DoubleDelta`: Specialized codec optimized for monotonic time-series counters.
-- 3. `Gorilla`: Advanced floating-point compression scheme reducing raw PLC values.
-- 4. `quality`: OPC-standard industrial status bits (e.g., 192 = Good, 0 = Bad).
-- 5. `index_granularity`: Tightened to 2048 rows for ultra-fast trend chart rendering.
-- ------------------------------------------------------------------------------
