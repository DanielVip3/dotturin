CREATE TABLE fact_game_hourly
(
  game_id String,
  started_year UInt16,
  started_month UInt8,
  started_day UInt8,
  started_hour UInt8,

  max_concurrent_viewers UInt32,
  avg_viewers Float64,
  total_active_channels UInt32,

  version DateTime
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (game_id, started_year, started_month, started_day, started_hour);