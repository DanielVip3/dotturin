CREATE TABLE fact_game_hourly
(
  game_id Int64,
  date_id UInt32,
  time_hour UInt8, /* degenerate time dimension */

  max_viewers UInt32,
  sum_viewers Float64,
  count_observations UInt32,
  total_active_channels UInt32,

  inserted_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (date_id, game_id, time_hour);