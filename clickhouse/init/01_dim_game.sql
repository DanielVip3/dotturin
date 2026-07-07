CREATE TABLE IF NOT EXISTS dim_game
(
  game_id Int64,
  game_name String,

  inserted_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY game_id;