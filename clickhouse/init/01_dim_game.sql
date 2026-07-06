CREATE TABLE IF NOT EXISTS dim_game
(
  game_id String,
  game_name String
)
ENGINE = MergeTree
ORDER BY game_id;