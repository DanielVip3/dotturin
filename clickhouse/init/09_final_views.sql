CREATE VIEW IF NOT EXISTS twitch.dim_game_final AS
SELECT *
FROM twitch.dim_game
FINAL;

CREATE VIEW IF NOT EXISTS twitch.dim_date_final AS
SELECT *
FROM twitch.dim_date
FINAL;

CREATE VIEW IF NOT EXISTS twitch.dim_streamer_final AS
SELECT *
FROM twitch.dim_streamer
FINAL;

CREATE VIEW IF NOT EXISTS twitch.dim_language_final AS
SELECT *
FROM twitch.dim_language
FINAL;

CREATE VIEW IF NOT EXISTS twitch.fact_stream_hourly_final AS
SELECT *
FROM twitch.fact_stream_hourly
FINAL;