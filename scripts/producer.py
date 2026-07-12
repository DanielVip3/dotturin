from common import STREAM_AVRO_SCHEMA, GAME_AVRO_SCHEMA, TOPIC_STREAMS, TOPIC_GAMES
from dotenv import load_dotenv
import os
import requests
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

load_dotenv()

CLIENT_ID = os.environ.get("TWITCH_CLIENT_ID")
CLIENT_SECRET = os.environ.get("TWITCH_CLIENT_SECRET")

OAUTH_URL = "https://id.twitch.tv/oauth2/token"
API_STREAMS_URL = "https://api.twitch.tv/helix/streams?first=100"
API_GAMES_URL = "https://api.twitch.tv/helix/games"
API_IGDB_GAMES_URL = "https://api.igdb.com/v4/games"

sr_client = SchemaRegistryClient({"url": "http://schema-registry:8081"})

stream_avro_serializer = AvroSerializer(sr_client, STREAM_AVRO_SCHEMA)
game_avro_serializer = AvroSerializer(sr_client, GAME_AVRO_SCHEMA)
string_serializer = StringSerializer("utf_8")

producer = Producer(
  {
    "bootstrap.servers": "kafka:29092",
    "client.id": "twitch-producer",
    "message.max.bytes": 10485760,
  }
)


def producer_callback(err, msg):
  if err is not None:
    print(f"[-] Delivery error: {err}")
  else:
    print(f"[+] Message sent to {msg.topic()}")


def get_twitch_token():
  """Get OAuth2 token for Twitch API."""

  response = requests.post(
    OAUTH_URL,
    params={
      "client_id": CLIENT_ID,
      "client_secret": CLIENT_SECRET,
      "grant_type": "client_credentials",
    },
  )

  response.raise_for_status()
  return response.json()["access_token"]


def fetch_top_streams(token):
  """Fetch top 100 live streams of the moment from Twitch API."""

  response = requests.get(
    API_STREAMS_URL,
    headers={"Client-ID": CLIENT_ID, "Authorization": f"Bearer {token}"},
  )

  if response.status_code == 200:
    return response.json()
  else:
    print(f"[-] Twitch streams API Error! Status code: {response.status_code} - {response.text}")

    return None


def fetch_games_and_igdb(token, game_ids):
  """Fetch Twitch game metadata and enrich with IGDB data."""

  if not game_ids:
    return []

  auth_headers = {"Client-ID": CLIENT_ID, "Authorization": f"Bearer {token}"}

  # Fetch from Twitch games API
  response_twitch = requests.get(API_GAMES_URL, headers=auth_headers, params={"id": game_ids})

  if response_twitch.status_code != 200:
    print(f"[-] Twitch games API Error! Status: {response_twitch.status_code}")
    return []

  twitch_games = response_twitch.json().get("data", [])

  # Extract IGDB IDs and fetch from IGDB API
  igdb_ids = [g["igdb_id"] for g in twitch_games if g.get("igdb_id")]

  igdb_lookup = {}
  if igdb_ids:
    ids_string = ",".join(igdb_ids)

    # Apicalypse query syntax for IGDB
    query = (
      "fields id, "
      "summary, "
      "total_rating, "
      "total_rating_count, "
      "first_release_date, "
      "storyline, "
      "themes.name, "
      "player_perspectives.name, "
      "keywords.name, "
      "game_modes.name, "
      "platforms.name, "
      "platforms.platform_family.name, "
      "platforms.platform_type.name, "
      "url; "
      f"where id = ({ids_string}); "
      "limit 100;"
    )

    response_igdb = requests.post(API_IGDB_GAMES_URL, headers=auth_headers, data=query)

    if response_igdb.status_code == 200:
      igdb_games = response_igdb.json()
      igdb_lookup = {str(g["id"]): g for g in igdb_games}
    else:
      print(f"[-] IGDB games API Error! Status: {response_igdb.status_code} - {response_igdb.text}")

  # Enrich Twitch game data with IGDB data
  for game in twitch_games:
    igdb_id = game.get("igdb_id")

    # Attach IGDB dict, or None if the game isn't on IGDB
    game["igdb_data"] = igdb_lookup.get(igdb_id)

  return twitch_games


def main():
  try:
    print("[*] Generating Twitch OAuth token...")
    token = get_twitch_token()

    print("\n[*] Fetching top streams...")
    data = fetch_top_streams(token)

    if data is not None and "data" in data:
      stream_list = data["data"]

      # Produce each stream individually
      for stream in stream_list:
        producer.produce(
          topic=TOPIC_STREAMS,
          key=string_serializer(str(stream["id"])),
          value=stream_avro_serializer(stream, SerializationContext(TOPIC_STREAMS, MessageField.VALUE)),
          callback=producer_callback,
        )

      unique_game_ids = list({stream["game_id"] for stream in stream_list if stream.get("game_id")})

      print(f"[*] Fetching metadata for {len(unique_game_ids)} unique games...")
      games_data = fetch_games_and_igdb(token, unique_game_ids)

      if games_data:
        # Produce each game individually
        for game in games_data:
          producer.produce(
            topic=TOPIC_GAMES,
            key=string_serializer(str(game["id"])),
            value=game_avro_serializer(game, SerializationContext(TOPIC_GAMES, MessageField.VALUE)),
            callback=producer_callback,
          )

      # Block until all messages are delivered to Kafka
      producer.flush()

  except Exception as e:
    print(f"[-] Runtime error: {e}")


if __name__ == "__main__":
  print("[*] Starting Kafka producer...")
  main()
