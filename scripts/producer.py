from dotenv import load_dotenv
import os
import requests
import json
from confluent_kafka import Producer

load_dotenv()

config = {
  "bootstrap.servers": "kafka:29092",
  "client.id": "twitch-producer",
  "message.max.bytes": 10485760
}

producer = Producer(config)

CLIENT_ID = os.environ.get("TWITCH_CLIENT_ID")
CLIENT_SECRET = os.environ.get("TWITCH_CLIENT_SECRET")

OAUTH_URL = "https://id.twitch.tv/oauth2/token"
API_STREAMS_URL = "https://api.twitch.tv/helix/streams?first=100"

def producer_callback(err, msg):
  if err is not None:
    print(f"[-] Delivery error: {err}")
  else:
    print(f"[+] Message sent to {msg.topic()}")

def get_twitch_token():
  """Get OAuth2 token for Twitch API."""

  response = requests.post(OAUTH_URL, params={
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET,
    "grant_type": "client_credentials"
  })

  response.raise_for_status()
  return response.json()['access_token']

def fetch_top_streams(token):
  """Fetch top 100 live streams of the moment from Twitch API."""

  response = requests.get(API_STREAMS_URL, headers={
    "Client-ID": CLIENT_ID,
    "Authorization": f"Bearer {token}"
  })
  
  if response.status_code == 200:
    return response.json()
  else:
    print(f"[-] API Error! Status code: {response.status_code} - {response.text}")

    return None

def main():
  try:
    print("[*] Generating Twitch OAuth token...")
    token = get_twitch_token()

    print(f"\n[*] API request...")
      
    data = fetch_top_streams(token)
    
    if data is not None:
      payload = json.dumps(data)
      
      producer.produce(
        topic=os.environ.get("TOPIC_NAME"), 
        value=payload.encode('utf-8'), 
        callback=producer_callback
      )

      # TEMPORARY for testing [TODO: remove]
      producer.flush()
   
  except Exception as e:
    print(f"[-] Runtime error: {e}")

if __name__ == "__main__":
  print("[*] Starting Kafka producer...")
  main()