from dotenv import load_dotenv
load_dotenv()

import os
import json
import asyncio
import aiohttp
import xmltodict
import logging
import sys
import paho.mqtt.client as mqtt
from typing import List, Dict, Any

# MQTT Configuration
broker = os.environ.get('MQTT_HOST', 'gcmb.io')
client_id = os.environ['MQTT_CLIENT_ID']
username = os.environ['MQTT_USERNAME']
password = os.environ['MQTT_PASSWORD']
port = 8883

# Configure logging
log_level = os.environ.get('LOG_LEVEL', 'INFO')
logger = logging.getLogger()
logger.setLevel(log_level)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# Semaphore to limit concurrent requests
semaphore = asyncio.Semaphore(50)

async def fetch_feed(session: aiohttp.ClientSession, url: str) -> List[Dict[str, Any]]:
    """Fetches and parses an RSS feed from the given URL.

    Args:
        session: HTTP client session for making requests
        url: URL of the RSS feed to fetch

    Returns:
        List of feed items from the RSS channel

    Raises:
        ValueError: If the response content type is not XML/RSS
    """
    async with session.get(url) as response:
        content_type = response.headers.get('Content-Type', '').lower()
        if content_type.startswith('text/xml') or content_type.startswith('application/xml') or content_type.startswith('application/rss+xml'):
            text = await response.text()
            feed = xmltodict.parse(text)
            items = feed.get('rss', {}).get('channel', {}).get('item', [])
            return items
        else:
            raise ValueError(f"Unexpected content type: {content_type}")

async def fetch_with_semaphore(session: aiohttp.ClientSession, url: str) -> List[Dict[str, Any]]:
    """Fetch feed with a semaphore to limit concurrent requests."""
    async with semaphore:
        return await fetch_feed(session, url)

def load_persisted_feed(file_path: str) -> List[Dict[str, Any]]:
    """Load previously persisted feed items from a JSON file."""
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            return json.load(f)
    return []

def save_feed(file_path: str, items: List[Dict[str, Any]]) -> None:
    """Save feed items to a JSON file."""
    with open(file_path, 'w') as f:
        json.dump(items, f)


def connect_mqtt():
    """Connect to MQTT broker and return client."""
    def on_connect(client, userdata, flags, rc, properties):
        if rc == 0:
            logger.info("Connected to MQTT Broker")
        else:
            logger.error(f"Failed to connect, return code {rc}")

    def on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
        logger.warning(f"Disconnected from MQTT Broker, return code {reason_code}")

    mqtt_client = mqtt.Client(client_id=client_id, callback_api_version=mqtt.CallbackAPIVersion.VERSION2, reconnect_on_failure=True)
    mqtt_client.tls_set(ca_certs='/etc/ssl/certs/ca-certificates.crt')
    mqtt_client.username_pw_set(username, password)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_disconnect = on_disconnect

    mqtt_client.connect_async(broker, port)
    return mqtt_client

#def mqtt_publish(mqtt.Client, topic: str, msg: str) -> None:
def mqtt_publish(topic: str, msg: str) -> None:
    """Publish message to MQTT topic."""
    #result = client.publish(topic, msg, retain=False)
    #status = result.rc
    #if status == 0:
    #    logger.debug(f"Sent '{msg}' to topic {topic}")
    #else:
    #    logger.warning(f"Failed to send message to topic {topic}, reason: {status}")
    logger.info(f"Publishing '{msg}' to topic {topic}")
#
async def main() -> None:
    """Main function to fetch and process RSS feeds."""
    # Connect to MQTT
    # mqtt_client = connect_mqtt()
    # mqtt_client.loop_start()

    # Create feed data directory if it doesn't exist
    feed_dir = "feed_data"
    if not os.path.exists(feed_dir):
        os.makedirs(feed_dir)

    # Load feed list from JSON
    try:
        with open("feeds.json", 'r') as f:
            feed_config = json.load(f)
            feeds = feed_config.get('feeds', [])
    except FileNotFoundError:
        logger.error("feeds.json not found. Please run extract.py first to convert XML to JSON format.")
        #mqtt_client.loop_stop()
        return

    total_new_items = 0

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_with_semaphore(session, feed['url']) for feed in feeds]
        rss_feeds = await asyncio.gather(*tasks, return_exceptions=True)

        for i, rss_feed in enumerate(rss_feeds):
            if isinstance(rss_feed, Exception):
                logger.error(f"Error fetching feed {i}: {rss_feed}")
            else:
                feed_file = os.path.join(feed_dir, f"feed_{i}.json")
                persisted_items = load_persisted_feed(feed_file)
                new_items = [item for item in rss_feed if item not in persisted_items]
                if new_items:
                    logger.info(f"Found {len(new_items)} new items in feed {i}")
                    save_feed(feed_file, rss_feed)
                    total_new_items += len(new_items)
                    
                    # Publish new items to MQTT
                    feed_topic = f"rss/feed/{i}"
                    for item in new_items:
                        # Create a simplified version of the item for MQTT
                        mqtt_item = {
                            'title': item.get('title', ''),
                            'link': item.get('link', ''),
                            'description': item.get('description', ''),
                            'pubDate': item.get('pubDate', '')
                        }
                        mqtt_publish(f"{feed_topic}/item", json.dumps(mqtt_item))

    logger.info(f"Total new items found: {total_new_items}")
    #mqtt_client.loop_stop()

if __name__ == '__main__':
    asyncio.run(main())
 