from dotenv import load_dotenv
load_dotenv()

import os
import json
from datetime import datetime, timezone
import asyncio
import aiohttp

import logging
import sys
import base64
import paho.mqtt.client as mqtt
from typing import List, Dict, Any, Tuple, Optional

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
semaphore = asyncio.Semaphore(150)

from feed_parser import fetch_feed, FeedCache, find_new_items

def format_bytes(size):
    """Format bytes into a human-readable string."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024.0:
            return f"{size:.1f} {unit}"
        size /= 1024.0
    return f"{size:.1f} TB"

async def fetch_with_semaphore(
    session: aiohttp.ClientSession,
    url: str,
    progress_queue: asyncio.Queue,
    cache: FeedCache
) -> Tuple[List[Dict[str, Any]], FeedCache, bool, int]:
    """Fetch feed with a semaphore to limit concurrent requests.
    
    Args:
        session: HTTP client session
        url: URL to fetch
        progress_queue: Queue to report progress updates
        cache: Optional cache object for conditional requests
    
    Returns:
        Tuple containing:
            - List of feed items
            - Updated FeedCache object
            - Boolean indicating whether feed was modified
    """
    try:
        async with semaphore:
            items, was_modified, bytes_fetched = await fetch_feed(session, url, cache)
            await progress_queue.put((True, None))  # Report success
            return items, cache, was_modified, bytes_fetched
    except Exception as e:
        await progress_queue.put((False, str(e)))  # Report failure
        raise

def get_feed_filename(url: str) -> str:
    """Generate a filename for a feed based on its URL.
    
    Args:
        url: The URL of the feed
        
    Returns:
        A filename safe string based on base64 encoded URL
    """
    # Encode URL to base64 and remove padding characters
    url_base64 = base64.urlsafe_b64encode(url.encode()).decode().rstrip('=')
    return f"feed_{url_base64}.json"


def load_persisted_feed(file_path: str) -> Dict[str, Any]:
    """Load previously persisted feed data from a JSON file.
    
    Returns:
        Dictionary containing:
            - items: List of feed items
            - etag: ETag header value if present
            - last_modified: Last-Modified header value if present
    """
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            data = json.load(f)
            if isinstance(data, list):  # Handle old format
                return {'items': data, 'etag': None, 'last_modified': None}
            return data
    return {'items': [], 'etag': None, 'last_modified': None}

def save_feed_stats(feed_input_dir: str, feeds: List[Dict[str, Any]]) -> None:
    """Save updated feed stats back to the original JSON files.

    Args:
        feed_input_dir: Directory containing feed JSON files
        feeds: List of feed dictionaries with updated stats
    """
    # Group feeds by their source file
    feeds_by_file = {}
    for feed in feeds:
        source_file = feed.get('source_file')
        if source_file:
            if source_file not in feeds_by_file:
                feeds_by_file[source_file] = {'feeds': []}
            feeds_by_file[source_file]['feeds'].append(feed)

    # Save each group back to its source file
    for json_file, feed_config in feeds_by_file.items():
        file_path = os.path.join(feed_input_dir, json_file)
        try:
            with open(file_path, 'w') as f:
                json.dump(feed_config, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving stats to {json_file}: {str(e)}")

def save_feed(file_path: str, items: List[Dict[str, Any]], etag: Optional[str] = None, last_modified: Optional[str] = None) -> None:
    """Save feed items and caching headers to a JSON file.
    
    Args:
        file_path: Path to save the feed data
        items: List of feed items
        etag: Optional ETag header value
        last_modified: Optional Last-Modified header value
    """
    data = {
        'items': items,
        'etag': etag,
        'last_modified': last_modified
    }
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=2)


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
def load_feeds_from_directory(feed_input_dir: str) -> List[Dict[str, Any]]:
    """Load and combine feeds from all JSON files in the specified directory.

    Args:
        feed_input_dir: Path to directory containing feed JSON files

    Returns:
        List of feed dictionaries, each containing at least a 'url' key.
        Returns empty list if no feeds are found or if there are errors.
    """
    feeds = []

    if not os.path.exists(feed_input_dir):
        logger.error(f"Directory {feed_input_dir} not found. Please run extract.py first to generate feed files.")
        return feeds

    json_files = [f for f in os.listdir(feed_input_dir) if f.endswith('.json')]
    if not json_files:
        logger.error(f"No JSON files found in {feed_input_dir}. Please run extract.py first to generate feed files.")
        return feeds

    for json_file in json_files:
        try:
            with open(os.path.join(feed_input_dir, json_file), 'r') as f:
                feed_config = json.load(f)
                feed_list = feed_config.get('feeds', [])
                # Initialize stats if not present
                for feed in feed_list:
                    # Store the source file name for later saving
                    feed['source_file'] = json_file
                    if 'stats' not in feed:
                        feed['stats'] = {
                            'success_count': 0,
                            'failure_count': 0,
                            'last_success': None,
                            'last_failure': None,
                            'last_error': None
                        }
                feeds.extend(feed_list)
        except (json.JSONDecodeError, FileNotFoundError) as e:
            logger.error(f"Error reading {json_file}: {str(e)}")

    if not feeds:
        logger.error("No feeds found in any of the JSON files.")
    else:
        logger.info(f"Loaded {len(feeds)} feeds from {len(json_files)} files")

    return feeds


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

    # Load feeds from feed-input directory
    feeds = load_feeds_from_directory("feed-input")
    if not feeds:
        #mqtt_client.loop_stop()
        return

    feeds = feeds[:300]
    total_new_items = 0
    total_bytes_fetched = 0

    async with aiohttp.ClientSession() as session:
        total_feeds = len(feeds)
        fetch_successful = 0
        fetch_failed = 0
        progress_queue = asyncio.Queue()

        # Create tasks with progress queue
        tasks = []
        for feed in feeds:
            # Load persisted feed data and create cache object
            feed_file = os.path.join(feed_dir, get_feed_filename(feed['url']))
            feed_data = load_persisted_feed(feed_file)
            cache = FeedCache(feed_data['etag'], feed_data['last_modified'])
            tasks.append(fetch_with_semaphore(session, feed['url'], progress_queue, cache))
        
        # Start a background task to monitor progress
        async def monitor_progress():
            processed = 0
            while processed < total_feeds:
                success, error = await progress_queue.get()
                processed += 1
                if success:
                    nonlocal fetch_successful
                    fetch_successful += 1
                else:
                    nonlocal fetch_failed
                    fetch_failed += 1
                if processed % 100 == 0 or processed == total_feeds:
                    progress = (processed / total_feeds) * 100
                    logger.info(f"Fetching Progress: {progress:.1f}% ({processed}/{total_feeds} feeds fetched, {fetch_successful} successful, {fetch_failed} failed)")

        # Start both the fetching and monitoring tasks
        progress_task = asyncio.create_task(monitor_progress())
        rss_feeds = await asyncio.gather(*tasks, return_exceptions=True)
        await progress_task  # Ensure progress monitoring completes

        # Reset counters for processing phase
        successful_count = 0
        failed_count = 0

        for i, rss_feed in enumerate(rss_feeds):
            if (i + 1) % 100 == 0:
                progress = ((i + 1) / total_feeds) * 100
                logger.info(f"Progress: {progress:.1f}% ({i + 1}/{total_feeds} feeds processed, {successful_count} successful, {failed_count} failed)")
            if isinstance(rss_feed, Exception):
                logger.error(f"Error fetching feed {i} with url {feeds[i]['url']}: {rss_feed}")
                failed_count += 1
                # Update failure stats
                feeds[i]['stats']['failure_count'] += 1
                feeds[i]['stats']['last_failure'] = datetime.now(timezone.utc).isoformat()
                feeds[i]['stats']['last_error'] = str(rss_feed)
            else:
                items, cache, was_modified, bytes_fetched = rss_feed
                total_bytes_fetched += bytes_fetched
                successful_count += 1
                # Update success stats
                feeds[i]['stats']['success_count'] += 1
                feeds[i]['stats']['last_success'] = datetime.now(timezone.utc).isoformat()
                feeds[i]['stats']['last_error'] = None

                # Check for new items and save feed data
                feed_file = os.path.join(feed_dir, get_feed_filename(feeds[i]['url']))
                feed_data = load_persisted_feed(feed_file)
                
                # Find new items by comparing with persisted items
                new_items = find_new_items(feed_data['items'], items)
                if new_items or was_modified:
                    # Save the feed data with updated items and cache headers
                    save_feed(feed_file, items, cache.etag, cache.last_modified)
                    
                    # Process new items
                    feed_topic = f"rss/{feeds[i]['url']}"
                    for item in new_items:
                        mqtt_item = {
                            'title': item['title'],
                            'link': item['link'],
                            'description': item['description'],
                            'pubDate': item['pubDate']
                        }
                        mqtt_publish(f"{feed_topic}/item", json.dumps(mqtt_item))
                    total_new_items += len(new_items)
                    if new_items:
                        logger.info(f"Found {len(new_items)} new items in feed {feeds[i]['url']}")
                else:
                    logger.debug(f"Feed {feeds[i]['url']} not modified since last fetch")


        # Save updated feed stats after processing all feeds
        save_feed_stats("feed-input", feeds)

    logger.info(f"Total new items found: {total_new_items}")
    logger.info(f"Total data fetched: {format_bytes(total_bytes_fetched)}")
    logger.info(f"Final stats: {successful_count} feeds successful, {failed_count} feeds failed")
    #mqtt_client.loop_stop()

if __name__ == '__main__':
    asyncio.run(main())
