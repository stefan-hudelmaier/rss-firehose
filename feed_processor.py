"""Feed processor module for handling RSS/Atom feed processing."""
import os
import json
import base64
import logging
import asyncio
import aiohttp
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple, Optional

logger = logging.getLogger(__name__)

class ProgressMonitor:
    """Monitors progress of feed fetching and processing."""
    def __init__(self, total_feeds: int):
        self.total_feeds = total_feeds
        self.fetch_successful = 0
        self.fetch_failed = 0
        self.progress_queue: asyncio.Queue = asyncio.Queue()

    async def monitor_progress(self) -> None:
        """Monitor progress of feed fetching."""
        processed = 0
        while processed < self.total_feeds:
            success, error = await self.progress_queue.get()
            processed += 1
            if success:
                self.fetch_successful += 1
            else:
                self.fetch_failed += 1
            if processed % 100 == 0 or processed == self.total_feeds:
                progress = (processed / self.total_feeds) * 100
                logger.info(
                    f"Fetching Progress: {progress:.1f}% ({processed}/{self.total_feeds} "
                    f"feeds fetched, {self.fetch_successful} successful, {self.fetch_failed} failed)"
                )

class FeedProcessor:
    """Handles the processing of RSS/Atom feeds."""
    def __init__(self, feed_dir: str, feed_input_dir: str):
        self.feed_dir = feed_dir
        self.feed_input_dir = feed_input_dir
        self.semaphore = asyncio.Semaphore(150)

    def get_feed_filename(self, url: str) -> str:
        """Generate a filename for a feed based on its URL."""
        url_base64 = base64.urlsafe_b64encode(url.encode()).decode().rstrip('=')
        return f"feed_{url_base64}.json"

    def load_persisted_feed(self, file_path: str) -> List[Dict[str, Any]]:
        """Load previously persisted feed items from a JSON file."""
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                return json.load(f)
        return []

    def save_feed(self, file_path: str, items: List[Dict[str, Any]]) -> None:
        """Save feed items to a JSON file."""
        with open(file_path, 'w') as f:
            json.dump(items, f)

    def update_feed_stats(self, feed: Dict[str, Any], success: bool, error: Optional[str] = None) -> None:
        """Update feed statistics after a fetch attempt."""
        stats = feed.setdefault('stats', {
            'success_count': 0,
            'failure_count': 0,
            'last_success': None,
            'last_failure': None,
            'last_error': None
        })

        if success:
            stats['success_count'] += 1
            stats['last_success'] = datetime.now(timezone.utc).isoformat()
        else:
            stats['failure_count'] += 1
            stats['last_failure'] = datetime.now(timezone.utc).isoformat()
            stats['last_error'] = error

    def save_feed_stats(self, feeds: List[Dict[str, Any]]) -> None:
        """Save updated feed stats back to the original JSON files."""
        feeds_by_file = {}
        for feed in feeds:
            source_file = feed.get('source_file')
            if source_file:
                if source_file not in feeds_by_file:
                    feeds_by_file[source_file] = {'feeds': []}
                feeds_by_file[source_file]['feeds'].append(feed)

        for json_file, feed_config in feeds_by_file.items():
            file_path = os.path.join(self.feed_input_dir, json_file)
            try:
                with open(file_path, 'w') as f:
                    json.dump(feed_config, f, indent=2)
            except Exception as e:
                logger.error(f"Error saving stats to {json_file}: {str(e)}")

    async def process_feed_results(
        self, 
        feeds: List[Dict[str, Any]], 
        rss_feeds: List[Any],
        mqtt_publisher: Any
    ) -> Tuple[int, int, int]:
        """Process the results of feed fetching.
        
        Returns:
            Tuple of (successful_count, failed_count, total_new_items)
        """
        total_feeds = len(feeds)
        successful_count = 0
        failed_count = 0
        total_new_items = 0

        for i, rss_feed in enumerate(rss_feeds):
            if (i + 1) % 100 == 0:
                progress = ((i + 1) / total_feeds) * 100
                logger.info(
                    f"Progress: {progress:.1f}% ({i + 1}/{total_feeds} feeds processed, "
                    f"{successful_count} successful, {failed_count} failed)"
                )

            if isinstance(rss_feed, Exception):
                logger.error(f"Error fetching feed {i} with url {feeds[i]['url']}: {rss_feed}")
                failed_count += 1
                self.update_feed_stats(feeds[i], False, str(rss_feed))
            else:
                successful_count += 1
                self.update_feed_stats(feeds[i], True)

                feed_file = os.path.join(self.feed_dir, self.get_feed_filename(feeds[i]['url']))
                persisted_items = self.load_persisted_feed(feed_file)
                new_items = [item for item in rss_feed if item not in persisted_items]
                
                if new_items:
                    logger.info(f"Found {len(new_items)} new items in feed {i}")
                    self.save_feed(feed_file, rss_feed)
                    total_new_items += len(new_items)

                    if mqtt_publisher:
                        feed_topic = f"rss/feed/{base64.urlsafe_b64encode(feeds[i]['url'].encode()).decode().rstrip('=')}"
                        for item in new_items:
                            mqtt_publisher.publish_feed_item(feed_topic, item)

        # Save updated feed stats
        self.save_feed_stats(feeds)

        return successful_count, failed_count, total_new_items
