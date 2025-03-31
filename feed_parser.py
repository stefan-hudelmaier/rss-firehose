"""Feed parser module for handling RSS/Atom feed parsing using feedparser."""
import logging
import aiohttp
import feedparser
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone
from time import mktime

logger = logging.getLogger(__name__)

class FeedCache:
    """Class to handle feed caching headers."""
    def __init__(self, etag: Optional[str] = None, last_modified: Optional[str] = None, conditional_fetch_supported: Optional[bool] = None):
        self.etag = etag
        self.last_modified = last_modified
        self.conditional_fetch_supported = conditional_fetch_supported

    def get_headers(self) -> Dict[str, str]:
        """Get headers for conditional request."""
        headers = {}
        if self.etag:
            headers['If-None-Match'] = self.etag
        if self.last_modified:
            headers['If-Modified-Since'] = self.last_modified
        return headers

    def update_from_response(self, response: aiohttp.ClientResponse) -> None:
        """Update cache headers from response."""
        self.etag = response.headers.get('ETag')
        self.last_modified = response.headers.get('Last-Modified')
        
        # A feed supports conditional fetching if it provides either ETag or Last-Modified
        if self.conditional_fetch_supported is None:
            self.conditional_fetch_supported = bool(self.etag or self.last_modified)

def find_new_items(current_items: List[Dict[str, Any]], fetched_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Find new items in fetched_items that aren't in current_items.
    
    Args:
        current_items: List of currently stored feed items
        fetched_items: List of newly fetched feed items
        
    Returns:
        List of items that are new (not in current_items)
    """
    # Create a set of tuples of key fields for current items
    current_keys = {
        (str(item.get('title', '')), str(item.get('link', '')), str(item.get('pubDate', '')))
        for item in current_items
    }
    
    # Return items whose key fields don't exist in current_items
    return [
        item for item in fetched_items
        if (str(item.get('title', '')), str(item.get('link', '')), str(item.get('pubDate', ''))) not in current_keys
    ]


def normalize_feed_entry(entry: feedparser.FeedParserDict) -> Dict[str, Any]:
    """Normalize a feedparser entry to our standard format.

    Args:
        entry: A feedparser entry

    Returns:
        Dictionary with normalized feed item data
    """
    # Get the published date, trying different possible fields
    published = None
    if hasattr(entry, 'published_parsed') and entry.published_parsed:
        published = datetime.fromtimestamp(mktime(entry.published_parsed), timezone.utc).isoformat()
    elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
        published = datetime.fromtimestamp(mktime(entry.updated_parsed), timezone.utc).isoformat()

    return {
        'title': getattr(entry, 'title', ''),
        'link': getattr(entry, 'link', ''),
        'description': getattr(entry, 'description', '') or getattr(entry, 'summary', ''),
        'pubDate': published or ''
    }

async def fetch_feed(session: aiohttp.ClientSession, url: str, cache: FeedCache) -> Tuple[List[Dict[str, Any]], bool, int]:
    """Fetches and parses an RSS or Atom feed from the given URL.

    Args:
        session: HTTP client session for making requests
        url: URL of the feed to fetch
        cache: Optional cache object containing ETag and Last-Modified headers

    Returns:
        Tuple containing:
            - List of normalized feed items
            - Boolean indicating whether the feed was modified (True) or not (False)

    Raises:
        ValueError: If the feed cannot be parsed
    """
    headers = cache.get_headers()
    async with session.get(url, headers=headers, timeout=5) as response:
        # Update cache headers from response
        if cache:
            cache.update_from_response(response)

        # If feed hasn't changed, return empty list and False
        if response.status == 304:  # Not Modified
            logger.debug(f"Feed {url} not modified since last fetch")
            return [], False, 0

        if response.status != 200:
            raise ValueError(f"HTTP error {response.status}")
            
        content = await response.text()
        feed = feedparser.parse(content)
        
        if feed.bozo:  # feedparser encountered an error
            raise ValueError(str(feed.bozo_exception))
            
        bytes_fetched = len(content.encode('utf-8'))
        return [normalize_feed_entry(entry) for entry in feed.entries], True, bytes_fetched
