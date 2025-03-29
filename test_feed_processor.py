import unittest
from datetime import datetime
from feed_parser import normalize_feed_entry, find_new_items
import feedparser

class TestFeedProcessor(unittest.TestCase):
    def test_normalize_feed_entry(self):
        # Create a sample feedparser entry
        entry = feedparser.FeedParserDict()
        entry.title = "Test Title"
        entry.link = "http://example.com/test"
        entry.description = "Test Description"
        # Use published_parsed instead of published string
        entry.published_parsed = (2025, 3, 29, 22, 0, 0, 0, 0, 0)
        
        normalized = normalize_feed_entry(entry)
        
        self.assertEqual(normalized["title"], "Test Title")
        self.assertEqual(normalized["link"], "http://example.com/test")
        self.assertEqual(normalized["description"], "Test Description")
        self.assertEqual(normalized["pubDate"], "2025-03-29T21:00:00+00:00")

    def test_normalize_feed_entry_missing_fields(self):
        # Test with missing fields
        entry = feedparser.FeedParserDict()
        entry.title = "Test Title"
        # No link or description
        
        normalized = normalize_feed_entry(entry)
        
        self.assertEqual(normalized["title"], "Test Title")
        self.assertEqual(normalized["link"], "")
        self.assertEqual(normalized["description"], "")
        self.assertEqual(normalized["pubDate"], "")



class TestFeedComparison(unittest.TestCase):
    def test_find_new_items_empty_current(self):
        current = []
        fetched = [{"title": "Test", "link": "http://test.com", "description": "Test", "pubDate": "2025-03-29"}]
        
        new_items = find_new_items(current, fetched)
        self.assertEqual(len(new_items), 1)
        self.assertEqual(new_items[0]["title"], "Test")

    def test_find_new_items_no_new(self):
        item = {"title": "Test", "link": "http://test.com", "description": "Test", "pubDate": "2025-03-29"}
        current = [item]
        fetched = [item]
        
        new_items = find_new_items(current, fetched)
        self.assertEqual(len(new_items), 0)

    def test_find_new_items_mixed(self):
        current = [
            {"title": "Old", "link": "http://old.com", "description": "Old", "pubDate": "2025-03-28"},
        ]
        fetched = [
            {"title": "Old", "link": "http://old.com", "description": "Old", "pubDate": "2025-03-28"},
            {"title": "New", "link": "http://new.com", "description": "New", "pubDate": "2025-03-29"},
        ]
        
        new_items = find_new_items(current, fetched)
        self.assertEqual(len(new_items), 1)
        self.assertEqual(new_items[0]["title"], "New")

    def test_find_new_items_different_description(self):
        """Test that items with same key fields but different description are considered the same"""
        current = [
            {"title": "Test", "link": "http://test.com", "description": "Old desc", "pubDate": "2025-03-29"},
        ]
        fetched = [
            {"title": "Test", "link": "http://test.com", "description": "New desc", "pubDate": "2025-03-29"},
        ]
        
        new_items = find_new_items(current, fetched)
        self.assertEqual(len(new_items), 0)

if __name__ == '__main__':
    unittest.main()
