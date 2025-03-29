import xmltodict
import aiohttp
import asyncio
import os
import json
import xmltodict
import tempfile

def convert_elly_joel_feeds_to_json(xml_file_path, json_file_path):
    with open(xml_file_path) as f:
        feeds = xmltodict.parse(f.read())['opml']['body']['outline']
    
    feed_list = [{'url': feed['@xmlUrl']} for feed in feeds if '@xmlUrl' in feed]
    
    with open(json_file_path, 'w') as f:
        json.dump({'feeds': feed_list}, f, indent=2)


def convert_boyter_feeds_to_json(input, output):
    """
    Format: [{"url": "https://theconversation.com/au/articles.atom", "tags": ["news", "australia"], "failCount": 0, "lastCheckTime": 1725511008}, {"url": "https://www.techrepublic.com/rssfeeds/articles/", "tags": ["technology"], "failCount": 0, "lastCheckTime": 1725511008}, ...]
    """
    with open(input) as f:
        feeds = json.load(f)
    
    feed_list = [{'url': feed['url']} for feed in feeds]
    
    with open(output, 'w') as f:
        json.dump({'feeds': feed_list}, f, indent=2)


def download_file(url: str) -> str:
    """Download a file from a URL and return the path to the downloaded file."""
    async def download():
        # Generate a completely temporary filename
        tmp_filename = tempfile.NamedTemporaryFile().name
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    with open(tmp_filename, 'wb') as f:
                        f.write(await response.read())
                    return tmp_filename
                else:
                    raise Exception(f"Failed to download file: {response.status}")
    return asyncio.run(download())


if __name__ == '__main__':
    # Create feed-input directory if it doesn't exist
    feed_input_dir = 'feed-input'
    if not os.path.exists(feed_input_dir):
        os.makedirs(feed_input_dir)

    tmp_file = download_file('https://raw.githubusercontent.com/EllyLoel/RSS-feed-collection/refs/heads/main/subscriptions.xml')
    convert_elly_joel_feeds_to_json(tmp_file, os.path.join(feed_input_dir, 'elly_joel_feeds.json'))

    tmp_file2 = download_file('https://raw.githubusercontent.com/boyter/rss-feeds/refs/heads/main/feeds.json')
    convert_boyter_feeds_to_json(tmp_file2, os.path.join(feed_input_dir, 'boyter_feeds.json'))
