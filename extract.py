import xmltodict
import aiohttp
import asyncio
import os
import json
import xmltodict

def convert_feeds_to_json(xml_file_path, json_file_path):
    """Convert feeds from XML (OPML) format to JSON format.

    Args:
        xml_file_path (str): Path to the input XML file
        json_file_path (str): Path to save the JSON output
    """
    with open(xml_file_path) as f:
        feeds = xmltodict.parse(f.read())['opml']['body']['outline']
    
    feed_list = [{'url': feed['@xmlUrl']} for feed in feeds if '@xmlUrl' in feed]
    
    with open(json_file_path, 'w') as f:
        json.dump({'feeds': feed_list}, f, indent=2)

if __name__ == '__main__':
    convert_feeds_to_json('feeds.xml', 'feeds.json')

