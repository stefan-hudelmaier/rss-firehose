# Run via Docker

```
docker run --rm -it \
  -e MQTT_CLIENT_ID=123 \
  -e MQTT_USERNAME=123 \
  -e MQTT_PASSWORD=123 \
  -v $(pwd)/feed_data:/app/feed_data \
  -v $(pwd)/feed_input:/app/feed_input \
  ghcr.io/stefan-hudelmaier/rss-firehose:main
```
