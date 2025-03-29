"""MQTT publisher module for publishing feed updates."""
import os
import json
import logging
import paho.mqtt.client as mqtt
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class MQTTPublisher:
    """Handles MQTT connection and publishing."""
    def __init__(self):
        self.broker = os.environ.get('MQTT_HOST', 'gcmb.io')
        self.client_id = os.environ['MQTT_CLIENT_ID']
        self.username = os.environ['MQTT_USERNAME']
        self.password = os.environ['MQTT_PASSWORD']
        self.port = 8883
        self.client: Optional[mqtt.Client] = None

    def connect(self) -> None:
        """Connect to MQTT broker."""
        def on_connect(client, userdata, flags, rc, properties):
            if rc == 0:
                logger.info("Connected to MQTT Broker")
            else:
                logger.error(f"Failed to connect, return code {rc}")

        def on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
            logger.warning(f"Disconnected from MQTT Broker, return code {reason_code}")

        self.client = mqtt.Client(
            client_id=self.client_id,
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            reconnect_on_failure=True
        )
        self.client.tls_set(ca_certs='/etc/ssl/certs/ca-certificates.crt')
        self.client.username_pw_set(self.username, self.password)
        self.client.on_connect = on_connect
        self.client.on_disconnect = on_disconnect

        self.client.connect_async(self.broker, self.port)
        self.client.loop_start()

    def disconnect(self) -> None:
        """Disconnect from MQTT broker."""
        if self.client:
            self.client.loop_stop()

    def publish(self, topic: str, msg: str) -> None:
        """Publish a message to an MQTT topic."""
        if self.client:
            logger.info(f"Publishing '{msg}' to topic {topic}")
            self.client.publish(topic, msg)

    def publish_feed_item(self, feed_topic: str, item: Dict[str, Any]) -> None:
        """Publish a feed item to MQTT."""
        mqtt_item = {
            'title': item.get('title', ''),
            'link': item.get('link', ''),
            'description': item.get('description', ''),
            'pubDate': item.get('pubDate', '')
        }
        self.publish(f"{feed_topic}/item", json.dumps(mqtt_item))
