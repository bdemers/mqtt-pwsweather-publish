#!/usr/bin/python
"""
publish.py
Simple MQTT subscriber of weather data then publishing it to the WeatherUnderground API.
Uploads the current temperature, humidity, wind speed and wind direction from a given Personal Weather Station
"""

# IMPORTS
import urllib.request as urllib2
import urllib.parse
import json
import paho.mqtt.client as paho
import os
import logging
import sys
import datetime

# Log to STDOUT
logger = logging.getLogger("mqtt-wunderground")
logger.setLevel(logging.INFO)
consoleHandler = logging.StreamHandler()
logger.addHandler(consoleHandler)

# Component config
config = {}
config['pws_id'] = ""
config['pws_pass'] = ""

sub_topics = {}
sub_topics['wind_dir_deg'] = "winddir"
sub_topics['wind_avg_mi_h'] = "windspeedmph"
sub_topics['humidity'] = "humidity"
sub_topics['temperature_F'] = "tempf"
sub_topics['time'] = "dateutc"
sub_topics['dewpoint'] = 'dewptf'


# Get MQTT servername/address
# Supports Docker environment variable format MQTT_URL = tcp://#.#.#.#:1883
MQTT_URL = os.environ.get('MQTT_URL')
if MQTT_URL is None:
    logger.info("MQTT_URL is not set, using default localhost:1883")
    config['broker_address'] = "localhost"
    config['broker_port'] = 1883
else:
    config['broker_address'] = MQTT_URL.split("//")[1].split(":")[0]
    config['broker_port'] = 1883

# Get config topic
config['config_topic'] = os.environ.get('CONFIG_TOPIC')
if config['config_topic'] is None:
    logger.info("CONFIG_TOPIC is not set, exiting")
    raise sys.exit()

# Get Weather Underground PWS ID
config['pws_id'] = os.environ.get('CONFIG_PWS_ID')
if config['pws_id'] is None:
    logger.info("CONFIG_PWS_ID is not set, exiting")
    raise sys.exit()

# Get Weather Underground PWS KEY
config['pws_pass'] = os.environ.get('CONFIG_PWS_PASS')
if config['pws_pass'] is None:
    logger.info("CONFIG_PWS_PASS is not set, exiting")
    raise sys.exit()

# Create the callbacks for Mosquitto
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info("Connected to broker " + str(config['broker_address'] + ":" + str(config['broker_port'])))

        # Subscribe to device config
        logger.info("Subscribing to device config at " + config['config_topic'])
        client.subscribe(config['config_topic'])


def on_subscribe(mosq, obj, mid, granted_qos):
    logger.info("Subscribed with message ID " + str(mid) + " and QOS " + str(granted_qos) + " acknowledged by broker")


def on_message(mosq, obj, msg):
    payload_as_string = msg.payload.decode("utf-8")
    logger.info("Received message: " + msg.topic + ": " + payload_as_string)
    if msg.topic == config['config_topic']:

        parsed_json = json.loads(payload_as_string)

        # Calculate dew point
        parsed_json['dewpoint'] = parsed_json['temperature_F'] - ((100.0 - parsed_json['humidity']) / 2.788 )

        pws_url = "http://www.pwsweather.com/pwsupdate/pwsupdate.php?" + \
                 "&ID=" + urllib.parse.quote(config['pws_id']) + \
                 "&PASSWORD=" + urllib.parse.quote(config['pws_pass'])

        for key in parsed_json:
            # logger.info('item: ' + key + ' - ' + str(parsed_json[key]))
            if key in sub_topics:
                arg_name = sub_topics[key]
                value = urllib.parse.quote(str(parsed_json[key])) # 2020-11-15T21:00:10
                if "time" == key:
                    time = datetime.datetime.fromisoformat(parsed_json[key])
                    value = urllib.parse.quote_plus(time.strftime("%Y-%m-%d %H:%M:%S")) # YYYY-MM-DD HH:MM:SS
                pws_url += ('&' + arg_name + '=' + value)
        # logger.info('url: '+ pws_url)

        try:
            resonse = urllib2.urlopen(pws_url)
        except urllib2.URLError as e:
            logger.error('URLError: ' + str(pws_url) + ': ' + str(e.reason))
            return None
        except:
            import traceback
            logger.error('Exception: ' + traceback.format_exc())
            return None
        resonse.close()

def on_publish(mosq, obj, mid):
    # logger.info("Published message with message ID: "+str(mid))
    pass


# Create the Mosquitto client
mqttclient = paho.Client()

# Bind the Mosquitte events to our event handlers
mqttclient.on_connect = on_connect
mqttclient.on_subscribe = on_subscribe
mqttclient.on_message = on_message
mqttclient.on_publish = on_publish


# Connect to the Mosquitto broker
logger.info("Connecting to broker " + config['broker_address'] + ":" + str(config['broker_port']))
mqttclient.connect(config['broker_address'], config['broker_port'], 60)

# Start the Mosquitto loop in a non-blocking way (uses threading)
mqttclient.loop_forever()
