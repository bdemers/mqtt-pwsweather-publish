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
import re
from datetime import datetime, timedelta
from statistics import mean

# Log to STDOUT
logger = logging.getLogger("mqtt-weather")
logger.setLevel(logging.INFO)
consoleHandler = logging.StreamHandler()
logger.addHandler(consoleHandler)

# Component config
config = {}
config['pws_id'] = ""
config['pws_pass'] = ""
config['wu_id'] = ""
config['wu_key'] = ""
# config['weather_sensor_topic']
# config['pressure_sensor_topic']

# pws weather attribute mapping
pws_sub_topics = {}
pws_sub_topics['wind_dir_deg'] = "winddir"
pws_sub_topics['wind_avg_mi_h'] = "windspeedmph"
pws_sub_topics['humidity'] = "humidity"
pws_sub_topics['temperature_F'] = "tempf"
pws_sub_topics['time'] = "dateutc"
pws_sub_topics['dewpoint'] = 'dewptf'
pws_sub_topics['pressure_hg'] = 'baromin'

# weather underground attribute mapping
wu_sub_topics = {}
wu_sub_topics['wind_dir_deg'] = "winddir" # 0-360 instantaneous wind direction
wu_sub_topics['wind_avg_mi_h'] = "windspeedmph" # mph instantaneous wind speed
wu_sub_topics['humidity'] = "humidity"  # % outdoor humidity 0-100%
wu_sub_topics['temperature_F'] = "tempf"  # F outdoor temperature
wu_sub_topics['time'] = "dateutc"  # YYYY-MM-DD HH:MM:SS (mysql format)
wu_sub_topics['dewpoint'] = 'dewptf'  # F outdoor dewpoint F
wu_sub_topics['pressure_hg'] = 'baromin'  # barometric pressure inches

last_pressure_reading = {}

event_history = []

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
config['weather_sensor_topic'] = os.environ.get('WEATHER_TOPIC')
if config['weather_sensor_topic'] is None:
    logger.info("WEATHER_TOPIC is not set, exiting")
    raise sys.exit()

config['pressure_sensor_topic'] = os.environ.get('PRESSURE_TOPIC')
if config['pressure_sensor_topic'] is None:
    logger.info("PRESSURE_TOPIC is not set, exiting")
    raise sys.exit()

# Get Weather Underground PWS ID
config['pws_id'] = os.environ.get('CONFIG_PWS_ID')
if config['pws_id'] is None:
    logger.info("CONFIG_PWS_ID is not set, exiting")
    raise sys.exit()

# Get PWS Weather Password
config['pws_pass'] = os.environ.get('CONFIG_PWS_PASS')
if config['pws_pass'] is None:
    logger.info("CONFIG_PWS_PASS is not set, exiting")
    raise sys.exit()

# Get PWS Weather ID
config['wu_id'] = os.environ.get('CONFIG_WU_ID')
if config['wu_id'] is None:
    logger.info("CONFIG_WU_ID is not set, exiting")
    raise sys.exit()

# Get Weather Underground PWS KEY
config['wu_key'] = os.environ.get('CONFIG_WU_KEY')
if config['wu_key'] is None:
    logger.info("CONFIG_WU_KEY is not set, exiting")
    raise sys.exit()

def normalize_weather_event(event):

    # Calculate dew point
    event['dewpoint'] = event['temperature_F'] - ((100.0 - event['humidity']) / 2.788)

    if event['time']:
        event['time'] = datetime.fromisoformat(event['time'])

    # if we have data from the pressure sensor add it to the event
    # updating the event allows for calculations of averages if/when needed
    if 'pressure_hg' in last_pressure_reading:
        event['pressure_hg'] = last_pressure_reading['pressure_hg']

    logger.info('event: ' + str(event))

    global event_history
    event_history.append(event)

    # remove all events older than 10 minutes
    ten_min_ago = datetime.utcnow() - timedelta(minutes=10)
    event_history = list(filter(lambda e: e['time'] > ten_min_ago, event_history))

    # get the 10 minute average wind speed
    speeds = list(map(lambda e: e['wind_avg_mi_h'], event_history))
    event['wind_10m_avg_mi_h'] = mean(speeds)
    return event

# Create the callbacks for Mosquitto
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info("Connected to broker " + str(config['broker_address'] + ":" + str(config['broker_port'])))

        # Subscribe to device config
        logger.info("Subscribing to device config at " + config['weather_sensor_topic'])
        client.subscribe(config['weather_sensor_topic'])
        logger.info("Subscribing to device config at " + config['pressure_sensor_topic'])
        client.subscribe(config['pressure_sensor_topic'])


def on_subscribe(mosq, obj, mid, granted_qos):
    logger.info("Subscribed with message ID " + str(mid) + " and QOS " + str(granted_qos) + " acknowledged by broker")


def on_message(mosq, obj, msg):
    payload_as_string = msg.payload.decode("utf-8")
    logger.info("Received message: " + msg.topic + ": " + payload_as_string)
    if msg.topic == config['weather_sensor_topic']:

        parsed_json = json.loads(payload_as_string)
        event = normalize_weather_event(parsed_json)
        send_pws_data(event)
        send_wu_data(event)

    elif msg.topic == config['pressure_sensor_topic']:

        parsed_json = json.loads(payload_as_string)
        global last_pressure_reading
        if parsed_json['time']:

            # this time value does NOT parse cleaning as the TZ part is formatted as 0000, instead of 00:00
            # for now just tweak the time string
            time = re.sub(r"(.*[\-\+]\d\d)(\d\d)", r"\1:\2", parsed_json['time'])
            parsed_json['time'] = datetime.fromisoformat(time)
        last_pressure_reading = parsed_json

def send_pws_data(event):
    pws_url = "http://www.pwsweather.com/pwsupdate/pwsupdate.php?" + \
              "&ID=" + urllib.parse.quote(config['pws_id']) + \
              "&PASSWORD=" + urllib.parse.quote(config['pws_pass'])

    send_get_request(pws_url, event, pws_sub_topics)

def send_wu_data(event):
    wu_url = "https://weatherstation.wunderground.com/weatherstation/updateweatherstation.php?action=updateraw" + \
             "&ID=" + config['wu_id'] + \
             "&PASSWORD=" + config['wu_key']

    send_get_request(wu_url, event, wu_sub_topics)

def send_get_request(base_url, data, argMap):

    request_url = base_url

    for key in data:
        # logger.info('item: ' + key + ' - ' + str(parsed_json[key]))
        if key in argMap:
            arg_name = argMap[key]
            value = urllib.parse.quote(str(data[key]))  # 2020-11-15T21:00:10
            if "time" == key:
                time = data[key]
                value = urllib.parse.quote_plus(time.strftime("%Y-%m-%d %H:%M:%S"))  # YYYY-MM-DD HH:MM:SS
            request_url += ('&' + arg_name + '=' + value)
    logger.info('url: ' + request_url)

    try:
        resonse = urllib2.urlopen(request_url)
    except urllib2.URLError as e:
        logger.error('URLError: ' + str(request_url) + ': ' + str(e.reason))
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
