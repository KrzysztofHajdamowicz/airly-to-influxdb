#!/usr/bin/python3.5

# -*- coding: utf-8 -*-
import requests
import logging
import argparse
import math
from influxdb import InfluxDBClient

"""Python bridge between Airly.eu API and InfluxDB."""
"""This code is meant to be started by crond every 10 minutes."""

parser = argparse.ArgumentParser(description='Fetches air quality data from\
 Airly.eu sensor and pushes them into InfluxDB')
parser.add_argument("--verbose",
                    help='Set verbosity level',
                    choices=['DEBUG', 'INFO', 'WARNING', 'CRITICAL'],
                    default='CRITICAL'
                    )
parser.add_argument('--airly_sensorid',
                    help='''Airly.eu sensor ID,
                    fetch it from https://developer.airly.eu/docs''',
                    required=True
                    )
parser.add_argument('--airly_apikey',
                    help='''Airly.eu API access token,
                        get it on https://developer.airly.eu/docs''',
                    required=True
                    )
parser.add_argument('--airly_url',
                    help='Airly API URL',
                    default='https://airapi.airly.eu/v1/sensor/measurements'
                    )
parser.add_argument('--InfluxDB_host', default='localhost')
parser.add_argument('--InfluxDB_port', default='8086')
parser.add_argument('--InfluxDB_user', default='root')
parser.add_argument('--InfluxDB_password', default='root')
parser.add_argument('--InfluxDB_database', required=True)

args = parser.parse_args()
if args.verbose:
    logging.basicConfig(level=args.verbose)


def get_airly(airly_sensorid, airly_apikey, airly_url):
    payload = {
        'sensorId': airly_sensorid,
        'apikey': airly_apikey
    }
    logging.debug('Requesting JSON from ' + airly_url)
    response = requests.get(airly_url, params=payload)
    _airly_json = response.json()
    _airly_pm1 = math.floor(_airly_json['currentMeasurements']['pm1'])
    _airly_pm25 = math.floor(_airly_json['currentMeasurements']['pm25'])
    _airly_pm10 = math.floor(_airly_json['currentMeasurements']['pm10'])
    _airly_pressure = math.floor(_airly_json['currentMeasurements']['pressure'])
    _airly_humidity = math.floor(_airly_json['currentMeasurements']['humidity'])
    _airly_temperature = _airly_json['currentMeasurements']['temperature']
    logging.debug('Received PM1: ' + str(_airly_pm1))
    logging.debug('Received PM2.5: ' + str(_airly_pm25))
    logging.debug('Received PM10: ' + str(_airly_pm10))
    logging.debug('Received pressure: ' + str(_airly_pressure))
    logging.debug('Received humidity: ' + str(_airly_humidity))
    logging.debug('Received temperature: ' + str(_airly_temperature))

    return {
        'PM1': _airly_pm1,
        'PM25': _airly_pm25,
        'PM10': _airly_pm10,
        'pressure': _airly_pressure,
        'temperature': _airly_temperature,
        'humidity': _airly_humidity
    }


def wite_to_InfluxDB(InfluxDB_host,
                     InfluxDB_port,
                     InfluxDB_user,
                     InfluxDB_password,
                     InfluxDB_database,
                     values,
                     sensorId):
    client = InfluxDBClient()
    client = InfluxDBClient(
        host=InfluxDB_host,
        port=InfluxDB_port,
        username=InfluxDB_user,
        password=InfluxDB_password,
        database=InfluxDB_database
    )
    logging.debug('Connected to InfluxDB')
    json_body = [
        {
            "measurement": "AQI",
            "tags": {
                "sensorId": sensorId,
            },
            "fields": values
        }
    ]
    logging.debug(json_body)
    client.write_points(points=json_body, time_precision='ms')
    logging.info('Sent metrics to InfluxDB')


values = get_airly(airly_sensorid=args.airly_sensorid,
                   airly_apikey=args.airly_apikey,
                   airly_url=args.airly_url)
wite_to_InfluxDB(InfluxDB_host=args.InfluxDB_host,
                 InfluxDB_port=args.InfluxDB_port,
                 InfluxDB_user=args.InfluxDB_user,
                 InfluxDB_password=args.InfluxDB_password,
                 InfluxDB_database=args.InfluxDB_database,
                 values=values,
                 sensorId=args.airly_sensorid
                 )
