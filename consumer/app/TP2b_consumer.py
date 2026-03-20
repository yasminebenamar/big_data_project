import logging
import os, time
import json

from kafka import KafkaAdminClient, KafkaConsumer

import sys

def consumer_from_kafka(topic,stations):
    consumer = KafkaConsumer(bootstrap_servers='kafka:29092',
                                 auto_offset_reset='earliest',
                                 consumer_timeout_ms=1000)
    consumer.subscribe(['velib-stations'])
    

    for message in consumer:
        station = json.loads(message.value.decode())
        station_number = station["number"]
        contract = station["contract_name"]
        available_bike_stands = station["available_bike_stands"]

        if contract not in stations:
            stations[contract] = {}
            city_stations = stations[contract]
        if station_number not in city_stations:
            city_stations[station_number] = available_bike_stands

        count_diff = available_bike_stands - city_stations[station_number]
        if count_diff != 0:
            city_stations[station_number] = available_bike_stands
            print("{}{} {} ({})".format(
                "+" if count_diff > 0 else "",
                count_diff, station["address"], contract
            ))

    consumer.close()

def main():
    print("consumer : ")
    stations = {}
    topic = 'velib-stations'
    try:
        admin = KafkaAdminClient(bootstrap_servers='kafka:29092')
    except Exception:
        pass

    while True:
        print("b")
        consumer_from_kafka(topic,stations)
        time.sleep(15)


if __name__ == "__main__":
    main()
    
