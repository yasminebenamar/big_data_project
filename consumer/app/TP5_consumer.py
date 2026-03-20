import time
import json
from datetime import datetime
from kafka import KafkaAdminClient, KafkaConsumer

def consumer_from_kafka(topic,stations):

    stations_bikes={}
    stations_date={}

    stations={}
    

        
    consumer = KafkaConsumer("taxifares", bootstrap_servers='broker-1:19092',auto_offset_reset="earliest",
enable_auto_commit=True, auto_commit_interval_ms=1000)
    consumer.subscribe(["velib-stations"])

    city_stations={}
    # if consumer.empt
    for message in consumer:
        station = json.loads(message.value.decode())
        print(station)
        #print("da",datetime.utcfromtimestamp(station["last_update"]/1000).strftime('%Y-%m-%d %H:%M:%S'))
        #station_number = station["number"]
        #station_last_update = station["last_update"]

        #if station_number ==30:
        #    print(station)
        #name = station["name"]
        #available_bike_stands = station["available_bike_stands"]
            
        #if name not in city_stations:
        #    city_stations[name] = available_bike_stands
        #    #print("st",station_number, city_stations[station_number])

        #count_diff = available_bike_stands - city_stations[name]
        #if count_diff != 0:
        #    #print(available_bike_stands,city_stations[station_number])
        #    city_stations[name] = available_bike_stands
        #    print("{}{} {} ({})".format(
        #        "+" if count_diff > 0 else "",
        #        count_diff, station["address"], name
        #    ))
        #else:
        #    #print("0",station["address"])
    
    consumer.close()



def main():
    print("consumer : ")
    stations = {}
    topic = 'velib-stations'
    try:
        admin = KafkaAdminClient(bootstrap_servers='kafka:29092')
    except Exception:
        pass

    consumer_from_kafka(topic,stations)

if __name__ == "__main__":
    main()
    
