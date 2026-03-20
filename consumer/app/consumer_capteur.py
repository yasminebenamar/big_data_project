import time
import json
from datetime import datetime
from kafka import KafkaAdminClient, KafkaConsumer

def consumer_from_kafka(topic,stations):


        
    consumer = KafkaConsumer("kafka-demo-events", bootstrap_servers='broker-1:29092',auto_offset_reset="earliest",
enable_auto_commit=True, auto_commit_interval_ms=1000)
    consumer.subscribe(["kafka-demo-events"])

    # if consumer.empt
    for message in consumer:
        capteur = json.loads(message.value.decode())
        
        print(capteur)
    
    consumer.close()



def main():
    print("consumer : ")
    stations = {}
    topic = 'kafka-demo-events'
    try:
        admin = KafkaAdminClient(bootstrap_servers='broker-1:29092')
    except Exception:
        pass

        while True:
        print("b")
        consumer_from_kafka(topic,stations)
        time.sleep(15)


if __name__ == "__main__":
    main()
    
