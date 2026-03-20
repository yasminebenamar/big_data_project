import json
import time
import urllib.request
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic


def main():

    # --- Création du topic (une seule fois) ---
    admin = KafkaAdminClient(bootstrap_servers="broker:29092", client_id="topic_creator")
    server_topics = admin.list_topics()
    topic = "earthquakes"

    if topic not in server_topics:
        try:
            print("create new topic :", topic)
            topic1 = NewTopic(name=topic, num_partitions=1, replication_factor=1)
            admin.create_topics([topic1])
        except Exception as e:
            print("Erreur création topic :", e)
    else:
        print(topic, "est déjà créé")

    admin.close()

    # --- Création du producer (une seule fois) ---
    producer = KafkaProducer(bootstrap_servers=['broker:29092'])

    # --- Boucle principale ---
    while True:
        try:
            # Recalcul des dates à chaque itération
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(minutes=60)

            url = (
                "https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson"
                f"&starttime={start_time.strftime('%Y-%m-%dT%H:%M:%S')}"
                f"&endtime={end_time.strftime('%Y-%m-%dT%H:%M:%S')}"
            )

            response = urllib.request.urlopen(url, timeout=15)
            data = json.loads(response.read().decode("utf-8"))
            features = data["features"]

            print("Nombre de séismes :", len(features))

            for quake in features:
                message = {
                    "time": quake["properties"]["time"],
                    "mag": quake["properties"]["mag"],
                    "place": quake["properties"]["place"],
                    "rms": quake["properties"]["rms"],
                    "gap": quake["properties"]["gap"],
                    "nst": quake["properties"]["nst"],
                    "magType": quake["properties"]["magType"],
                    "dmin": quake["properties"]["dmin"],
                    "longitude": quake["geometry"]["coordinates"][0],
                    "latitude": quake["geometry"]["coordinates"][1],
                    "depth": quake["geometry"]["coordinates"][2],
                    "id": quake["id"]
                }

                try:
                    # Un seul send par message
                    future = producer.send(topic, json.dumps(message).encode("utf-8"))
                    record_metadata = future.get(timeout=30)
                    print("Message envoyé dans", record_metadata.topic,
                          "partition", record_metadata.partition,
                          "offset", record_metadata.offset)
                except Exception as e:
                    print(f"LE BROKER A REJETÉ LE MESSAGE : {e}")

            producer.flush()
            print("{} Produced {} earthquakes".format(
                datetime.fromtimestamp(time.time()), len(features)))

        except urllib.error.URLError as e:
            print(f"⚠️ Erreur réseau USGS, nouvelle tentative dans 10s : {e}")

        except Exception as e:
            print(f"⚠️ Erreur inattendue : {e}")

        time.sleep(10)


if __name__ == "__main__":
    main()
