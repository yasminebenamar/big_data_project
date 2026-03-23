#!/bin/bash

BATCH_INTERVAL=3600

# Fonction de vérification
check() {
    if [ $? -ne 0 ]; then
        echo "Erreur lors du lancement de $1"
        exit 1
    fi
}

# Lancer le Producer
docker exec -d producer bash -c "python3 /producer/app/producer.py > /tmp/producer.log 2>&1"
check "Producer"
echo "Producer démarré"

# Lancer le Speed Layer (streaming)
docker exec -d spark-master bash -c "python3 /opt/spark-apps/speed_layer.py > /tmp/speed.log 2>&1"
check "Speed Layer"
echo "Speed Layer démarré"

# Lancer Grafana (Docker) si pas déjà lancé
if [ -z "$(docker ps -q -f name=grafana)" ]; then
    docker run -d -p 3000:3000 --name=grafana grafana/grafana
    check "Grafana"
    echo "Grafana démarré sur http://localhost:3000"
else
    echo "Grafana déjà en cours d'exécution"
fi

echo "Attente génération données..."
sleep 60

# Batch périodique
while true; do
    echo "Lancement Batch Layer..."
    docker exec spark-master bash -c "python3 /opt/spark-apps/batch_layer.py > /tmp/batch.log 2>&1"
    if [ $? -eq 0 ]; then
        echo "Batch terminé. Prochain dans ${BATCH_INTERVAL}s"
    else
        echo "Batch échoué. Voir /tmp/batch.log. Nouvelle tentative dans ${BATCH_INTERVAL}s"
    fi
    sleep $BATCH_INTERVAL
done
