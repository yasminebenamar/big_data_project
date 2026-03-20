#!/bin/bash
 
echo "🛑 Arrêt du Producer..."
docker exec producer bash -c "pkill -f producer.py" 2>/dev/null
echo "✅ Producer arrêté"
 
echo "🛑 Arrêt du Speed Layer..."
docker exec spark-master bash -c "pkill -f speed_layer.py" 2>/dev/null
echo "✅ Speed Layer arrêté"
 
echo "🛑 Arrêt du Batch Layer (si en cours)..."
docker exec spark-master bash -c "pkill -f batch_layer.py" 2>/dev/null
echo "✅ Batch Layer arrêté"
 
echo "✅ Tout est arrêté !"