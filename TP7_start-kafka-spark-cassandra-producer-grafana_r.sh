cd kafka
docker compose -f docker-compose_apache_mono_kafdrop.yml up -d
cd ../spark
docker compose -f docker-compose_mono.yml up -d
cd ../cassandra
docker compose -f docker-compose_mono.yml up -d
cd ../grafana
docker compose  up -d
cd ../producer
docker compose up -d