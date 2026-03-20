# Kappa architecture

The goal is to test kappa architecture in using kafka, spark streaming, cassandrfa and grafana to visualize the obtained result.

## Introduction

Let's imagine we've got a few snack machines and we want to scale infinitly our snack machines,
to increase our income. For this we would need some kind of data processing, to analyze our
snack machines. 

EXAMPLE: We need to know:
- Manage your home sensors (temperature)


For this processing this repository shows the Kappa Architecture. Focused on a simple question: "Observe your home temperature and compute some statistics."



## Kappa Architecture

#### Start the containers
```
bash TP7_start-kafka-spark-cassandra-producer.sh
```

#### Start the scripts
```
python TP7_start_kappa.py
```

#### Stop the script
```
python TP7_stop_generate.py
```

#### Stop your containers
```
bash TP7_stop-kafka-spark-cassandra-producer.sh
```


Figure 2: Kappa-Architecture

Whole kappa architecture is created out of few docker containers.

- **Kafka** with data-generator and control-center (http://localhost:19000/)
- **Cassandra** for saving different views
- **Spark** with workers and master (http://localhost:9080/)
- **Grafana** for visualization (http://localhost:3000/)

## Grafana Setup


### Connect to Apache Cassandra

1. Go to http://localhost:3000
2. login (username=admin, password=admin) # can be changed
3. Go to Connections -> Add new connection -> Apache Cassandra -> Install
```
   cassandra1:9042
```
4. Go to Connections -> Data sources -> Apache Cassandra
5. Fill up following entry -- Host: cassandra1:9042
```
   select item, valeur, time_stamp from demo.transactions5;
  ``` 
6. Save & Test




