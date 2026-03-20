"""
Demo Spark Structured Streaming + Apache Kafka + Cassandra
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,LongType,IntegerType,FloatType,StringType
from pyspark.sql.functions import split,sum,from_json,col
from pyspark.sql.functions import *

import sys
import uuid

scala_version = '2.13'
spark_version = '4.0.1' 
cassandra_version = '3.5.1' 

packages = [
    f'com.datastax.spark:spark-cassandra-connector_{scala_version}:{cassandra_version}',
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}'
]


def generate_uuid():
    """
    Generating UUID for tracking what spark does every step (only needed for validation)
    str(uuid.uuid4())
    :return: UUID
    """
    return str(uuid.uuid4())

uuid_udf = udf(generate_uuid, StringType())


def main():

    from cassandra.cluster import Cluster
    clstr=Cluster(['cassandra'])
    session=clstr.connect()

    qry=''' 
    CREATE KEYSPACE IF NOT EXISTS demo 
    WITH replication = {
        'class' : 'SimpleStrategy',
        'replication_factor' : 1
    };'''
	
    session.execute(qry) 

    qry=''' 
    CREATE TABLE IF NOT EXISTS demo.transactions5 (
        id uuid,
        item TEXT,
        valeur float,
        time_stamp TIMESTAMP,
        PRIMARY KEY ((id), time_stamp)
    );'''

    session.execute(qry) 

    capteurSchema = StructType([
                StructField("date",StringType(),False),
                StructField("numero",IntegerType(),False),
                StructField("capteur",StringType(),False),
                StructField("valeur",FloatType(),False)
            ])

    #capteurSchema = StructType([
    #            StructField("date",StringType(),False),
    #           StructField("valeur",FloatType(),False)
    #        ])

    spark = SparkSession.builder\
        .appName("Spark-Kafka-Cassandra")\
        .master("spark://spark-master:7077").config("spark.driver.memory", "1g")\
        .config("spark.jars.packages", ",".join(packages)) \
        .config("spark.cassandra.connection.host", "cassandra").config("spark.executor.memory", "1g").config("spark.cores.max",2) \
        .config("spark.cassandra.connection.port", "9042")\
        .config("spark.eventLog.enabled", "true")\
        .config("spark.eventLog.dir", "file:/tmp/spark-events")\
        .getOrCreate()


    spark.sparkContext.setLogLevel("ERROR")

    topic = 'kafka-demo-events'

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("subscribe", topic) \
        .option("delimeter",",") \
        .option("startingOffsets", "latest") \
        .load()

    df1 = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"),capteurSchema).alias("data")).select("data.*")

    df1 = df1.withColumn("id",uuid_udf()).withColumn("time_stamp",to_timestamp(col('date'), "yyyy-MM-dd HH:mm:ss")).withColumn("item",lit("capteur 1")).drop("date").drop("numero").drop("capteur")


    df1.printSchema()

    def writeToCassandra(writeDF, epochId):
        writeDF.write \
            .format("org.apache.spark.sql.cassandra")\
            .mode('append')\
            .options(table="transactions5", keyspace="demo")\
            .save()

    df1.writeStream \
        .option("spark.cassandra.connection.host","cassandra:9042")\
        .foreachBatch(writeToCassandra) \
        .outputMode("update") \
        .start()\
        .awaitTermination()
   
if __name__ == "__main__":
    main()
