"""
Spark Structured Streaming + Apache Kafka + Cassandra
Consumer de séismes USGS
"""

import uuid

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import (col, from_json, to_timestamp, udf,
                                   unix_timestamp, when)
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType, StructField,
                               StructType)

scala_version = '2.13'
spark_version = '4.0.1'
cassandra_version = '3.5.1'

packages = [
    f'com.datastax.spark:spark-cassandra-connector_{scala_version}:{cassandra_version}',
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}'
]


def generate_uuid():
    return str(uuid.uuid4())

uuid_udf = udf(generate_uuid, StringType())


def main():

    from cassandra.cluster import Cluster
    clstr = Cluster(['cassandra'])
    session = clstr.connect()

    session.execute('''
        CREATE KEYSPACE IF NOT EXISTS seismes
        WITH replication = {
            'class': 'SimpleStrategy',
            'replication_factor': 1
        };
    ''')

    session.execute('''
        CREATE TABLE IF NOT EXISTS seismes.earthquakes_streaming (
            id        UUID,
            time      TIMESTAMP,
            mag       DOUBLE,
            rms       DOUBLE,
            gap       DOUBLE,
            nst       INT,
            magtype   TEXT,
            place     TEXT,
            longitude DOUBLE,
            latitude  DOUBLE,
            depth     DOUBLE,
            PRIMARY KEY ((id), time)
        );
    ''')

    earthquakeSchema = StructType([
        StructField("time",      LongType(),    False),
        StructField("mag",       DoubleType(),  True),
        StructField("place",     StringType(),  True),
        StructField("rms",       DoubleType(),  True),
        StructField("gap",       DoubleType(),  True),
        StructField("nst",       IntegerType(), True),  # FIX: LongType -> IntegerType pour matcher Cassandra INT
        StructField("magType",   StringType(),  True),
        StructField("longitude", DoubleType(),  True),
        StructField("latitude",  DoubleType(),  True),
        StructField("depth",     DoubleType(),  True),
        StructField("id",        StringType(),  True)   
    ])

    spark = (SparkSession.builder
        .appName("Spark-Kafka-Cassandra-Earthquakes_Streaming")
        .master("spark://spark-master:7077")
        .config("spark.driver.memory", "512m")
        .config("spark.executor.memory", "512m")
        .config("spark.cores.max", 1)
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.cassandra.connection.host", "cassandra")
        .config("spark.cassandra.connection.port", "9042")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    df_raw = (spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "broker:29092")
        .option("subscribe", "earthquakes")
        .option("startingOffsets", "latest")
        .load()
    )

    df1 = (df_raw
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), earthquakeSchema).alias("data"))
        .select("data.*")
    )

    df1 = (df1
        .withColumn("id", uuid_udf())
        .withColumn("time", to_timestamp(col("time") / 1000))
    )
    
    
    # rename columns 
    df1 = df1.withColumnRenamed("magType", "magtype")
    
    # Data Quality 
    df1 = df1.withColumn(
    "is_reliable",
    (col("rms") < 1) & (col("gap") < 180) & (col("nst") > 10))

    df1 = df1.withColumn(
    "is_missing",
    col("mag").isNull() | col("latitude").isNull() | col("longitude").isNull())
    
    df_clean = df1.filter(col("is_reliable") & ~col("is_missing"))
    df_rejected = df1.filter(~col("is_reliable") | col("is_missing"))

    
    # Latency
    df_clean = df_clean.withColumn("latency_sec", unix_timestamp(current_timestamp()) - unix_timestamp(col("time")))
    
    # Alert Creation 
    df_clean = df_clean.withColumn(
    "alert_level",
    when(col("mag") >= 5, "CRITICAL")
    .when(col("mag") >= 4, "WARNING")
    .otherwise("NONE"))
    
    # FIX: density n'existe pas dans le schéma, on la crée directement avec lit(1)
    df_clean = df_clean.withColumn("density", lit(1))
    df_clean = df_clean.withColumn(
        "danger_index",
        col("mag")*0.6 + (1/col("depth"))*0.2 + col("density")*0.2
    )
    
    df_clean = df_clean.withColumn(
    "category",
    when(col("mag") < 3, "LOW")
    .when(col("mag") < 4, "MODERATE")
    .when(col("mag") < 5, "HIGH")
    .otherwise("CRITICAL")  
    )
    
    
    df_clean.printSchema()

    # --- Écriture Cassandra ---
    def writeToCassandra(writeDF, epochId):
        (writeDF.select("id", "time", "mag", "place", "rms", "gap", "nst", "magtype",
                    "longitude", "latitude", "depth")
            .write
            .format("org.apache.spark.sql.cassandra")
            .options(table="earthquakes_streaming", keyspace="seismes")
            .mode('append')
            .save()
        )
    df_processed = (df_clean
        .withColumn("year", year("time"))
        .withColumn("month", month("time"))
        .withColumn("day", dayofmonth("time"))
        .withColumn("hour", hour("time"))
    )
    
    

    query1 = df_processed.writeStream \
        .foreachBatch(writeToCassandra) \
        .outputMode("append")  \
        .start()
   
    # --- Écriture Parquet ---
    query2 = df_processed.writeStream \
        .format("parquet") \
        .option("path", "./data_earthquakes_parquet") \
        .option("checkpointLocation", "./checkpoints_parquet") \
        .partitionBy("year", "month", "day", "hour") \
        .outputMode("append") \
        .trigger(processingTime="60 seconds") \
        .start()

    spark.streams.awaitAnyTermination()
   


if __name__ == "__main__":
    main()