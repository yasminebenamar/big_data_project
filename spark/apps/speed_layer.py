"""
Spark Structured Streaming + Apache Kafka + Cassandra
Consumer de séismes USGS
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType,IntegerType
from pyspark.sql.functions import from_json, col, unix_timestamp, when, to_timestamp, udf, lit, expr, year, month, dayofmonth, hour, current_timestamp
from pyspark.sql.functions import *

import uuid

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

    # Nombre de séismes par type d'alertes
    session.execute('''
        CREATE TABLE IF NOT EXISTS seismes.alerts_count (
            alert_level  TEXT,
            total BIGINT,
            PRIMARY KEY (alert_level)
        );
    ''')

    # Nombre de séismes par catégorie
    session.execute('''
        CREATE TABLE IF NOT EXISTS seismes.category_count (
            category  TEXT,
            total BIGINT,
            PRIMARY KEY (category)
        );
    ''')

    earthquakeSchema = StructType([
        StructField("time",      LongType(),    False),
        StructField("mag",       DoubleType(),  True),
        StructField("place",     StringType(),  True),
        StructField("rms",       DoubleType(),  True),
        StructField("gap",       DoubleType(),  True),
        StructField("nst",       IntegerType(), True),  
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
        .option("failOnDataLoss", "false")
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
    col("mag").isNull() | col("latitude").isNull() | col("longitude").isNull()  | col("depth").isNull() | (col("depth") == 0))
    
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
                    "longitude", "latitude", "depth","category", "danger_index", "is_reliable", "is_missing", "alert_level")
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

    # --- Écriture des counts dans Cassandra ---
    def writeCountsToCassandra(batchDF, batchId):
        # Count par alert_level
        alert_count_df = (batchDF.groupBy("alert_level")
                        .count()
                        .withColumnRenamed("count", "total")
                        )
        # Upsert dans la table alerts_count
        alert_count_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="alerts_count", keyspace="seismes") \
            .mode("append") \
            .save()
        
        # Count par category
        category_count_df = (batchDF.groupBy("category")
                            .count()
                            .withColumnRenamed("count", "total")
                            )
        # Upsert dans la table category_count
        category_count_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="category_count", keyspace="seismes") \
            .mode("append") \
            .save()
        
    def foreachBatchFunction(batchDF, batchId):
        # Écriture des séismes dans la table principale
        writeToCassandra(batchDF, batchId)
        
        # Écriture des counts
        writeCountsToCassandra(batchDF, batchId)

    # Lancer le streaming avec counts
    query = df_processed.writeStream \
    .foreachBatch(foreachBatchFunction) \
    .outputMode("append") \
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