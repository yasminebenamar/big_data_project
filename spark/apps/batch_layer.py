"""
Spark Batch + Cassandra
Batch de séismes USGS + Agrégations analytiques
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, min, max, count,
    year, month, dayofmonth, weekofyear, hour,
    round, desc, to_date
)
from pyspark.sql.types import StringType, IntegerType 

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


def write_to_cassandra(df, table):
    """Helper générique pour écrire un DataFrame dans Cassandra."""
    (df.write
        .format("org.apache.spark.sql.cassandra")
        .options(table=table, keyspace="seismes")
        .mode("append")
        .save()
    )


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

    # Table principale
    session.execute('''
        CREATE TABLE IF NOT EXISTS seismes.earthquakes_batch (
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

    # Moyenne par jour
    session.execute('''
        CREATE TABLE IF NOT EXISTS seismes.avg_mag_per_day (
            day     DATE,
            avg_mag DOUBLE,
            PRIMARY KEY (day)
        );
    ''')

    # Moyenne par semaine
    session.execute('''
        CREATE TABLE IF NOT EXISTS seismes.avg_mag_per_week (
            year    INT,
            week    INT,
            avg_mag DOUBLE,
            PRIMARY KEY (year, week)
        );
    ''')

    # Min / Max / Count global
    session.execute('''
        CREATE TABLE IF NOT EXISTS seismes.stats_global (
            stat_key TEXT,
            min_mag  DOUBLE,
            max_mag  DOUBLE,
            total    BIGINT,
            PRIMARY KEY (stat_key)
        );
    ''')

    # Top 10 magnitudes
    session.execute('''
        CREATE TABLE IF NOT EXISTS seismes.top10_magnitudes (
            rank  INT,
            mag   DOUBLE,
            place TEXT,
            time  TIMESTAMP,
            PRIMARY KEY (rank)
        );
    ''')

    # Nombre de séismes par mois par région
    session.execute('''
        CREATE TABLE IF NOT EXISTS seismes.count_per_month_region (
            year  INT,
            month INT,
            place TEXT,
            total BIGINT,
            PRIMARY KEY ((year, month), place)
        );
    ''')

    # Régions les plus sismiques
    session.execute('''
        CREATE TABLE IF NOT EXISTS seismes.most_seismic_regions (
            place   TEXT,
            total   BIGINT,
            avg_mag DOUBLE,
            PRIMARY KEY (place)
        );
    ''')

    # Nombre de séismes par heure
    session.execute('''
        CREATE TABLE IF NOT EXISTS seismes.count_per_hour (
            hour  INT,
            total BIGINT,
            PRIMARY KEY (hour)
        );
    ''')

    spark = (SparkSession.builder
        .appName("Spark-Kafka-Cassandra-Earthquakes_Batch")
        .master("spark://spark-master:7077")
        .config("spark.driver.memory", "512m")
        .config("spark.executor.memory", "512m")
        .config("spark.cores.max", 1)
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.cassandra.connection.host", "cassandra")
        .config("spark.cassandra.connection.port", "9042")
        .getOrCreate()
    )

    df_historic = spark.read.parquet("./data_earthquakes_parquet")

    df_historic = (df_historic
        .withColumn("nst",       col("nst").cast("int"))
        .withColumn("rms",       col("rms").cast("double"))
        .withColumn("gap",       col("gap").cast("double"))
        .withColumn("mag",       col("mag").cast("double"))
        .withColumn("latitude",  col("latitude").cast("double"))
        .withColumn("longitude", col("longitude").cast("double"))
        .withColumn("depth",     col("depth").cast("double"))
    )

    # Écriture table principale
    write_to_cassandra(df_historic, "earthquakes_batch")

    # --- Moyenne par jour ---
    df_avg_day = (df_historic
        .withColumn("day", to_date(col("time")))
        .groupBy("day")
        .agg(round(avg("mag"), 3).alias("avg_mag"))
    )
    write_to_cassandra(df_avg_day, "avg_mag_per_day")

    # --- Moyenne par semaine ---
    df_avg_week = (df_historic
        .withColumn("year", year("time"))
        .withColumn("week", weekofyear("time"))
        .groupBy("year", "week")
        .agg(round(avg("mag"), 3).alias("avg_mag"))
    )
    write_to_cassandra(df_avg_week, "avg_mag_per_week")

    # --- Min / Max / Count global ---
    df_stats = (df_historic
        .agg(
            round(min("mag"), 3).alias("min_mag"),
            round(max("mag"), 3).alias("max_mag"),
            count("*").alias("total")
        )
        .withColumn("stat_key", col("total").cast("string"))
    )
    write_to_cassandra(df_stats, "stats_global")

    # --- Top 10 magnitudes ---
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number

    df_top10 = (df_historic
    .select("mag", "place", "time")
    .orderBy(desc("mag"))
    .limit(10)
)

    # 2. Ajouter le rank en INT (pas Long)
    window_top = Window.orderBy(desc("mag"))
    df_top10 = (df_top10
        .withColumn("rank", row_number().over(window_top).cast(IntegerType()))
    )

    write_to_cassandra(df_top10, "top10_magnitudes")

    # --- Nombre de séismes par mois par région ---
    df_month_region = (df_historic
        .withColumn("year",  year("time"))
        .withColumn("month", month("time"))
        .groupBy("year", "month", "place")
        .agg(count("*").alias("total"))
    )
    write_to_cassandra(df_month_region, "count_per_month_region")

    # --- Régions les plus sismiques ---
    df_regions = (df_historic
        .groupBy("place")
        .agg(
            count("*").alias("total"),
            round(avg("mag"), 3).alias("avg_mag")
        )
        .orderBy(desc("total"))
    )
    write_to_cassandra(df_regions, "most_seismic_regions")

    # --- Nombre de séismes par heure ---
    df_per_hour = (df_historic
        .withColumn("hour", hour("time"))
        .groupBy("hour")
        .agg(count("*").alias("total"))
        .orderBy("hour")
    )
    write_to_cassandra(df_per_hour, "count_per_hour")


if __name__ == "__main__":
    main()