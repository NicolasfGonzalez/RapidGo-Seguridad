from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
)

def main():
    spark = SparkSession.builder \
        .appName("RapidGo-StreamingConsumer") \
        .getOrCreate()   

    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("courier_id", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("speed", DoubleType(), True),
        StructField("battery_level", IntegerType(), True),
        StructField("status", StringType(), True),
        StructField("timestamp", StringType(), True),
    ])

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "driver_security_events") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) AS json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", col("timestamp").cast(TimestampType()))

    alerted_df = parsed_df \
        .withColumn("speed_alert", col("speed") > 45.0) \
        .withColumn("battery_alert", col("battery_level") < 15) \
        .withColumn("event_date", col("event_time").cast("date"))

    windowed_avg_df = alerted_df \
        .withWatermark("event_time", "10 minutes") \
        .groupBy(window(col("event_time"), "5 minutes", "1 minute"), col("courier_id")) \
        .agg(avg("speed").alias("avg_speed"))

    # Salida 1: consola con alertas
    alerts_q = alerted_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    # Salida 2: Data Lake (Parquet)
    datalake_q = alerted_df.writeStream \
        .format("parquet") \
        .option("path", "/opt/app/datalake/processed/security_telemetry") \
        .option("checkpointLocation", "/opt/app/data/checkpoints/security_telemetry") \
        .partitionBy("event_date") \
        .trigger(processingTime="30 seconds") \
        .outputMode("append") \
        .start()

    # Salida 3: promedio por ventana 
    window_q = windowed_avg_df.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    print("Spark Structured Streaming Consumer en ejecución...")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()