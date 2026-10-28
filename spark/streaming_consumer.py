import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, window, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

def main():
    # 1. Inicializar la sesión de Spark
    spark = SparkSession.builder \
        .appName("RapidGo-Security-Streaming") \
        .master("local[*]") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 2. Definir el esquema JSON del evento de telemetría
    schema = StructType([
        StructField("courier_id", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("speed", DoubleType(), True),
        StructField("battery_level", IntegerType(), True),
        StructField("status", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])

    # 3. Leer el stream desde Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "driver_security_events") \
        .option("startingOffsets", "latest") \
        .load()

    # Extraer y parsear el valor JSON del mensaje de Kafka
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", col("timestamp").cast(TimestampType()))

    # 4. Aplicar reglas de alerta operacionales en tiempo real
    # Umbrales: Velocidad > 45 km/h y Batería < 15%
    alerted_df = parsed_df.withColumn(
        "speed_alert", col("speed") > 45.0
    ).withColumn(
        "battery_alert", col("battery_level") < 15
    )

    # 5. Agregación por ventana (Ej: Velocidad promedio en ventanas de 5 minutos)
    windowed_avg_df = alerted_df \
        .withWatermark("event_time", "10 minutes") \
        .groupBy(
            window(col("event_time"), "5 minutes", "2 minutes"),
            col("courier_id")
        ) \
        .agg(avg("speed").alias("avg_speed"))

    # 6. Escribir resultados en consola (para pruebas locales) y en Data Lake (Parquet)
    # Salida 1: Consola de eventos con alertas activas
    query_alerts = alerted_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    # Salida 2: Persistencia en Data Lake (Capa Processed - Parquet) con Checkpoint
    query_datalake = alerted_df.writeStream \
        .format("parquet") \
        .option("path", "data/processed/security_telemetry") \
        .option("checkpointLocation", "data/checkpoints/security_telemetry") \
        .outputMode("append") \
        .start()

    print("Spark Structured Streaming Consumer en ejecución...")
    
    # Mantener el streaming activo
    query_alerts.awaitTermination()

if __name__ == "__main__":
    main()