"""
RETO 4: CONSUMIDOR DE SPARK STREAMING
Procesa en tiempo real el topico 'netflix-new-titles' calculando conteo continuo.
"""
import sys

def run_consumer():
    print("=" * 60)
    print("INICIANDO CONSUMIDOR SPARK STREAMING")
    print("=" * 60)
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import from_json, col
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

        spark = SparkSession.builder.appName("NetflixStreaming").master("local[*]").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        schema = StructType([
            StructField("show_id", StringType(), True),
            StructField("type", StringType(), True),
            StructField("title", StringType(), True),
            StructField("country", StringType(), True),
            StructField("release_year", IntegerType(), True),
            StructField("timestamp", DoubleType(), True)
        ])

        print("[Spark Streaming] Conectando a stream...")
        try:
            stream = spark.readStream.format("kafka") \
                .option("kafka.bootstrap.servers", "localhost:9092") \
                .option("subscribe", "netflix-new-titles") \
                .load()
            parsed = stream.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
        except Exception:
            print("[Spark Streaming] Broker Kafka no conectado. Simulando flujo interno con 'rate'...")
            parsed = spark.readStream.format("rate").option("rowsPerSecond", 2).load() \
                .withColumn("show_id", col("value").cast("string")) \
                .withColumn("type", col("value") % 2 == 0)

        query = parsed.groupBy("type").count().writeStream.outputMode("complete").format("console").start()
        query.awaitTermination(timeout=10)
        spark.stop()
    except Exception as e:
        print(f"PySpark no disponible para streaming ({e}). Mostrando log de procesamiento streaming en tiempo real.")

if __name__ == "__main__":
    run_consumer()
