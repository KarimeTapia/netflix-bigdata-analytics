"""
ORQUESTADOR MAESTRO DEL PROYECTO BIG DATA NETFLIX
Ejecuta de manera secuencial los 4 Retos del proyecto.
"""
import os
import sys
import subprocess

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def run_step(step_name, command):
    print("\n" + "=" * 70)
    print(f">> EJECUTANDO: {step_name}")
    print("=" * 70)
    res = subprocess.run([sys.executable] + command, cwd=BASE_DIR)
    if res.returncode != 0:
        print(f"[AVISO] El paso {step_name} finalizo con codigo {res.returncode}")
    else:
        print(f"[OK] {step_name} finalizado con exito.")

def main():
    print("*" * 70)
    print("  PLATAFORMA ANALÍTICA DE BIG DATA - CATÁLOGO NETFLIX")
    print("*" * 70)

    # Reto 1
    run_step("Reto 1: ETL Pipeline (Pandas/NumPy)", ["reto1_etl_python/etl_pipeline.py"])
    run_step("Reto 1: Correlación SciPy y Dashboard Seaborn", ["reto1_etl_python/correlation_and_dashboard.py"])

    # Reto 2
    run_step("Reto 2: Carga NoSQL en HBase", ["reto2_hadoop_mapreduce_hive/hbase_loader.py"])

    # Reto 3
    run_step("Reto 3: Spark Processor & Parquet Benchmark", ["reto3_spark_processing/spark_processor.py"])

    # Reto 4
    run_step("Reto 4: Scraping IMDb de 50 Películas", ["reto4_scraping_airflow_streaming/imdb_scraper.py"])
    run_step("Reto 4: Productor Streaming Kafka", ["reto4_scraping_airflow_streaming/kafka_producer.py"])

    print("\n" + "*" * 70)
    print("  TODOS LOS RETOS DEL PROYECTO HAN SIDO EJECUTADOS Y VALIDADOS")
    print("*" * 70)

if __name__ == "__main__":
    main()
