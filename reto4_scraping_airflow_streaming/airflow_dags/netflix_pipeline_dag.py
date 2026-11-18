"""
RETO 4: DAG EN APACHE AIRFLOW
Secuencia: Tarea 1 (Scraping/Ingesta) -> Tarea 2 (Spark ETL) -> Tarea 3 (Carga a Hive/PostgreSQL)
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "netflix_pipeline_dag",
    default_args=default_args,
    description="Pipeline secuencial Netflix: Scraping -> Spark -> Hive/PostgreSQL",
    schedule_interval="@daily",
    catchup=False,
    tags=["netflix", "bigdata"]
)

task1_scraping = BashOperator(
    task_id="tarea_1_scraping_ingesta",
    bash_command="python /opt/airflow/dags/scripts/imdb_scraper.py",
    dag=dag
)

task2_spark = BashOperator(
    task_id="tarea_2_spark_etl",
    bash_command="python /opt/airflow/dags/scripts/spark_processor.py",
    dag=dag
)

def carga_almacen():
    print("Cargando resultados refinados a Hive Metastore y PostgreSQL DW...")
    return True

task3_carga = PythonOperator(
    task_id="tarea_3_carga_hive_postgres",
    python_callable=carga_almacen,
    dag=dag
)

task1_scraping >> task2_spark >> task3_carga
