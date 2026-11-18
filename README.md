# Netflix Global Catalog: Big Data & Distributed ETL Pipeline

Proyecto práctico de Ingeniería de Datos y Sistemas Distribuidos que implementa un flujo completo de ingesta, limpieza, almacenamiento distribuido, procesamiento en memoria, web scraping, orquestación y streaming en tiempo real sobre el catálogo global de Netflix.

---

## Tabla de Contenido
- [1. Estructura del Proyecto](#1-estructura-del-proyecto)
- [2. Requisitos Previos](#2-requisitos-previos)
- [3. Ubicación de Ejecución](#3-ubicación-de-ejecución)
- [4. Instrucciones de Ejecución](#4-instrucciones-de-ejecución)
  - [4.1 Ejecución completa (Pipeline Maestro)](#41-ejecución-completa-pipeline-maestro)
  - [4.2 Ejecución independiente por reto](#42-ejecución-independiente-por-reto)
    - [Reto 1: Caracterización Big Data y Pipeline ETL con Python](#reto-1-caracterización-big-data-y-pipeline-etl-con-python)
    - [Reto 2: Ingesta, Almacenamiento Distribuido y MapReduce en Hadoop](#reto-2-ingesta-almacenamiento-distribuido-y-mapreduce-en-hadoop)
    - [Reto 3: Procesamiento Distribuido e In-Memory con Apache Spark](#reto-3-procesamiento-distribuido-e-in-memory-con-apache-spark)
    - [Reto 4: Integración Externa, Scraping, Orquestación y Streaming](#reto-4-integración-externa-scraping-orquestación-y-streaming)

---

## 1. Estructura del Proyecto

```text
TecnoE/
├── data/
│   ├── netflix_titles.csv                  # Dataset original (Kaggle)
│   ├── netflix_cleaned.csv                 # Dataset limpio y descompilado (Reto 1)
│   └── netflix_enriched.csv                # Dataset enriquecido con IMDb (Reto 4)
│
├── reto1_etl_python/
│   ├── 5_Vs_Justificacion_Tecnica.md       # Justificación técnica de las 5 V's
│   ├── etl_pipeline.py                     # Pipeline ETL (Pandas/NumPy)
│   ├── correlation_and_dashboard.py        # Análisis de correlación y visualización
│   └── output/
│       └── dashboard_reto1.png             # Dashboard estático de 4 cuadrantes
│
├── reto2_hadoop_mapreduce_hive/
│   ├── mr_actor_titles_count.py            # MapReduce con mrjob
│   ├── hbase_loader.py                     # Persistencia NoSQL en HBase
│   └── hive_schema_and_queries.hql         # Esquema y consultas HQL
│
├── reto3_spark_processing/
│   ├── spark_processor.py                  # PySpark, Parquet y transformaciones
│   └── output/
│       └── netflix_parquet/                # Datos particionados por release_year
│
├── reto4_scraping_airflow_streaming/
│   ├── imdb_scraper.py                     # Web Scraping con BeautifulSoup
│   ├── kafka_producer.py                   # Productor Kafka
│   ├── spark_streaming_consumer.py         # Consumidor Spark Streaming
│   └── airflow_dags/
│       └── netflix_pipeline_dag.py         # DAG para Apache Airflow
│
├── Dockerfile                              # Imagen Docker para el stack
├── docker-compose.yml                      # Orquestador Docker Compose
├── requirements.txt                        # Dependencias del proyecto
└── run_project.py                          # Orquestador maestro local
```

---

## 2. Requisitos Previos

* **Python 3.9 o superior.**
* **Terminal de comandos:** PowerShell (Windows) o Bash/Terminal (Linux/macOS).
* **Dependencias de Python:** se instalan con el siguiente comando, desde cualquier ubicación:
  ```bash
  pip install pandas numpy scipy matplotlib seaborn mrjob pyarrow pyspark kafka-python-ng psycopg2-binary beautifulsoup4 requests
  ```
* **Opcional (ejecución distribuida real):** Java 8/11/17/21, Apache Hadoop, Apache Spark, Apache Kafka y Apache Airflow (o Docker Desktop).

---

## 3. Ubicación de Ejecución

Todos los comandos de esta guía deben ejecutarse en una terminal (PowerShell o CMD) posicionada estrictamente en la carpeta raíz del proyecto:

```text
C:\Users\karim\TecnoE
```

**Para abrir la terminal directamente en esa ruta:**
1. Abrir el Explorador de Archivos de Windows y navegar hasta `C:\Users\karim\TecnoE`.
2. Hacer clic en la barra de direcciones superior, escribir `powershell` y presionar Enter.
3. Verificar que el indicador de la consola muestre `PS C:\Users\karim\TecnoE>`.

---

## 4. Instrucciones de Ejecución

### 4.1 Ejecución completa (Pipeline Maestro)

Desde la terminal posicionada en `C:\Users\karim\TecnoE`, ejecutar el siguiente comando para correr de forma secuencial la limpieza ETL, los análisis estadísticos, la generación de gráficos, MapReduce, Spark y el scraping:

```powershell
python run_project.py
```

---

### 4.2 Ejecución independiente por reto

Todos los comandos siguientes se ingresan en la terminal ubicada en `C:\Users\karim\TecnoE`.

---

#### Reto 1: Caracterización Big Data y Pipeline ETL con Python

* **Qué hace el código:**
  * Ingesta del catálogo de Netflix desde un archivo CSV.
  * Limpieza y eliminación de registros duplicados.
  * Imputación estratégica de valores nulos en director y país.
  * Parseo de fechas a tipo `datetime`.
  * Descompilación de la columna de duración en dos campos numéricos independientes (`duration_min` y `seasons_count`).
  * Cálculo de correlaciones estadísticas con SciPy y generación de un dashboard estático de 4 cuadrantes con Seaborn/Matplotlib.
* **Justificación técnica (5 V's):**
  * Documentada en `reto1_etl_python/5_Vs_Justificacion_Tecnica.md`.
* **Comandos de ejecución:**
  ```powershell
  python reto1_etl_python/etl_pipeline.py
  python reto1_etl_python/correlation_and_dashboard.py
  ```
* **Resultado esperado en consola:**
  * Mensajes de estado sobre la lectura del CSV original.
  * Conteo de nulos imputados.
  * Confirmación de la separación de la columna de duración en `duration_min` y `seasons_count`.
  * Confirmación de la creación del archivo limpio y del dashboard.
* **Archivos generados:**

| Archivo | Contenido |
| :--- | :--- |
| `data/netflix_cleaned.csv` | Imputación de nulos, parseo de fechas y separación de duración en `duration_min` y `seasons_count`. |
| `reto1_etl_python/output/dashboard_reto1.png` | Dashboard de 4 cuadrantes: distribución, Top 10 países, evolución anual y mapa de calor de correlación. |

---

#### Reto 2: Ingesta, Almacenamiento Distribuido y MapReduce en Hadoop

* **Qué hace el código:**
  * Configura la estructura de directorios en HDFS para almacenar el dataset procesado.
  * Ejecuta un trabajo MapReduce con `mrjob` que cuenta los títulos asociados a cada actor de la columna de reparto (`cast`).
  * Prepara la integración con una tabla NoSQL en HBase.
  * Define esquemas y consultas en Apache Hive.
* **Comandos HDFS** *(ejecutar en una terminal con el entorno Hadoop activo)*:
  ```bash
  hdfs dfs -mkdir -p /data/netflix/raw/
  hdfs dfs -put data/netflix_cleaned.csv /data/netflix/raw/
  hdfs dfs -ls /data/netflix/raw/
  ```
* **Comandos de ejecución local:**
  ```powershell
  python reto2_hadoop_mapreduce_hive/mr_actor_titles_count.py data/netflix_cleaned.csv
  python reto2_hadoop_mapreduce_hive/hbase_loader.py
  ```
* **Resultado esperado en consola:**
  * Listado de actores con la cantidad total de títulos en los que participan, por ejemplo: `("Anupam Kher", 43)`.
  * Registros confirmados de inserción en la simulación NoSQL de HBase.
* **Consultas Apache Hive:**
  * Definidas en `reto2_hadoop_mapreduce_hive/hive_schema_and_queries.hql`.
  * Incluyen `LATERAL VIEW explode` y funciones de ventana (`ROW_NUMBER()`) para el Top 5 de géneros por país.

---

#### Reto 3: Procesamiento Distribuido e In-Memory con Apache Spark

* **Qué hace el código:**
  * Inicializa una sesión de PySpark y carga el dataset optimizado, particionándolo por año de lanzamiento en formato columnar Parquet.
  * Ejecuta filtros temporales (últimos 3 años).
  * Aplica tokenización y remoción de palabras vacías (stop-words) sobre las descripciones.
  * Realiza agregaciones distribuidas para calcular la duración promedio por país.
* **Comando de ejecución:**
  ```powershell
  python reto3_spark_processing/spark_processor.py
  ```
* **Resultado esperado en consola:**
  * Métricas de ejecución de SparkSQL y comparativa CSV vs. Parquet.
  * Tablas con las duraciones promedio de películas agrupadas por país de origen.
  * Confirmación de la escritura exitosa en bloques de formato columnar.
* **Operaciones realizadas:**
  1. Conversión y particionamiento en Parquet (`release_year`).
  2. Comparativa de rendimiento CSV vs. Parquet (tiempo y espacio en disco).
  3. Filtrado de los últimos 3 años (`year_added >= 2019`).
  4. Tokenización y remoción de stop-words en `description`.
  5. Cálculo de duración promedio por país.
* **Salida generada:**
  * `reto3_spark_processing/output/netflix_parquet/`

---

#### Reto 4: Integración Externa, Scraping, Orquestación y Streaming

* **Qué hace el código:**
  * Realiza web scraping para extraer puntuaciones de IMDb de una muestra de 50 películas.
  * Define un DAG para Apache Airflow.
  * Simula un flujo de datos en tiempo real mediante un productor y un consumidor de Kafka.

* **Web scraping:**
  ```powershell
  python reto4_scraping_airflow_streaming/imdb_scraper.py
  ```
  * *Resultado esperado en consola:* Mensajes de progreso del request HTTP por cada una de las 50 películas analizadas con su puntuación de IMDb obtenida, antes de guardar `data/netflix_enriched.csv`.

* **Orquestación con Apache Airflow:**
  * El DAG secuencial (`Scraping` -> `Spark ETL` -> `Carga`) se encuentra en `reto4_scraping_airflow_streaming/airflow_dags/netflix_pipeline_dag.py`.
  * Debe copiarse a la carpeta `dags/` de una instancia activa de Airflow.

* **Simulación de streaming:**
  * Requiere dos ventanas de PowerShell abiertas en `C:\Users\karim\TecnoE`.
  * **Ventana 1 (productor Kafka):**
    ```powershell
    python reto4_scraping_airflow_streaming/kafka_producer.py
    ```
  * **Ventana 2 (consumidor Spark Streaming):**
    ```powershell
    python reto4_scraping_airflow_streaming/spark_streaming_consumer.py
    ```
  * *Resultado esperado en consola:* El productor despliega los payloads en formato JSON transmitidos al tópico. El consumidor imprime en tiempo real los micro-lotes procesados con las métricas actualizadas de los nuevos títulos.
