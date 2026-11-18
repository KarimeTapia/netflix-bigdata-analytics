"""
================================================================================
RETO 2: CARGADOR DE RESULTADOS MAPREDUCE A TABLA NOSQL DE APACHE HBASE
================================================================================
Este script toma la salida refinada del trabajo MapReduce (conteo de títulos por
actor) y la persiste en una tabla NoSQL distribuida de Apache HBase.

Modelo de Datos HBase:
- Tabla: 'netflix_actors'
- Familia de Columnas: 'cf_stats'
  - cf_stats:actor_name (String)
  - cf_stats:titles_count (Integer / String)
  - cf_stats:top_tier (Boolean / String: 'TRUE' si >= 10 títulos)
  - cf_stats:updated_at (Timestamp ISO-8601)
- RowKey: Nombre del actor sanitizado (formato UTF-8).

Comandos equivalentes en la Shell de HBase:
  hbase shell
  > create 'netflix_actors', 'cf_stats'
  > put 'netflix_actors', 'Anupam Kher', 'cf_stats:titles_count', '42'
  > scan 'netflix_actors', {LIMIT => 10}
================================================================================
"""

import os
import sys
import glob
import json
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("HBaseLoader")


class HBaseClient:
    """
    Cliente para interactuar con Apache HBase.
    Soporta conexión real vía HappyBase (Thrift) y modo emulado local
    para pruebas en entornos sin clúster activo.
    """
    def __init__(self, host: str = "localhost", port: int = 9090):
        self.host = host
        self.port = port
        self.connection = None
        self.emulated_table = {}
        self.table_name = "netflix_actors"
        self.column_family = "cf_stats"

        # Intentar importar happybase si está disponible en el entorno
        try:
            import happybase
            self.connection = happybase.Connection(host=self.host, port=self.port, autoconnect=False)
            self.connection.open()
            logger.info(f"Conectado exitosamente al servidor HBase Thrift en {self.host}:{self.port}")
        except Exception as e:
            logger.warning(
                f"No se pudo conectar a HBase Thrift ({e}). "
                "Activando motor NoSQL HBase Emulado para ejecución y validación de pruebas."
            )
            self.connection = None

    def create_table_if_not_exists(self):
        """Crea la tabla NoSQL y define la familia de columnas."""
        if self.connection:
            tables = [t.decode("utf-8") for t in self.connection.tables()]
            if self.table_name not in tables:
                logger.info(f"Creando tabla '{self.table_name}' con familia de columnas '{self.column_family}' en HBase...")
                self.connection.create_table(
                    self.table_name,
                    {self.column_family: dict(max_versions=3, compression="SNAPPY")}
                )
            else:
                logger.info(f"La tabla '{self.table_name}' ya existe en HBase.")
        else:
            logger.info(f"[HBase-Mock] Creando tabla '{self.table_name}' con familia '{self.column_family}:*'")
            self.emulated_table.clear()

    def batch_insert_actor_counts(self, actor_counts: dict):
        """Inserta registros de forma masiva (Batch Mutation) en HBase."""
        logger.info(f"Iniciando inserción de {len(actor_counts)} registros en HBase...")
        timestamp_str = datetime.utcnow().isoformat()
        inserted = 0

        if self.connection:
            table = self.connection.table(self.table_name)
            with table.batch(batch_size=1000) as batch:
                for actor, count in actor_counts.items():
                    row_key = actor.encode("utf-8")
                    data = {
                        f"{self.column_family}:actor_name".encode(): actor.encode("utf-8"),
                        f"{self.column_family}:titles_count".encode(): str(count).encode("utf-8"),
                        f"{self.column_family}:top_tier".encode(): ("TRUE" if count >= 10 else "FALSE").encode("utf-8"),
                        f"{self.column_family}:updated_at".encode(): timestamp_str.encode("utf-8")
                    }
                    batch.put(row_key, data)
                    inserted += 1
        else:
            for actor, count in actor_counts.items():
                self.emulated_table[actor] = {
                    f"{self.column_family}:actor_name": actor,
                    f"{self.column_family}:titles_count": count,
                    f"{self.column_family}:top_tier": True if count >= 10 else False,
                    f"{self.column_family}:updated_at": timestamp_str
                }
                inserted += 1

        logger.info(f"Inserción completada en HBase. Registros persistidos: {inserted}")

    def scan_top_actors(self, limit: int = 10):
        """Ejecuta una consulta SCAN en HBase para mostrar los actores más destacados."""
        logger.info(f"Ejecutando SCAN sobre '{self.table_name}' para obtener el Top {limit} actores...")
        
        if self.connection:
            table = self.connection.table(self.table_name)
            # En HBase el scan nativo recorre por row_key lexicográfico
            records = []
            for key, data in table.scan(limit=1000):
                records.append({
                    "actor": key.decode("utf-8"),
                    "count": int(data[f"{self.column_family}:titles_count".encode()].decode("utf-8"))
                })
        else:
            records = [
                {"actor": k, "count": v[f"{self.column_family}:titles_count"]}
                for k, v in self.emulated_table.items()
            ]

        # Ordenar por cantidad de títulos descendente
        sorted_records = sorted(records, key=lambda x: x["count"], reverse=True)[:limit]
        print("\n" + "="*50)
        print(f"HBASE SCAN RESULT - TOP {limit} ACTORES POR TÍTULOS")
        print("="*50)
        for rank, item in enumerate(sorted_records, 1):
            print(f"[{rank:2d}] {item['actor']:<35} : {item['count']} títulos")
        print("="*50 + "\n")
        return sorted_records


def parse_mrjob_output_files(mr_dir: str) -> dict:
    """Lee y unifica los archivos de salida generados por MapReduce (part-*)."""
    actor_counts = {}
    part_files = glob.glob(os.path.join(mr_dir, "part-*"))
    
    if not part_files:
        raise FileNotFoundError(f"No se encontraron archivos part-* en {mr_dir}")

    for file_path in part_files:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = line.split("\t")
                if len(parts) == 2:
                    actor = parts[0].strip().strip('"')
                    try:
                        count = int(parts[1].strip())
                        actor_counts[actor] = count
                    except ValueError:
                        continue
    return actor_counts


def run_hbase_pipeline(mr_output_dir: str):
    """Punto de entrada para ejecutar la carga en HBase."""
    logger.info("=== INICIANDO CARGA DE RESULTADOS MAPREDUCE EN HBASE ===")
    actor_counts = parse_mrjob_output_files(mr_output_dir)
    logger.info(f"Total de actores analizados por MapReduce: {len(actor_counts):,}")
    
    hbase = HBaseClient()
    hbase.create_table_if_not_exists()
    hbase.batch_insert_actor_counts(actor_counts)
    hbase.scan_top_actors(limit=15)
    logger.info("=== CARGA EN HBASE FINALIZADA EXITOSAMENTE ===")


if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    DEFAULT_MR_OUTPUT = os.path.join(BASE_DIR, "reto2_hadoop_mapreduce_hive", "output_mr")
    
    mr_dir = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_MR_OUTPUT
    run_hbase_pipeline(mr_dir)
