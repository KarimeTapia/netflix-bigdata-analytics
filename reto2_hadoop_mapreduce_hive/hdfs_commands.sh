#!/bin/bash
# ==============================================================================
# RETO 2: COMANDOS CLI DE HADOOP HDFS
# ==============================================================================
# Este script documenta y ejecuta la administración distribuida del sistema
# de archivos HDFS para el catálogo de Netflix.
#
# Pasos incluidos:
# 1. Creación de la estructura de directorios (/data/netflix/raw/)
# 2. Ingesta del archivo limpio mediante 'hdfs dfs -put'
# 3. Listado e inspección de metadatos con 'hdfs dfs -ls'
# 4. Demostración de réplica de bloques y salud con 'hdfs fsck'
# 5. Consulta de factor de replicación con 'hdfs dfs -stat'
# ==============================================================================

set -e

HDFS_RAW_DIR="/data/netflix/raw"
LOCAL_CSV_FILE="../data/netflix_cleaned.csv"

echo "=== 1. CREANDO ESTRUCTURA DE DIRECTORIOS EN HDFS ==="
hdfs dfs -mkdir -p ${HDFS_RAW_DIR}
echo "Directorio creado: ${HDFS_RAW_DIR}"

echo ""
echo "=== 2. CARGANDO DATASET PROCESADO A HDFS (-put) ==="
if [ -f "${LOCAL_CSV_FILE}" ]; then
    hdfs dfs -put -f ${LOCAL_CSV_FILE} ${HDFS_RAW_DIR}/netflix_cleaned.csv
    echo "Archivo cargado con éxito en ${HDFS_RAW_DIR}/netflix_cleaned.csv"
else
    echo "Aviso: Archivo local no encontrado en ${LOCAL_CSV_FILE}. Verifique la ruta."
fi

echo ""
echo "=== 3. INSPECCIÓN Y LISTADO DE ARCHIVOS EN HDFS (-ls) ==="
hdfs dfs -ls -h ${HDFS_RAW_DIR}

echo ""
echo "=== 4. DEMOSTRACIÓN DE RÉPLICA DE BLOQUES (hdfs dfs -stat) ==="
# Formato: %r = factor de réplica, %o = tamaño de bloque, %b = tamaño en bytes
echo "Formato: [Factor Réplica] [Tamaño de Bloque (bytes)] [Tamaño Total (bytes)]"
hdfs dfs -stat "%r replica(s) | Bloque: %o bytes | Total: %b bytes" ${HDFS_RAW_DIR}/netflix_cleaned.csv

echo ""
echo "=== 5. INSPECCIÓN DETALLADA DE BLOQUES Y UBICACIÓN FÍSICA (hdfs fsck) ==="
hdfs fsck ${HDFS_RAW_DIR}/netflix_cleaned.csv -files -blocks -locations

echo ""
echo "=== COMANDOS HDFS FINALIZADOS CORRECTAMENTE ==="
