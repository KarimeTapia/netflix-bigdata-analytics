"""
================================================================================
RETO 1: PIPELINE ETL CON PYTHON (PANDAS Y NUMPY)
================================================================================
Módulo de ingesta, saneamiento, imputación estratégica y transformación
del catálogo global de películas y series de Netflix.

Requisitos cumplidos:
1. Ingesta del CSV original de Kaggle.
2. Detección y eliminación de duplicados.
3. Corrección de desalineaciones en columnas (ej. 'rating' desplazado a 'duration').
4. Imputación estratégica de nulos en 'director' y 'country'.
5. Parsing de 'date_added' a objeto datetime.
6. Descompilación de 'duration' en dos columnas numéricas: 'duration_min' y 'seasons_count'.
7. Exportación del dataset procesado a formato CSV optimizado.
================================================================================
"""

import os
import sys
import logging
import pandas as pd
import numpy as np

# Configuración de logging profesional
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("NetflixETL")


def load_dataset(filepath: str) -> pd.DataFrame:
    """Realiza la ingesta del archivo CSV y valida su existencia."""
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"El archivo de datos no existe en: {filepath}")
    
    logger.info(f"Iniciando ingesta del dataset desde: {filepath}")
    df = pd.read_csv(filepath)
    logger.info(f"Dataset cargado exitosamente. Filas: {df.shape[0]}, Columnas: {df.shape[1]}")
    return df


def clean_duplicates_and_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    """
    Elimina duplicados basados en show_id y corrige registros desalineados
    conocidos donde la duración se filtró en la columna 'rating'.
    """
    initial_rows = len(df)
    
    # 1. Eliminación de duplicados
    df = df.drop_duplicates(subset=["show_id"]).copy()
    duplicates_removed = initial_rows - len(df)
    logger.info(f"Duplicados eliminados: {duplicates_removed}")

    # 2. Corrección de desalineación conocida: 'rating' contiene valores de duración ('min')
    misplaced_mask = df["rating"].str.contains("min", na=False)
    misplaced_count = misplaced_mask.sum()
    if misplaced_count > 0:
        logger.info(f"Detectadas {misplaced_count} filas con duración en la columna 'rating'. Corrigiendo...")
        # Trasladar el valor de duración a 'duration' y asignar 'Unknown' al rating
        df.loc[misplaced_mask & df["duration"].isna(), "duration"] = df.loc[misplaced_mask, "rating"]
        df.loc[misplaced_mask, "rating"] = "Unknown"

    return df


def strategic_imputation(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica imputación estratégica en 'director' y 'country':
    - Para 'country': intenta imputar a partir del país más frecuente asociado
      al director (si existe); en caso contrario, asigna 'Unknown Country'.
    - Para 'director': imputa con 'Unknown Director'.
    """
    logger.info("Aplicando imputación estratégica de valores nulos...")
    
    # Mapeo contextual de Director -> País más frecuente
    director_country_map = (
        df.dropna(subset=["director", "country"])
        .groupby("director")["country"]
        .agg(lambda x: x.mode()[0] if not x.mode().empty else "Unknown Country")
        .to_dict()
    )

    # Imputación guiada por director para 'country'
    missing_country_mask = df["country"].isna()
    inferred_countries = df.loc[missing_country_mask, "director"].map(director_country_map)
    df.loc[missing_country_mask, "country"] = inferred_countries.fillna("Unknown Country")

    # Imputación para 'director'
    df["director"] = df["director"].fillna("Unknown Director")

    # Imputación de soporte para 'cast' y 'rating'
    df["cast"] = df["cast"].fillna("Unknown Cast")
    df["rating"] = df["rating"].fillna("Unknown")

    logger.info("Imputación completada: Nulos en 'director' y 'country' resueltos a 0.")
    return df


def parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convierte la columna 'date_added' en un objeto datetime estandarizado.
    Maneja espacios en blanco y formatos textuales heterogéneos.
    """
    logger.info("Parseando columna 'date_added' a objetos datetime...")
    df["date_added_cleaned"] = df["date_added"].astype(str).str.strip()
    df["date_added"] = pd.to_datetime(df["date_added_cleaned"], format="mixed", errors="coerce")
    
    # Extraer columnas temporales auxiliares útiles para análisis y particionamiento
    df["year_added"] = df["date_added"].dt.year.fillna(df["release_year"]).astype(int)
    df["month_added"] = df["date_added"].dt.month.fillna(1).astype(int)
    
    df.drop(columns=["date_added_cleaned"], inplace=True)
    logger.info(f"Parsing temporal completado. Rango temporal: {df['date_added'].min()} a {df['date_added'].max()}")
    return df


def decompile_duration(df: pd.DataFrame) -> pd.DataFrame:
    """
    Descompila la columna 'duration' en dos columnas numéricas separadas:
    - 'duration_min' (entero): minutos de duración para Películas (0 para Series).
    - 'seasons_count' (entero): número de temporadas para Series (0 para Películas).
    """
    logger.info("Descompilando columna 'duration' en 'duration_min' y 'seasons_count'...")
    
    # Extracción numérica mediante expresiones regulares
    df["duration"] = df["duration"].fillna("0 min")
    
    # Extraer minutos para Movie
    movie_mask = df["type"] == "Movie"
    extracted_min = df["duration"].str.extract(r"(\d+)\s*min", expand=False)
    df["duration_min"] = np.where(movie_mask, pd.to_numeric(extracted_min, errors="coerce").fillna(0), 0).astype(int)
    
    # Extraer temporadas para TV Show
    tv_mask = df["type"] == "TV Show"
    extracted_seasons = df["duration"].str.extract(r"(\d+)\s*Season", expand=False)
    df["seasons_count"] = np.where(tv_mask, pd.to_numeric(extracted_seasons, errors="coerce").fillna(0), 0).astype(int)
    
    logger.info(
        f"Descompilación exitosa -> Películas con duración > 0: {(df['duration_min'] > 0).sum()}, "
        f"Series con temporadas > 0: {(df['seasons_count'] > 0).sum()}"
    )
    return df


def run_pipeline(input_csv: str, output_csv: str) -> pd.DataFrame:
    """Ejecuta el ciclo de vida completo del pipeline ETL."""
    logger.info("=== INICIANDO PIPELINE ETL DE NETFLIX ===")
    df = load_dataset(input_csv)
    df = clean_duplicates_and_anomalies(df)
    df = strategic_imputation(df)
    df = parse_dates(df)
    df = decompile_duration(df)
    
    # Guardar archivo limpio
    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    df.to_csv(output_csv, index=False, encoding="utf-8")
    logger.info(f"Dataset procesado guardado exitosamente en: {output_csv}")
    logger.info("=== PIPELINE ETL FINALIZADO CON ÉXITO ===")
    return df


if __name__ == "__main__":
    # Rutas por defecto del proyecto
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    DEFAULT_INPUT = os.path.join(BASE_DIR, "data", "netflix_titles.csv")
    DEFAULT_OUTPUT = os.path.join(BASE_DIR, "data", "netflix_cleaned.csv")
    
    input_file = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_INPUT
    output_file = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_OUTPUT
    
    run_pipeline(input_file, output_file)
