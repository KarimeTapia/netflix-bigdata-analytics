"""
================================================================================
RETO 2: MAPREDUCE CON PYTHON (MRJOB) - CONTEO DE TÍTULOS POR ACTOR
================================================================================
Este script implementa un algoritmo distribuido MapReduce para calcular
la cantidad total de títulos asociados a cada actor en la columna 'cast'.

Arquitectura MapReduce:
- Mapper: Lee cada línea del dataset CSV, desarticula la lista de actores
  separados por comas y emite pares clave-valor (actor, 1).
- Combiner: Realiza una pre-agregación local en memoria en cada worker node.
- Reducer: Suma todas las apariciones de cada actor emitiendo (actor, total_titulos).

Modos de ejecución:
1. Local (pruebas rápidas):
   python mr_actor_titles_count.py ../data/netflix_cleaned.csv > output_actors.tsv

2. En clúster Hadoop distribuido:
   python mr_actor_titles_count.py -r hadoop hdfs:///data/netflix/raw/netflix_cleaned.csv --output-dir hdfs:///data/netflix/output/actors_count
================================================================================
"""

# Compatibilidad para Python 3.13 / 3.14 (donde el módulo 'pipes' fue sustituido por 'shlex')
try:
    import pipes
except ImportError:
    import shlex as pipes
    import sys
    sys.modules["pipes"] = pipes

import sys
import csv
from mrjob.job import MRJob
from mrjob.step import MRStep


class MRActorTitlesCount(MRJob):

    def mapper_init(self):
        """Inicializa cualquier estado previo al mapeo si fuera necesario."""
        self.header_processed = False

    def mapper(self, _, line):
        """
        Fase Map:
        Lee la línea CSV, extrae la columna 'cast' (índice 4),
        desarticula los actores y emite (actor, 1).
        """
        # Parseo seguro con csv.reader para respetar comillas y comas internas
        try:
            reader = csv.reader([line])
            row = next(reader)
        except Exception:
            return

        # Omitir cabecera
        if not row or row[0] == "show_id" or len(row) < 5:
            return

        cast_field = row[4].strip()
        
        # Ignorar valores nulos o imputados genéricos
        if not cast_field or cast_field in ["Unknown Cast", "null", "NaN"]:
            return

        # Desarticular actores separados por comas
        actors = [actor.strip() for actor in cast_field.split(",") if actor.strip()]
        for actor in actors:
            # Emitir par (clave, valor): (nombre_actor, 1)
            yield (actor, 1)

    def combiner(self, actor, counts):
        """
        Fase Combiner (Optimización de red):
        Suma parcial de frecuencias en el nodo mapper local.
        """
        yield (actor, sum(counts))

    def reducer(self, actor, counts):
        """
        Fase Reduce:
        Agrega la suma global de títulos por actor.
        """
        yield (actor, sum(counts))


if __name__ == "__main__":
    MRActorTitlesCount.run()
