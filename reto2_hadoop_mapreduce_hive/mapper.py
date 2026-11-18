#!/usr/bin/env python3
"""
================================================================================
RETO 2: MAPPER ESTÁNDAR PARA HADOOP STREAMING
================================================================================
Lee líneas desde STDIN (archivo CSV de Netflix), parsea las filas respetando
comillas, extrae los actores de la columna 'cast' (índice 4) y emite
pares tabulados: <actor>\t1 a STDOUT.
================================================================================
"""

import sys
import csv

def main():
    reader = csv.reader(sys.stdin)
    for row in reader:
        # Omitir cabecera o filas truncadas
        if not row or row[0] == "show_id" or len(row) < 5:
            continue
        
        cast_field = row[4].strip()
        if not cast_field or cast_field in ["Unknown Cast", "null", "NaN"]:
            continue
            
        actors = [a.strip() for a in cast_field.split(",") if a.strip()]
        for actor in actors:
            # Emitir a stdout: clave<tabulador>valor
            print(f"{actor}\t1")

if __name__ == "__main__":
    main()
