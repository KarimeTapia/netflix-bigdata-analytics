#!/usr/bin/env python3
"""
================================================================================
RETO 2: REDUCER ESTÁNDAR PARA HADOOP STREAMING
================================================================================
Lee pares tabulados <actor>\t1 desde STDIN (previamente ordenados por clave
por el framework de Hadoop MapReduce / Shuffle & Sort) y emite el conteo
total acumulado de títulos por actor: <actor>\t<total_titulos>.
================================================================================
"""

import sys

def main():
    current_actor = None
    current_count = 0

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        parts = line.split("\t")
        if len(parts) != 2:
            continue

        actor, count_str = parts[0], parts[1]
        try:
            count = int(count_str)
        except ValueError:
            continue

        if current_actor == actor:
            current_count += count
        else:
            if current_actor is not None:
                print(f"{current_actor}\t{current_count}")
            current_actor = actor
            current_count = count

    # Emitir el último actor
    if current_actor is not None:
        print(f"{current_actor}\t{current_count}")

if __name__ == "__main__":
    main()
