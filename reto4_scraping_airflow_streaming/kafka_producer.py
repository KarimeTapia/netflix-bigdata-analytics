"""
RETO 4: PRODUCTOR DE APACHE KAFKA
Transmite nuevos registros de peliculas en formato JSON al topico 'netflix-new-titles'.
"""
import os
import sys
import time
import json
import pandas as pd

def run_producer(limit=20):
    print("=" * 60)
    print("INICIANDO PRODUCTOR KAFKA: TÓPICO 'netflix-new-titles'")
    print("=" * 60)
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    csv_path = os.path.join(base_dir, "data", "netflix_cleaned.csv")
    df = pd.read_csv(csv_path)

    producer = None
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        print("[Kafka] Conectado a Kafka Broker en localhost:9092")
    except Exception as e:
        print(f"[Kafka] Broker externo no activo ({e}). Ejecutando transmision simulada:")

    for idx, row in df.head(limit).iterrows():
        payload = {
            "show_id": row["show_id"],
            "type": row["type"],
            "title": row["title"],
            "country": row["country"],
            "release_year": int(row["release_year"]),
            "timestamp": time.time()
        }
        if producer:
            producer.send("netflix-new-titles", value=payload)
        print(f"-> [netflix-new-titles] ID: {payload['show_id']} | {payload['type']} | '{payload['title']}' ({payload['release_year']})")
        time.sleep(0.05)

    if producer:
        producer.flush()
    print("Productor Kafka finalizo el envio.")

if __name__ == "__main__":
    run_producer()
