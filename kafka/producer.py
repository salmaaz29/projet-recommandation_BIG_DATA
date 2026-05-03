import json
import time
import pandas as pd
from kafka import KafkaProducer

# ── Configuration ──────────────────────────────────────────────
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME   = 'reviews_stream'
CSV_PATH     = '../data/Reviews.csv'
DELAY        = 0.1   # secondes entre chaque message (simule le temps réel)
# ───────────────────────────────────────────────────────────────

def create_producer():
    """Crée et retourne un KafkaProducer connecté au broker."""
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        # Si Kafka n'est pas dispo, on réessaie jusqu'à 5 fois
        retries=5
    )
    print(f"✅ Connecté à Kafka sur {KAFKA_BROKER}")
    return producer


def load_dataset(path):
    """Charge uniquement les colonnes utiles du CSV."""
    print(f"📂 Chargement du dataset depuis {path}...")
    df = pd.read_csv(
        path,
        usecols=['UserId', 'ProductId', 'Score', 'Time'],
        dtype={
            'UserId':    str,
            'ProductId': str,
            'Score':     float,
            'Time':      int
        }
    )
    # Supprimer les lignes incomplètes  pour éviter d'envoyer des données corrompues.
    df = df.dropna()
    print(f"✅ Dataset chargé : {len(df)} avis disponibles")
    return df


def stream_reviews(producer, df):
    """Envoie chaque avis dans le topic Kafka avec un délai."""
    print(f"🚀 Démarrage du stream vers le topic '{TOPIC_NAME}'...")
    print(f"   (délai entre messages : {DELAY}s)\n")

    for index, row in df.iterrows():
        message = {
            'UserId':    row['UserId'],
            'ProductId': row['ProductId'],
            'Score':     row['Score'],
            'Time':      row['Time']
        }

        # Envoi du message dans le topic
        producer.send(TOPIC_NAME, value=message)

        # Affichage de confirmation toutes les 100 lignes
        if index % 100 == 0:
            print(f"   ✉️  Message #{index} envoyé → {message}")

        time.sleep(DELAY)

    producer.flush()  # S'assure que tous les messages sont bien envoyés
    print("\n✅ Tous les messages ont été envoyés.")


if __name__ == '__main__':
    producer = create_producer()
    df       = load_dataset(CSV_PATH)
    stream_reviews(producer, df)