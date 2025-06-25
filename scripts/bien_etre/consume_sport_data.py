import os
import json
import sqlalchemy
import logging
from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError

load_dotenv()

# === Logger ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# === Connexion PostgreSQL ===
DATABASE_URL = "postgresql+psycopg2://postgres:postgres@localhost:5432/poc_avantages_sportifs"
engine = sqlalchemy.create_engine(DATABASE_URL)

# === Connexion Redpanda (Kafka) ===
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'sport-activities'
GROUP_ID = 'activity-consumer-group'

consumer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'earliest' 
}

consumer = Consumer(consumer_conf)
consumer.subscribe([TOPIC_NAME])

# === Insertion dans PostgreSQL ===
def insert_activity(activity):
    with engine.begin() as conn:
        conn.execute(
            sqlalchemy.text("""
                INSERT INTO activities_sportives (
                    id_employe, debut_activite, fin_activite, type_activite, distance, temps_sec, commentaire
                ) VALUES (
                    :id_employe, :debut, :fin, :type, :distance, :temps, :commentaire
                )
            """),
            {
                'id_employe': activity['id_employe'],
                'debut': activity['debut_activite'],
                'fin': activity['fin_activite'],
                'type': activity['type_activite'],
                'distance': activity['distance'],
                'temps': activity['temps_sec'],
                'commentaire': activity['commentaire']
            }
        )

# === Pipeline principal de consommation ===
def main():
    logging.info("Démarrage du consumer streaming...")
    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logging.error(f"Erreur Kafka : {msg.error()}")
                continue

            try:
                activity = json.loads(msg.value().decode('utf-8'))
                insert_activity(activity)
                logging.info(f"Activité insérée : employé {activity['id_employe']} - {activity['type_activite']}")
            except Exception as e:
                logging.error(f"Erreur traitement message : {e}")

    except KeyboardInterrupt:
        logging.info("Arrêt manuel du consumer.")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
