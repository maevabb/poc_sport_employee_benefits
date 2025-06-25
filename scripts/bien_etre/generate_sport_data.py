import os
import json
import random
import time
from datetime import datetime, timedelta
import argparse

import pandas as pd
import sqlalchemy
from dotenv import load_dotenv
from confluent_kafka import Producer

# Charger les variables d'environnement
load_dotenv()

# Logger simple
import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Connexion PostgreSQL
DATABASE_URL = "postgresql+psycopg2://postgres:postgres@localhost:5432/poc_avantages_sportifs"
engine = sqlalchemy.create_engine(DATABASE_URL)

# Connexion Redpanda (Kafka via confluent_kafka)
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'sport-activities'

producer = Producer({'bootstrap.servers': KAFKA_BROKER})

# Mapping simple pour normaliser les fautes
SPORT_MAPPING = {
    'Runing': 'Running',
    'Tennis': 'Tennis',
    'Randonnée': 'Randonnée',
    'Natation': 'Natation',
    'Football': 'Football',
    'Rugby': 'Rugby',
    'Badminton': 'Badminton',
    'Voile': 'Voile',
    'Boxe': 'Boxe',
    'Judo': 'Judo',
    'Escalade': 'Escalade',
    'Triathlon': 'Triathlon',
    'Équitation': 'Équitation',
    'Tennis de table': 'Tennis de table',
    'Basketball': 'Basketball'
}

DISTANCE_RELEVANT_SPORTS = [
    'Running', 'Randonnée', 'Natation', 'Triathlon', 'Équitation', 'Voile'
]

def generate_activity(employee_id, sport_type):
    now = datetime.now()
    start_date = now - timedelta(days=365)
    debut = start_date + timedelta(days=random.randint(0, 365))
    duree = random.randint(1800, 7200)  # entre 30min et 2h
    fin = debut + timedelta(seconds=duree)

    if sport_type in DISTANCE_RELEVANT_SPORTS:
        distance_m = random.randint(2000, 30000)
    else:
        distance_m = None

    commentaire = random.choice(["", "Bonne séance", "Fatiguant", "Top performance !"])

    return {
        'id_employe': int(employee_id),  # Conversion explicite ici
        'debut_activite': debut.isoformat(),
        'fin_activite': fin.isoformat(),
        'type_activite': sport_type,
        'distance': distance_m,
        'temps_sec': duree,
        'commentaire': commentaire
    }

def fetch_employees_with_sport():
    query = """
        SELECT id_employe, pratique_sportive
        FROM employes
        WHERE pratique_sportive IS NOT NULL
    """
    return pd.read_sql(query, con=engine)

def main(nb_messages=300):
    df = fetch_employees_with_sport()
    logging.info(f"{len(df)} employés avec pratique sportive trouvés.")

    for i in range(nb_messages):
        row = df.sample(n=1).iloc[0]
        sport_pratique = row['pratique_sportive']
        sport_type = SPORT_MAPPING.get(sport_pratique, sport_pratique)
        activity = generate_activity(row['id_employe'], sport_type)

        # Envoi dans Redpanda
        producer.produce(TOPIC_NAME, value=json.dumps(activity).encode('utf-8'))
        producer.flush() 

        logging.info(f"Message {i+1}/{nb_messages} envoyé : {activity}")

        time.sleep(3)

    logging.info("Simulation terminée.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulateur d'activités sportives")
    parser.add_argument("--nb_messages", type=int, default=300, help="Nombre de messages à générer")
    args = parser.parse_args()

    main(nb_messages=args.nb_messages)
