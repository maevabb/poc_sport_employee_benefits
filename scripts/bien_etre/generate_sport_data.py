import json
import random
import time
from datetime import datetime, timedelta
import argparse
import logging
import pandas as pd

# === Config ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
from scripts.config import (engine)
from scripts.config_kafka import (producer, TOPIC_NAME)

# === Paramètres métier ===
from scripts.params import (NB_MESSAGES_DEFAULT, TIME_SLEEP_DEFAULT, SPORT_MAPPING, DISTANCE_RELEVANT_SPORTS)

# === Fonctions ===
def generate_activity(employee_id, sport_type):
    """
    Génère aléatoirement une activité sportive simulée pour un employé.
    """
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
    """
    Récupère les employés ayant une pratique sportive renseignée dans la base de données.
    """
    query = """
        SELECT id_employe, pratique_sportive
        FROM employes
        WHERE pratique_sportive IS NOT NULL
    """
    return pd.read_sql(query, con=engine)

def main(nb_messages, time_sleep):
    """
    Fonction principale qui simule des activités sportives :
    - Sélectionne aléatoirement un employé avec une pratique sportive.
    - Génère une activité réaliste.
    - Envoie le message vers Redpanda (Kafka) avec un délai paramétré.
    """
    df = fetch_employees_with_sport()
    logging.info(f"{len(df)} employés avec pratique sportive trouvés.")

    for i in range(nb_messages):
        row = df.sample(n=1).iloc[0]
        sport_pratique = row['pratique_sportive']
        sport_type = SPORT_MAPPING.get(sport_pratique, sport_pratique)
        activity = generate_activity(row['id_employe'], sport_type)

        producer.produce(TOPIC_NAME, value=json.dumps(activity).encode('utf-8'))
        producer.flush() 

        logging.info(f"Message {i+1}/{nb_messages} envoyé : {activity}")

        time.sleep(time_sleep)

    logging.info("Simulation terminée.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulateur d'activités sportives")
    parser.add_argument("--nb_messages", type=int, default=NB_MESSAGES_DEFAULT, help="Nombre de messages à générer")
    parser.add_argument("--time_sleep", type=int, default=TIME_SLEEP_DEFAULT, help="Temps de pause entre chaque message (en secondes)")
    args = parser.parse_args()

    main(nb_messages=args.nb_messages, time_sleep=args.time_sleep)
