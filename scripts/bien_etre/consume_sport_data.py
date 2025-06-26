import os
import json
import sqlalchemy
import logging
import requests
from datetime import datetime
from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError

load_dotenv()

# === Logger ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# === Connexion PostgreSQL ===
DATABASE_URL = "postgresql+psycopg2://postgres:postgres@localhost:5432/poc_avantages_sportifs"
engine = sqlalchemy.create_engine(DATABASE_URL)

# === Slack ===
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID")

def send_to_slack(message):
    headers = {
        "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        "Content-type": "application/json"
    }
    payload = {
        "channel": SLACK_CHANNEL_ID,
        "text": message
    }
    response = requests.post("https://slack.com/api/chat.postMessage", headers=headers, json=payload)
    if not response.ok or not response.json().get("ok"):
        logging.error(f"Erreur Slack : {response.text}")
    else:
        logging.info(f"Message Slack envoyé : {message}")

def get_employee_name(id_employe):
    with engine.begin() as conn:
        result = conn.execute(
            sqlalchemy.text("SELECT prenom, nom FROM employes WHERE id_employe = :id"),
            {"id": id_employe}
        ).fetchone()
        return result if result else (None, None)

def build_slack_message(activity, prenom, nom):
    sport = activity['type_activite']
    distance_km = round(activity["distance"] / 1000, 2) if activity["distance"] else None
    temps_min = activity['temps_sec'] // 60
    temps_hr = round(activity["temps_sec"] / 3600, 2)
    commentaire = activity.get("commentaire", "").strip()

    if sport.lower() == "randonnée" and distance_km:
        base_message = f":hiking_boot: Magnifique {prenom} {nom} ! Une randonnée de {distance_km} km terminée en {temps_hr} heures ! :sunrise_over_mountains:"
    elif sport.lower() == "running" and distance_km:
        base_message = f":person_running: Bravo {prenom} {nom} ! Tu viens de courir {distance_km} km en {temps_min} min ! Quelle énergie ! :fire:"
    elif distance_km:
        base_message = f":party_popper: Super {prenom} {nom} ! Une belle séance de {sport} de {distance_km} km en {temps_min} minutes vient d'être enregistrée !"
    else:
        base_message = f":party_popper: Super {prenom} {nom} ! Une séance de {sport} de {temps_min} minutes vient d'être enregistrée !"
    
   
    if sport.lower() == "natation":
        base_message += " :person_swimming::water_wave:"
    elif sport.lower() == "tennis":
        base_message += " :tennis:"
    elif sport.lower() == "football":
        base_message += " :soccer_ball:"
    elif sport.lower() == "rugby":
        base_message += " :rugby_football:"
    elif sport.lower() == "boxe":
        base_message += " :boxing_glove:"
    elif sport.lower() == "équitation":
        base_message += " :horse_racing:"
    else:
        base_message += " :sports_medal:"

    if commentaire:
        base_message += f"\n\"({commentaire})\""

    return base_message

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

# === Fonction de validation des messages ===
def validate_activity(activity):
    # Vérification ID employé
    if not isinstance(activity['id_employe'], int) or activity['id_employe'] <= 0:
        raise ValueError("ID employé invalide")

    # Vérification des dates
    try:
        debut = datetime.fromisoformat(activity['debut_activite'])
        fin = datetime.fromisoformat(activity['fin_activite'])
        if fin < debut:
            raise ValueError("Fin d'activité antérieure au début")
    except Exception as e:
        raise ValueError(f"Erreur de parsing des dates : {e}")

    # Vérification distance (si renseignée)
    if activity['distance'] is not None and activity['distance'] < 0:
        raise ValueError("Distance négative")

    # Vérification temps
    if activity['temps_sec'] < 0:
        raise ValueError("Durée négative")

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

# === Main loop ===
def main():
    logging.info("🟢 Consumer démarré...")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error() and msg.error().code() != KafkaError._PARTITION_EOF:
                logging.error(f"Erreur Kafka : {msg.error()}")
                continue
            try:
                activity = json.loads(msg.value().decode('utf-8'))
                validate_activity(activity)
                insert_activity(activity)
                prenom, nom = get_employee_name(activity['id_employe'])
                if prenom:
                    slack_msg = build_slack_message(activity, prenom, nom)
                    send_to_slack(slack_msg)
                else:
                    logging.warning(f"Employé {activity['id_employe']} introuvable pour Slack.")
            except Exception as e:
                logging.error(f"Message rejeté : {e}")
    except KeyboardInterrupt:
        logging.info("Arrêt du consumer.")
    finally:
        consumer.close()

if __name__ == "__main__":
    if not SLACK_BOT_TOKEN or not SLACK_CHANNEL_ID:
        logging.error("Variables SLACK_BOT_TOKEN ou SLACK_CHANNEL_ID manquantes.")
    else:
        main()
