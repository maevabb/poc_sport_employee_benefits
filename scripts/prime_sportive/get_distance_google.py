import time
import pandas as pd
import requests
import sqlalchemy
import logging

# === Config ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
from scripts.config import (engine, GOOGLE_API_KEY)

# === Paramètres métier ===
from scripts.params import (WORK_ADDRESS)

# === Appel de l'API Google avec gestion des exceptions ===
def get_distance(origin_address):
    """
    Utilise l'API Google Distance Matrix pour calculer la distance en kilomètres
    entre une adresse d'origine et l'adresse de l'entreprise.
    """
    url = "https://maps.googleapis.com/maps/api/distancematrix/json"
    params = {
        "origins": origin_address,
        "destinations": WORK_ADDRESS,
        "units": "metric",
        "key": GOOGLE_API_KEY
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        if data['status'] != 'OK':
            return None, "Erreur API Google"

        element = data['rows'][0]['elements'][0]
        if element['status'] != 'OK':
            return None, element['status']

        distance_km = element['distance']['value'] / 1000
        return distance_km, "Valide"

    except requests.exceptions.RequestException as e:
        logging.warning(f"Erreur de connexion API pour l'adresse {origin_address}: {e}")
        return None, "Erreur connexion API"

# === Lecture des adresses à traiter ===
def fetch_addresses():
    """
    Récupère les adresses des employés dont la distance n'a pas encore été calculée
    ou dont l'adresse a changé.
    """
    query = """
        SELECT e.id_employe, e.adresse_domicile, c.adresse_domicile AS adresse_calcul
        FROM employes e
        LEFT JOIN commuting_distance c ON e.id_employe = c.id_employe
        WHERE c.id_employe IS NULL OR e.adresse_domicile <> c.adresse_domicile;
    """
    return pd.read_sql(query, con=engine)

# === Insertion / mise à jour des résultats en base ===
def upsert_distance(id_employe, adresse_domicile, distance_km, statut):
    """
    Insère ou met à jour la distance entre le domicile de l'employé et l'entreprise
    dans la table `commuting_distance`.
    """
    with engine.begin() as conn:
        conn.execute(
            sqlalchemy.text("""
                INSERT INTO commuting_distance (id_employe, adresse_domicile, distance_km, statut_distance, date_calcul)
                VALUES (:id, :adresse, :dist, :statut, CURRENT_DATE)
                ON CONFLICT (id_employe) DO UPDATE SET
                  adresse_domicile = EXCLUDED.adresse_domicile,
                  distance_km = EXCLUDED.distance_km,
                  statut_distance = EXCLUDED.statut_distance,
                  date_calcul = CURRENT_DATE
            """),
            {"id": id_employe, "adresse": adresse_domicile, "dist": distance_km, "statut": statut}
        )

# === Pipeline principal ===
def main():
    """
    Pipeline principal :
    - Récupère les adresses à traiter.
    - Calcule les distances via l'API Google.
    - Met à jour la base avec les résultats.
    - Ajoute un délai pour respecter les limites de l'API.
    """
    df = fetch_addresses()
    logging.info(f"{len(df)} adresses à traiter")

    for _, row in df.iterrows():
        distance_km, statut = get_distance(row['adresse_domicile'])
        upsert_distance(row['id_employe'], row['adresse_domicile'], distance_km, statut)
        logging.info(f"Employé {row['id_employe']} - Distance : {distance_km} km - Statut : {statut}")
        time.sleep(0.5)  # Pause de 500ms pour respecter l'API rate limit

if __name__ == "__main__":
    main()
