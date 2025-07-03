import os
import logging
import boto3
import pandas as pd
import sqlalchemy
from io import BytesIO
from dotenv import load_dotenv

load_dotenv()

# === Logger ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# === Config ===
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = "p12-sport-data-solution"
PREFIX_RH = "donnees-rh/"
PREFIX_SPORT = "donnees-sportives/"

DATABASE_URL = "postgresql+psycopg2://postgres:postgres@localhost:5432/poc_avantages_sportifs"
engine = sqlalchemy.create_engine(DATABASE_URL)

# === S3 Client ===
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)

# === Utilitaires pour téléchargement ===
def fetch_latest_excel_from_prefix(prefix):
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
    files = sorted(
        [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".xlsx")],
        reverse=True
    )
    if not files:
        raise FileNotFoundError(f"Aucun fichier trouvé pour le préfixe {prefix}")
    latest_file_key = files[0]
    logging.info(f"📄 Fichier sélectionné : {latest_file_key}")
    obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=latest_file_key)
    return pd.read_excel(BytesIO(obj["Body"].read()))

# === Nettoyage RH ===
def clean_rh_data(df):
    logging.info("Nettoyage des données rh...")
    original_len = len(df)

    rename_mapping = {
        'ID salarié': 'id_employe',
        'Nom': 'nom',
        'Prénom': 'prenom',
        'Date de naissance': 'date_naissance',
        'BU': "bu",
        "Date d'embauche": 'date_embauche',
        'Salaire brut': 'salaire_brut',
        'Type de contrat': 'type_contrat',
        'Nombre de jours de CP': 'nb_jours_cp',
        'Adresse du domicile': 'adresse_domicile',
        'Moyen de déplacement': "mode_deplacement"
    }
    df = df.rename(columns=rename_mapping)
    df = df.drop_duplicates(subset="id_employe")
    after_dedup = len(df)
    df = df.dropna(subset=["id_employe", "nom", "prenom", "salaire_brut"])
    final_len = len(df)

    logging.info(f"RH : {original_len} lignes initiales ➜ {after_dedup} après dédupli ➜ {final_len} après nettoyage.")
    return df

# === Nettoyage sport ===
def clean_sport_data(df):
    logging.info("Nettoyage des données sportives...")
    original_len = len(df)

    rename_mapping = {
        'ID salarié': 'id_employe',
        "Pratique d'un sport": 'pratique_sportive'
    }
    df = df.rename(columns=rename_mapping)
    df = df.drop_duplicates(subset="id_employe")
    after_dedup = len(df)
    df = df.dropna(subset=["id_employe", "pratique_sportive"])
    final_len = len(df)

    logging.info(f"Sport : {original_len} lignes ➜ {after_dedup} après dédupli ➜ {final_len} après nettoyage.")
    return df

# === Fusion RH + sport ===
def merge_data(df_rh, df_sport):
    logging.info("Fusion des données RH et sportives...")
    return df_rh.merge(df_sport[['id_employe', 'pratique_sportive']], on="id_employe", how="left")


# === Insertion ou update ===
def upsert_employe(row):
    with engine.begin() as conn:
        conn.execute(sqlalchemy.text("""
            INSERT INTO employes (
                id_employe, nom, prenom, date_naissance, bu, date_embauche,
                salaire_brut, type_contrat, nb_jours_cp, adresse_domicile, mode_deplacement, pratique_sportive
            )
            VALUES (
                :id_employe, :nom, :prenom, :date_naissance, :bu, :date_embauche,
                :salaire_brut, :type_contrat, :nb_jours_cp, :adresse_domicile, :mode_deplacement, :pratique_sportive
            )
            ON CONFLICT (id_employe) DO UPDATE SET
                nom = EXCLUDED.nom,
                prenom = EXCLUDED.prenom,
                date_naissance = EXCLUDED.date_naissance,
                bu = EXCLUDED.bu,
                date_embauche = EXCLUDED.date_embauche,
                salaire_brut = EXCLUDED.salaire_brut,
                type_contrat = EXCLUDED.type_contrat,
                nb_jours_cp = EXCLUDED.nb_jours_cp,
                adresse_domicile = EXCLUDED.adresse_domicile,
                mode_deplacement = EXCLUDED.mode_deplacement,
                pratique_sportive = EXCLUDED.pratique_sportive;
        """), row)

# === Pipeline principal ===
def main():
    logging.info("Téléchargement des fichiers depuis S3...")
    df_rh = fetch_latest_excel_from_prefix(PREFIX_RH)
    df_sport = fetch_latest_excel_from_prefix(PREFIX_SPORT)

    df_rh_clean = clean_rh_data(df_rh)
    df_sport_clean = clean_sport_data(df_sport)

    df_final = merge_data(df_rh_clean, df_sport_clean)

    logging.info(f"{len(df_final)} employés à insérer ou mettre à jour...")

    for _, row in df_final.iterrows():
        upsert_employe(row.to_dict())
        logging.info(f"✅ Employé {row['id_employe']} traité.")

if __name__ == "__main__":
    main()