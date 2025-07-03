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
PREFIX_CLEAN = "clean_data/"

DATABASE_URL = "postgresql+psycopg2://postgres:postgres@localhost:5432/poc_avantages_sportifs"
engine = sqlalchemy.create_engine(DATABASE_URL)

# === S3 Client ===
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)

# === Utilitaires pour téléchargement ===
def fetch_latest_employes_csv(prefix):
    key = f"{prefix}cleaned_employes_latest.csv"
    logging.info(f"📄 Fichier chargé depuis S3 : {key}")
    obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
    return pd.read_csv(BytesIO(obj["Body"].read()))


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
    df = fetch_latest_employes_csv(PREFIX_CLEAN)
    
    logging.info(f"{len(df)} employés à insérer ou mettre à jour...")

    for _, row in df.iterrows():
        upsert_employe(row.to_dict())
        logging.info(f"✅ Employé {row['id_employe']} traité.")

if __name__ == "__main__":
    main()