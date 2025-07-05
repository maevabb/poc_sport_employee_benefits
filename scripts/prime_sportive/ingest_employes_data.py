import logging
import pandas as pd
import sqlalchemy
from io import BytesIO

# === Config ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
from config import (s3_client, BUCKET_NAME, PREFIX_CLEAN, engine)

# === Utilitaires pour téléchargement ===
def fetch_latest_employes_csv(prefix):
    """
    Télécharge depuis S3 le fichier CSV des employés nettoyés le plus récent (suffixe `_latest.csv`).
    """
    key = f"{prefix}cleaned_employes_latest.csv"
    logging.info(f"📄 Fichier chargé depuis S3 : {key}")
    obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
    return pd.read_csv(BytesIO(obj["Body"].read()))


# === Insertion ou update ===
def upsert_employe(row):
    """
    Insère ou met à jour les informations d’un employé dans la table `employes`.
    """
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
    """
    Pipeline principal :
    - Télécharge les données nettoyées depuis S3.
    - Insère ou met à jour chaque employé dans la base PostgreSQL.
    """
    logging.info("Téléchargement des fichiers depuis S3...")
    df = fetch_latest_employes_csv(PREFIX_CLEAN)
    
    logging.info(f"{len(df)} employés à insérer ou mettre à jour...")

    for _, row in df.iterrows():
        upsert_employe(row.to_dict())
        logging.info(f"✅ Employé {row['id_employe']} traité.")

if __name__ == "__main__":
    main()