import os
import sys
import logging
import pandas as pd
from io import BytesIO
from datetime import datetime

# === Config ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
scripts_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if scripts_path not in sys.path:
    sys.path.insert(0, scripts_path)
from config import (s3_client, BUCKET_NAME, PREFIX_RH, PREFIX_SPORT, PREFIX_CLEAN)

# === Utilitaires pour téléchargement ===
def fetch_latest_excel_from_prefix(prefix):
    """
    Télécharge le fichier Excel le plus récent depuis un préfixe donné dans un bucket S3.
    """
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
    """
    Nettoie et standardise les données issues du fichier RH :
    - Renommage des colonnes.
    - Encodage des noms/prénoms.
    - Suppression des doublons et des valeurs manquantes critiques..
    """
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

    for col in ["nom", "prenom"]:
        df[col] = (
            df[col]
            .astype(str)
            .str.encode("latin1", errors="ignore")
            .str.decode("utf-8", errors="ignore")
        )

    df = df.drop_duplicates(subset="id_employe")
    after_dedup = len(df)
    df = df.dropna(subset=["id_employe", "nom", "prenom", "salaire_brut"])
    final_len = len(df)

    logging.info(f"RH : {original_len} lignes initiales ➜ {after_dedup} après dédupli ➜ {final_len} après nettoyage.")
    return df

# === Nettoyage sport ===
def clean_sport_data(df):
    """
    Nettoie et standardise les données issues du fichier sport :
    - Renommage des colonnes.
    - Suppression des doublons et des enregistrements incomplets.
    """
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
    """
    Fusionne les données RH nettoyées avec les données sportives par ID employé.
    """
    logging.info("Fusion des données RH et sportives...")
    return df_rh.merge(df_sport[['id_employe', 'pratique_sportive']], on="id_employe", how="left")

# === Upload sur S3 ===
def upload_cleaned_data_to_s3(df):
    """
    Exporte les données nettoyées vers S3 sous deux versions :
    - Une version datée.
    - Une version 'latest' (toujours la plus récente).
    """
    date_str = datetime.today().strftime("%Y-%m-%d")
    dated_file_key = f"{PREFIX_CLEAN}cleaned_employes_{date_str}.csv"
    latest_file_key = f"{PREFIX_CLEAN}cleaned_employes_latest.csv"

    buffer_dated = BytesIO()
    df.to_csv(buffer_dated, index=False, encoding="utf-8-sig")
    buffer_dated.seek(0)
    s3_client.upload_fileobj(buffer_dated, BUCKET_NAME, dated_file_key)
    logging.info(f"✅ Données nettoyées enregistrées dans S3 ➜ {dated_file_key}")

    buffer_latest = BytesIO()
    df.to_csv(buffer_latest, index=False, encoding="utf-8-sig")
    buffer_latest.seek(0)
    s3_client.upload_fileobj(buffer_latest, BUCKET_NAME, latest_file_key)
    logging.info(f"✅ Données aussi disponibles sous ➜ {latest_file_key}")

# === Pipeline principal ===
def main():
    """
    Exécute l'ensemble du pipeline :
    - Téléchargement des fichiers RH et sport depuis S3.
    - Nettoyage des deux jeux de données.
    - Fusion des données.
    - Upload des fichiers nettoyés sur S3.
    """
    logging.info("Téléchargement des fichiers depuis S3...")
    df_rh = fetch_latest_excel_from_prefix(PREFIX_RH)
    df_sport = fetch_latest_excel_from_prefix(PREFIX_SPORT)

    df_rh_clean = clean_rh_data(df_rh)
    df_sport_clean = clean_sport_data(df_sport)

    df_final = merge_data(df_rh_clean, df_sport_clean)
    upload_cleaned_data_to_s3(df_final)


if __name__ == "__main__":
    main()