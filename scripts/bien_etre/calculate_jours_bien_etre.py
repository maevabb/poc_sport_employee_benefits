import pandas as pd
import sqlalchemy
import logging
from datetime import datetime, timedelta

# === Config ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
from scripts.config import (engine)

# === Paramètres métier ===
from scripts.params import (NB_ACTIVITES_MIN, NB_JOURS_BE)

# === Lecture des activités sportives des 12 derniers mois ===
def fetch_eligible_activities():
    """
    Récupère le nombre d'activités sportives pour chaque employé sur les 12 derniers mois.
    """
    query = """
        SELECT id_employe, COUNT(*) AS nb_activites
        FROM activities_sportives
        WHERE type_activite NOT ILIKE 'commuting'
          AND debut_activite >= CURRENT_DATE - INTERVAL '12 months'
        GROUP BY id_employe;
    """
    return pd.read_sql(query, con=engine)

# === Lecture des jours CP actuels dans employes ===
def fetch_cp_base():
    """
    Récupère le nombre actuel de jours de congés payés (CP) pour chaque employé.
    """
    query = "SELECT id_employe, nb_jours_cp FROM employes"
    return pd.read_sql(query, con=engine)

# === Calcul des jours bien-être et éligibilité ===
def compute_bien_etre(df_acts, df_cp):
    """
    Calcule l'éligibilité des employés aux journées bien-être et le nombre total de jours de CP.
    """
    df = df_acts.merge(df_cp, on="id_employe", how="left")
    df['eligible_be'] = df['nb_activites'] >= NB_ACTIVITES_MIN
    df['nb_jours_bien_etre'] = df['eligible_be'].astype(int) * NB_JOURS_BE
    df['nb_jours_cp_total'] = df['nb_jours_cp'].fillna(0) + df['nb_jours_bien_etre']
    return df[['id_employe', 'nb_activites', 'eligible_be', 'nb_jours_bien_etre', 'nb_jours_cp_total']]

# === Insertion ou mise à jour en base ===
def upsert_cp_be(row):
    """
    Insère ou met à jour les données de jours bien-être et de CP total
    pour un employé donné dans la table 'cp_bien_etre'.
    """
    with engine.begin() as conn:
        conn.execute(
            sqlalchemy.text("""
                INSERT INTO cp_bien_etre (id_employe, nb_activites, eligible_be, nb_jours_bien_etre, nb_jours_cp_total, date_calcul)
                VALUES (:id, :nb_acts, :eligible, :nb_be, :nb_total, CURRENT_DATE)
                ON CONFLICT (id_employe) DO UPDATE SET
                    nb_activites = EXCLUDED.nb_activites,
                    eligible_be = EXCLUDED.eligible_be,
                    nb_jours_bien_etre = EXCLUDED.nb_jours_bien_etre,
                    nb_jours_cp_total = EXCLUDED.nb_jours_cp_total,
                    date_calcul = CURRENT_DATE;
            """),
            {
                "id": row['id_employe'],
                "nb_acts": row['nb_activites'],
                "eligible": row['eligible_be'],
                "nb_be": row['nb_jours_bien_etre'],
                "nb_total": row['nb_jours_cp_total']
            }
        )

# === Pipeline principal ===
def main():
    """
    Exécute le pipeline complet de calcul des jours bien-être :
    - Récupération des données d'activités et de CP.
    - Calcul de l'éligibilité.
    - Insertion ou mise à jour en base.
    """
    df_acts = fetch_eligible_activities()
    df_cp = fetch_cp_base()

    logging.info(f"{len(df_acts)} salariés avec au moins une activité éligible détectés.")

    df_be = compute_bien_etre(df_acts, df_cp)

    for _, row in df_be.iterrows():
        upsert_cp_be(row)
        logging.info(f"Employé {row['id_employe']} - Eligible BE : {row['eligible_be']} - Jours BE : {row['nb_jours_bien_etre']} - Total CP : {row['nb_jours_cp_total']}")

if __name__ == "__main__":
    main()