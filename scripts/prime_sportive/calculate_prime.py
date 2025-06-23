import os
import pandas as pd
import sqlalchemy
import logging
from dotenv import load_dotenv

load_dotenv()

# === Logger ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# === Connexion PostgreSQL ===

DATABASE_URL = "postgresql+psycopg2://postgres:postgres@localhost:5432/poc_avantages_sportifs"
engine = sqlalchemy.create_engine(DATABASE_URL)

# === Paramètres métier ===

DISTANCE_MAX_VELO = 25
DISTANCE_MAX_MARCHE = 15
TAUX_PRIME = 0.05

# === Lecture des données employes + distance ===

def fetch_employees_with_distance():
    query = """
        SELECT e.id_employe, e.salaire_brut, e.mode_deplacement, c.distance_km
        FROM employes e
        JOIN commuting_distance c ON e.id_employe = c.id_employe;
    """
    return pd.read_sql(query, con=engine)

# === Calcul métier de la prime sportive ===

def compute_prime(df):
    conditions = (
        ((df['mode_deplacement'] == 'Vélo/Trottinette/Autres') & (df['distance_km'] <= DISTANCE_MAX_VELO)) |
        ((df['mode_deplacement'] == 'Marche/running') & (df['distance_km'] <= DISTANCE_MAX_MARCHE))
    )
    df['eligible_ps'] = conditions
    df['montant_prime'] = df['salaire_brut'] * TAUX_PRIME * df['eligible_ps'].astype(int)
    return df[['id_employe', 'eligible_ps', 'montant_prime']]

# === Insertion ou mise à jour en base ===

def upsert_prime(id_employe, eligible_ps, montant_prime):
    with engine.begin() as conn:
        conn.execute(
            sqlalchemy.text("""
                INSERT INTO prime_sportive (id_employe, eligible_ps, montant_prime, date_calcul)
                VALUES (:id, :eligible, :prime, CURRENT_DATE)
                ON CONFLICT (id_employe) DO UPDATE SET
                    eligible_ps = EXCLUDED.eligible_ps,
                    montant_prime = EXCLUDED.montant_prime,
                    date_calcul = CURRENT_DATE
            """),
            {"id": id_employe, "eligible": eligible_ps, "prime": montant_prime}
        )

# === Pipeline principal ===

def main():
    df = fetch_employees_with_distance()
    logging.info(f"{len(df)} salariés à traiter pour la prime sportive.")

    df_prime = compute_prime(df)

    for _, row in df_prime.iterrows():
        upsert_prime(row['id_employe'], row['eligible_ps'], row['montant_prime'])
        logging.info(f"Employé {row['id_employe']} - Éligible : {row['eligible_ps']} - Prime : {row['montant_prime']}")

if __name__ == "__main__":
    main()
