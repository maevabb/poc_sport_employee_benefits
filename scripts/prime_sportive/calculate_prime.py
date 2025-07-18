import sqlalchemy
import pandas as pd
import logging

# === Config ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
from config import (engine)

# === Paramètres métier ===
from params import (DISTANCE_MAX_VELO, DISTANCE_MAX_MARCHE, TAUX_PRIME,)

# === Lecture des données employes + distance ===

def fetch_employees_with_distance():
    """
    Récupère les données des employés avec leur salaire brut, leur mode de déplacement
    et la distance domicile-travail depuis la base de données.
    """
    query = """
        SELECT e.id_employe, e.salaire_brut, e.mode_deplacement, c.distance_km
        FROM employes e
        JOIN commuting_distance c ON e.id_employe = c.id_employe;
    """
    return pd.read_sql(query, con=engine)

# === Calcul métier de la prime sportive ===

def compute_prime(df):
    """
    Calcule l'éligibilité à la prime sportive et le montant correspondant.
    """
    conditions = (
        ((df['mode_deplacement'] == 'Vélo/Trottinette/Autres') & (df['distance_km'] <= DISTANCE_MAX_VELO)) |
        ((df['mode_deplacement'] == 'Marche/running') & (df['distance_km'] <= DISTANCE_MAX_MARCHE))
    )
    df['eligible_ps'] = conditions
    df['montant_prime'] = df['salaire_brut'] * TAUX_PRIME * df['eligible_ps'].astype(int)
    return df[['id_employe', 'eligible_ps', 'montant_prime']]

# === Insertion ou mise à jour en base ===

def upsert_prime(id_employe, eligible_ps, montant_prime):
    """
    Insère ou met à jour les données de prime sportive pour un employé dans la base de données.
    """
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
    """
    Pipeline principal de calcul de la prime sportive :
    - Récupération des données d'employés.
    - Calcul de l'éligibilité et du montant.
    - Insertion ou mise à jour en b
    """
    df = fetch_employees_with_distance()
    logging.info(f"{len(df)} salariés à traiter pour la prime sportive.")

    df_prime = compute_prime(df)

    for _, row in df_prime.iterrows():
        upsert_prime(row['id_employe'], row['eligible_ps'], row['montant_prime'])
        logging.info(f"Employé {row['id_employe']} - Éligible : {row['eligible_ps']} - Prime : {row['montant_prime']}")

if __name__ == "__main__":
    main()
