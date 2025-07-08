# === PARAMS GENERAUX ===
WORK_ADDRESS = "1362 Avenue des Platanes, 34970 Lattes, France"


# === PARAMS PRIME SPORTIVE ===
DISTANCE_MAX_VELO = 25      # Distance max en km entre domicile/travail pour valider le mode de déplacement vélo
DISTANCE_MAX_MARCHE = 15    # Distance max en km entre domicile/travail pour valider le mode de déplacement marche
TAUX_PRIME = 0.05           # Taux prime sur salaire brut annuel si éligible


# === PARAMS JOURS BIEN-ETRE ===
NB_ACTIVITES_MIN = 15       # Nb d'activités minimum /an pour déclencher le droit aux jours bien-être
NB_JOURS_BE = 5             # Nb de jours bien-être accordés

# générateur d'activités sportives
NB_MESSAGES_DEFAULT = 2    # Nb d'activités générées
TIME_SLEEP_DEFAULT = 3      # Durée en secondes entre chaque message

SPORT_MAPPING = {
    'Runing': 'Running',
    'Tennis': 'Tennis',
    'Randonnée': 'Randonnée',
    'Natation': 'Natation',
    'Football': 'Football',
    'Rugby': 'Rugby',
    'Badminton': 'Badminton',
    'Voile': 'Voile',
    'Boxe': 'Boxe',
    'Judo': 'Judo',
    'Escalade': 'Escalade',
    'Triathlon': 'Triathlon',
    'Équitation': 'Équitation',
    'Tennis de table': 'Tennis de table',
    'Basketball': 'Basketball'
}

DISTANCE_RELEVANT_SPORTS = [
    'Running', 'Randonnée', 'Natation', 'Triathlon', 'Équitation', 'Voile'
]