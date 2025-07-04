-- Nettoyage préalable
DROP TABLE IF EXISTS cp_bien_etre;
DROP TABLE IF EXISTS prime_sportive;
DROP TABLE IF EXISTS commuting_distance;
DROP TABLE IF EXISTS activities_sportives;
DROP TABLE IF EXISTS employees;

-- Table principale RH
CREATE TABLE employes (
    id_employe INTEGER PRIMARY KEY,
    nom TEXT NOT NULL,
    prenom TEXT NOT NULL,
    date_naissance DATE NOT NULL CHECK (date_naissance > '1900-01-01' AND date_naissance <= CURRENT_DATE),
    bu TEXT,
    date_embauche DATE CHECK (date_embauche <= CURRENT_DATE),
    salaire_brut NUMERIC NOT NULL CHECK (salaire_brut > 0),
    type_contrat TEXT,
    nb_jours_cp INTEGER NOT NULL CHECK (nb_jours_cp >= 0),
    adresse_domicile TEXT NOT NULL,
    mode_deplacement TEXT NOT NULL CHECK (mode_deplacement IN ('véhicule thermique/électrique', 'Vélo/Trottinette/Autres', 'Transports en commun', 'Marche/running')),
    pratique_sportive TEXT
);

-- Table des distances calculées
CREATE TABLE commuting_distance (
    id_employe INTEGER PRIMARY KEY REFERENCES employes(id_employe),
    adresse_domicile TEXT,
    distance_km NUMERIC,
    date_calcul DATE DEFAULT CURRENT_DATE,
    statut_distance TEXT
);

-- Table des primes calculées
CREATE TABLE prime_sportive (
    id_employe INTEGER PRIMARY KEY REFERENCES employes(id_employe),
    eligible_ps BOOLEAN,
    montant_prime NUMERIC,
    date_calcul DATE DEFAULT CURRENT_DATE
);

-- Table des activités sportives simulées
CREATE TABLE activities_sportives (
    id_activite SERIAL PRIMARY KEY,
    id_employe INTEGER NOT NULL REFERENCES employes(id_employe),
    debut_activite DATE NOT NULL,
    fin_activite DATE NOT NULL,
    type_activite TEXT,
    distance INTEGER,
    temps_sec INTEGER,
    commentaire TEXT
);

-- Table des jours bien-être calculés
CREATE TABLE cp_bien_etre (
    id_employe INTEGER PRIMARY KEY REFERENCES employes(id_employe),
    nb_activites INTEGER,
    eligible_be BOOLEAN,
    nb_jours_bien_etre INTEGER,
    nb_jours_cp_total INTEGER,
    date_calcul DATE DEFAULT CURRENT_DATE
);
