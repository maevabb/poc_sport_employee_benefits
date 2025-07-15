# POC Avantages Sportifs — Sport Data 
✨ Author: Maëva Beauvillain 📅 Start Date: Juin 2025 📅  Last Updated: 15 Juillet 2025

Ce projet constitue un **POC** pour l’entreprise Sport Data Solution.  
L'objectif est de construire un pipeline de données permettant :

- Le calcul de la prime sportive des collaborateurs, basée sur leurs modes de déplacement domicile-travail.
- Le calcul de jours "bien-être" supplémentaires, en fonction de leur pratique sportive régulière.

Ce POC vise à démontrer la faisabilité technique et à poser les bases d'une future industrialisation.

---

## Table des matières

1. [Objectif du projet](#1-objectif-du-projet)  
2. [Arborescence](#2-arborescence-du-projet)  
3. [Stack Technique](#3-stack-technique)  
4. [Architecture du projet](#4-architecture-du-projet)  
5. [Résultats & KPI](#5-résultats--kpi)  
6. [Instructions de déploiement](#6-instructions-de-déploiement)  

---

## 1. Objectif du projet

Mettre en place un pipeline automatisé capable de :
- Collecter et transformer des données RH et sportives
- Calculer automatiquement les avantages éligibles (prime / jours)
- Garantir la qualité des données injectées
- Publier des messages Slack pour créer une dynamique collective
- Visualiser les résultats RH dans Power BI

---

## 2. Arborescence du projet
```markdown
.
├── airflow/dags/               # Pipelines Airflow (prime & bien-être)
├── docs/                       # Présentation, cadrage, Power BI, schémas
├── great_expectations/         # Suites de validation et checkpoints
├── scripts/
│   ├── prime_sportive/         # Scripts liés à la prime (ingestion, distance, calcul)
│   ├── bien_etre/              # Scripts liés aux jours bien-être (génération, validation, calcul)
│   └── sql/                    # Script de création du schéma PostgreSQL
├── docker-compose.yml          # Orchestration des services
├── Dockerfile.airflow          # Dockerfile pour Airflow
├── Dockerfile.consumer         # Dockerfile pour le consumer Kafka
├── pyproject.toml              # Dépendances Python via Poetry
├── requirements-*.txt          # Dépendances séparées (Airflow, consumer)
└── README.md                   # Ce fichier

```

---

## 3. Stack technique

| Domaine                | Outils utilisés                                                |
|------------------------|----------------------------------------------------------------|
| Développement          | Python, Poetry, Docker                                         |
| Orchestration          | Apache Airflow                                                 |
| Tests de données       | Great Expectations                                             |
| Stockage               | PostgreSQL, Amazon S3                                          |
| API externe            | Google Maps API                                                |
| Streaming              | Redpanda (Kafka compatible)                                    |
| Visualisation          | Power BI                                                       |
| Notification           | Slack                                                          |

---

## 4. Architecture du projet
Le pipeline combine des traitements **batch** et ****streaming**, orchestrés avec Airflow et conteneurisés via **Docker**.
L’objectif est de collecter, transformer, valider et valoriser les données RH et sportives de manière automatique et sécurisée.

### Flux batch

- **Nettoyage des fichiers RH & sport**  
  → Scripts Python pour normaliser les colonnes, typer les données, et exporter en `.csv`

- **Ingestion dans PostgreSQL**  
  → Les données nettoyées sont insérées dans la base après passage d’un **checkpoint Great Expectations**

- **Calcul des distances domicile-bureau**  
  → Utilisation de l’**API Google Maps** pour calculer les distances en km selon le mode de déplacement déclaré

- **Calcul des primes sportives**  
  → Attribution de 5% de prime si les conditions sont remplies (distance raisonnable et mode actif)

- **Calcul des jours bien-être**  
  → Comptabilisation des activités sur 12 mois, attribution de 5 jours si 15 activités validées

- **Restitution via Power BI**  
  → Extraction des résultats pour visualisation RH des KPI

### Flux streaming

- **Génération d’activités sportives simulées**  
  → Un script Python publie dans un topic **Redpanda** (Kafka-like)

- **Consumer Kafka**  
  → Un autre script consomme les événements, valide les données avec **Great Expectations**, puis les insère en base PostgreSQL

- **Publication Slack**  
  → En parallèle, une notification personnalisée est automatiquement envoyée dans un canal Slack simulé, pour créer de l’émulation


### Services conteneurisés

Tous les composants sont packagés avec Docker :

- `airflow` : orchestration des DAGs  
- `redpanda` : broker streaming  
- `consumer` : script de consommation Kafka  
- `postgres` : base relationnelle  
- `great_expectations` : test de qualité

> Voir le fichier [`docker-compose.yml`](./docker-compose.yml) pour la configuration complète.

---

## 5. Résultats & KPI

Le POC produit plusieurs indicateurs clés visibles dans Power BI :

- Nombre d’activités sportives enregistrées (par mois / par salarié)
- Nombre de salariés éligibles à la prime sportive
- Montants totaux et moyens des primes versées
- Nombre de jours bien-être attribués
- Répartition des modes de déplacement actifs
- Données consolidées par service ou business unit (BU)

> Rapport Power BI disponible dans [`docs/rapport_power_BI.pdf`](./docs/rapport_power_BI.pdf)

---

## 6. Instructions de déploiement

### Prérequis :
- Python 3.11+
- Docker + Docker Compose
- Poetry installé (`pip install poetry`)

### Étapes d'installation et de lancement :

1. **Cloner le dépôt**
```bash
git clone <url_du_repo>
cd poc-avantages-sportifs
```
2. **Installer les dépendances Python avec Poetry**
``` bash
poetry install
```
3. **Lancer les services Docker**
``` bash
docker-compose up -d --build
```
4. **Accéder à l’interface Airflow**
- **URL** : [http://localhost:8080](http://localhost:8080)

5. **Activer les DAGs dans l’interface Airflow**
- `pipeline_prime_sportive`
- `pipeline_jours_bien_etre`

6. **Consulter les résultats**
- Les données traitées sont disponibles dans la base **PostgreSQL**
- Les messages d’activité sont diffusés automatiquement dans **Slack** (ou visibles dans les logs du `consumer`)
- Le rapport statique Power BI est consultable dans [`docs/rapport_power_BI.pdf`](./docs/rapport_power_BI.pdf)

---

Projet développé dans le cadre du parcours **Data Engineer** par OpenClassrooms.