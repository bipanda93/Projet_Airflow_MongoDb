# Projet Airflow MongoDB — Pipeline ETL Avancé

Pipeline ETL orchestré avec Apache Airflow, intégrant les concepts avancés d'orchestration de données et le stockage des métriques dans MongoDB.

## Stack Technique

- Apache Airflow 2.9.0 — Orchestration
- PostgreSQL 15 — Metadata DB Airflow
- MongoDB 7 — Stockage des métriques
- Docker — Conteneurisation
- Python 3.12 — Logique métier

## Concepts Airflow Implémentés

- FileSensor — Détection automatique du fichier CSV
- BranchPythonOperator — Workflow conditionnel
- XComs — Échange de données entre tâches
- Dynamic Tasks — Génération automatique par produit
- TriggerRule.ALL_DONE — Rapport généré même en cas d'erreur
- mode reschedule — Sensor non-bloquant

## Pipeline

FileSensor → Validation → Branching → Stats → Dynamic Tasks → Rapport → MongoDB

## Démarrage

git clone https://github.com/bipanda93/Projet_Airflow_MongoDb.git
cd Projet_Airflow_MongoDb
mkdir -p dags logs data
docker-compose up -d

## Accès

- Airflow UI : http://localhost:8182 — admin / admin
- MongoDB : localhost:27017
- PostgreSQL : localhost:5441 — airflow / airflow

## Configuration

Créer la connexion filesystem dans Airflow :
- Admin → Connections → +
- Connection Id : fs_default
- Connection Type : File (path)
- Path : /

## Auteur

Franck Ulrich BIPANDA — Mastère Data Engineering, Digital School de Paris
