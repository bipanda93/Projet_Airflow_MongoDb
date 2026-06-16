from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
import csv
import os

dag = DAG(
    dag_id='sales_pipeline_project',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
)

# Partie 1 : FileSensor
wait_for_file = FileSensor(
    task_id='wait_for_file',
    filepath='/opt/airflow/data/ventes.csv',
    poke_interval=10,
    timeout=300,
    mode='reschedule',
    dag=dag
)

# Partie 2 : Validation
def validate_file(**context):
    filepath = '/opt/airflow/data/ventes.csv'
    if not os.path.exists(filepath):
        raise Exception(f"Fichier introuvable : {filepath}")
    if os.path.getsize(filepath) == 0:
        raise Exception("Fichier vide !")
    with open(filepath) as f:
        reader = csv.reader(f)
        rows = list(reader)
        nb_lignes = len(rows) - 1
    print(f"Fichier valide : {nb_lignes} enregistrements trouvés")
    return nb_lignes

task_validate = PythonOperator(
    task_id='validate_file',
    python_callable=validate_file,
    provide_context=True,
    dag=dag
)

# Partie 3 : Branching
def check_data(**context):
    nb_lignes = context['ti'].xcom_pull(task_ids='validate_file')
    if nb_lignes > 0:
        return 'process_data'
    else:
        return 'stop_pipeline'

task_branch = BranchPythonOperator(
    task_id='check_data',
    python_callable=check_data,
    provide_context=True,
    dag=dag
)

task_stop = DummyOperator(
    task_id='stop_pipeline',
    dag=dag
)

# Partie 4 : Calcul des stats
def extract_stats(**context):
    filepath = '/opt/airflow/data/ventes.csv'
    with open(filepath) as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    nb_ventes = len(rows)
    chiffre_affaires = sum(float(r['montant']) for r in rows)
    montant_moyen = chiffre_affaires / nb_ventes
    print(f"Nb ventes : {nb_ventes}")
    print(f"CA total : {chiffre_affaires}")
    print(f"Montant moyen : {montant_moyen:.2f}")
    return {
        'nb_ventes': nb_ventes,
        'chiffre_affaires': chiffre_affaires,
        'montant_moyen': round(montant_moyen, 2)
    }

task_process = PythonOperator(
    task_id='process_data',
    python_callable=extract_stats,
    provide_context=True,
    dag=dag
)

# Partie 5 : Dynamic Tasks
def analyse_produit(produit, **context):
    filepath = '/opt/airflow/data/ventes.csv'
    with open(filepath) as f:
        reader = csv.DictReader(f)
        rows = [r for r in reader if r['produit'] == produit]
    nb = len(rows)
    ca = sum(float(r['montant']) for r in rows)
    print(f"{produit} → {nb} ventes, CA={ca}, moy={ca/nb:.2f}")

def get_produits():
    filepath = '/opt/airflow/data/ventes.csv'
    if not os.path.exists(filepath):
        return ['unknown']
    with open(filepath) as f:
        reader = csv.DictReader(f)
        return list(set(r['produit'] for r in reader))

taches_produits = []
for produit in get_produits():
    t = PythonOperator(
        task_id=f'analyse_{produit.lower()}',
        python_callable=analyse_produit,
        op_kwargs={'produit': produit},
        dag=dag
    )
    taches_produits.append(t)

# Partie 7 : Rapport final
def generate_report(**context):
    stats = context['ti'].xcom_pull(task_ids='process_data')
    print("=" * 40)
    print("RAPPORT FINAL")
    print("=" * 40)
    print(f"Nb ventes      : {stats['nb_ventes']}")
    print(f"CA total       : {stats['chiffre_affaires']} €")
    print(f"Montant moyen  : {stats['montant_moyen']} €")
    print("=" * 40)
    with open('/opt/airflow/data/rapport.txt', 'w') as f:
        f.write("nb_ventes,chiffre_affaires,montant_moyen\n")
        f.write(f"{stats['nb_ventes']},{stats['chiffre_affaires']},{stats['montant_moyen']}\n")
    print("Rapport sauvegardé dans /opt/airflow/data/rapport.txt")

task_report = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    provide_context=True,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag
)

# Partie 8 : MongoDB
def store_mongodb(**context):
    from pymongo import MongoClient

    stats = context['ti'].xcom_pull(task_ids='process_data')

    client = MongoClient('mongodb://mini_airflow_mongodb:27017/')
    db = client['airflow_project']
    collection = db['ventes_metrics']

    document = {
        'execution_date': str(context['execution_date']),
        'dag_id': context['dag'].dag_id,
        'nb_ventes': stats['nb_ventes'],
        'chiffre_affaires': stats['chiffre_affaires'],
        'montant_moyen': stats['montant_moyen'],
        'status': 'success'
    }

    collection.insert_one(document)
    print(f"Document inséré dans MongoDB : {document}")
    client.close()

task_mongodb = PythonOperator(
    task_id='store_mongodb',
    python_callable=store_mongodb,
    provide_context=True,
    dag=dag
)

# Dépendances
wait_for_file >> task_validate >> task_branch
task_branch >> task_process
task_branch >> task_stop
task_process >> taches_produits
taches_produits >> task_report
task_report >> task_mongodb