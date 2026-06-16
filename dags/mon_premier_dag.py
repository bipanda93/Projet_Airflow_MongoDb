from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Arguments par défaut appliqués à toutes les tâches
default_args = {
    'retries': 3,
    'retry_delay': timedelta(seconds=10),
    'on_failure_callback': lambda context: print(f"ALERTE : la tâche {context['task_instance'].task_id} a échoué !")
}

dag = DAG(
    dag_id='mon_premier_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval='0 8 * * 1-5',
    catchup=False,
    default_args=default_args
)

def extract():
    print("Extraction de 42 offres CDI Data Engineer")
    return 42

def transform(**context):
    nb_offres = context['ti'].xcom_pull(task_ids='extract')
    print(f"Filtrage de {nb_offres} offres Paris uniquement")

def load():
    print("Rapport envoyé au manager !")

task_extract = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag
)

task_transform = PythonOperator(
    task_id='transform',
    python_callable=transform,
    provide_context=True,
    dag=dag
)

task_load = PythonOperator(
    task_id='load',
    python_callable=load,
    dag=dag
)

task_extract >> task_transform >> task_load