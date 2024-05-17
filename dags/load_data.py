"""
### Tutorial Documentation
Documentation that goes along with the Airflow tutorial located
[here](https://airflow.apache.org/tutorial.html)
"""

from __future__ import annotations
import os
import re
import json
import time
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs
import logging
import typing as t
import requests
import pandas as pd
import psycopg2
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable





class Database:
    def __init__(self):
        try:
            pg_password = Variable.get("AZURE_PG_PASSWORD")
        except:
            pg_password = os.environ.get("AZURE_PG_PASSWORD")
        db_params = {
            "dbname": "db_ademe",
            "user": "ouamrane_lydia2022",
            "password": 'pg_password',
            "host": "postgres-mlops.postgres.database.azure.com",
            "port": "5432",
            "sslmode": "require",
        }

        self.connection = psycopg2.connect(**db_params)
        self.engine = create_engine(
            f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
        )

    def insert(self, insert_query):
        cursor = self.connection.cursor()

        cursor.execute(insert_query)
        self.connection.commit()
        cursor.close()

    def execute(self, query_):
        cursor = self.connection.cursor()

        cursor.execute(query_)
        self.connection.commit()
        cursor.close()

    def close(self):
        self.connection.close()
        self.engine.dispose()

logger = logging.getLogger(__name__)

# Chemin du fichier de résultats
try:
    DATA_PATH = "/opt/airflow/data/"
except: # pylint: disable=bare-except
    DATA_PATH = "/Users/guill/Desktop/MLops-ynov/data/"

DOWNLOADED_FILES_PATH = os.path.join(DATA_PATH)
URL_FILE_LOAD_DATA = os.path.join(DATA_PATH, "api", "url.json")
RESULTS_FILE_LOAD_DATA = os.path.join(DATA_PATH, "api", "results.json")

def check_environment_setup():
    """Check Env Setup"""
    logger.info("Checking environment setup...")
    logger.info("[info logger] cwd: %s",os.getcwd())
    logger.info("[info logger] URL_FILE: %s",URL_FILE_LOAD_DATA)
    assert os.path.isfile(URL_FILE_LOAD_DATA)
    logger.info("[info logger] RESULTS_FILE: %s",RESULTS_FILE_LOAD_DATA)

    try:
        pg_password = Variable.get("AZURE_PG_PASSWORD")
    except: # pylint: disable=bare-except
        pg_password = os.environ.get("AZURE_PG_PASSWORD")

    assert pg_password is not None


def ademe_api():
    """Get Data from Ademe API"""
    # test url file exists
    assert os.path.isfile(URL_FILE_LOAD_DATA)
    # open url file
    with open(URL_FILE_LOAD_DATA, encoding="utf-8") as file:
        url_load_data = json.load(file)
    assert url_load_data.get("url") is not None
    assert url_load_data.get("payload") is not None

    # make GET requests load data
    results_load_data = requests.get(
        url_load_data.get("url"),
        params=url_load_data.get("payload"),
        timeout=5)
    assert results_load_data.raise_for_status() is None

    data = results_load_data.json()

    # save results to file
    with open(RESULTS_FILE_LOAD_DATA, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=4, ensure_ascii=False)

def process_results():
    """Process results"""
    # test url file exists
    assert os.path.isfile(RESULTS_FILE_LOAD_DATA)

    # read previous API call output
    with open(RESULTS_FILE_LOAD_DATA, encoding="utf-8") as file:
        data = json.load(file)

    # new url is same as old url
    base_url = data.get("next").split("?")[0]

    # extract payload as dict
    parsed_url = urlparse(data.get("next"))
    query_params_data = parse_qs(parsed_url.query)
    new_payload = {
        k: v[0] if len(v) == 1 else v for k, v in query_params_data.items()
    }

    # save new url (same as old url) with new payload into url.json
    new_url = {"url": base_url, "payload": new_payload}

    with open(URL_FILE_LOAD_DATA, "w", encoding="utf-8") as file:
        json.dump(new_url, file, indent=4, ensure_ascii=False)

    # saves data to data file
    # append current timestamp (up to the second to the filename)
    timestamp = int(time.time())
    data_filename = os.path.join(
        DOWNLOADED_FILES_PATH,
        f"data_{timestamp}.json"
    )

    with open(data_filename, "w", encoding="utf-8") as file:
        json.dump(data["results"], file, indent=4, ensure_ascii=False)

# Initialisation de la connexion à la base de données PostgreSQL
def init_db_connection(pg_password: str) -> psycopg2._psycopg.connection:
    db = Database()
    query = "select count(*) from logement;"
    cur = db.connection.cursor()
    # Execute a query
    cur.execute(query)
    # Retrieve query results
    n = cur.fetchone()
    print("n:", n)

    active_connections = db.engine.pool.status()
    print(f"-- active connections {active_connections}")

    db.close()

# Fonction pour renommer les colonnes
def rename_columns(columns: t.List[str]) -> t.List[str]:
    """Rename columuns"""
    columns = [col.lower() for col in columns]

    rgxs = [
        (r"[°|/|']", "_"),
        (r"²", "2"),
        (r"[(|)]", ""),
        (r"é|è", "e"),
        (r"â", "a"),
        (r"^_", "dpe_"),
        (r"_+", "_"),
    ]
    for rgx in rgxs:
        columns = [re.sub(rgx[0], rgx[1], col) for col in columns]

    return columns

def drop_duplicates():
    """Drop duplicates"""
    query = """
        DELETE FROM dpe_logement
        WHERE id IN (
        SELECT id
        FROM (
            SELECT id, ROW_NUMBER() OVER (PARTITION BY n_dpe ORDER BY id DESC) AS rn
            FROM dpe_logement
        ) t
        WHERE t.rn > 1
        );
    """

    pg_password = Variable.get("AZURE_PG_PASSWORD")

    db = init_db_connection(pg_password)
    db.execute(query)
    db.close()

def upload_data():
    """
    Upload data to Azure  Azure Database for PostgreSQL
    """
    pg_password = Variable.get("AZURE_PG_PASSWORD")

    # Get all files in the data directory
    files = os.listdir(f'{DOWNLOADED_FILES_PATH}/*.json')
    files = [f for f in files if os.path.isfile(f)]

    # Connect to the database
    db = init_db_connection(pg_password)

    # Loop through all files and upload them to the database
    for file in files:
        with open(file, encoding="utf-8") as f:
            data = json.load(f)

        data = pd.DataFrame(data["results"])

        # Rename columns
        new_columns = rename_columns(data.columns)
        data.columns = new_columns
        data = data.astype(str).replace("nan", "")

        # Insert data into the database
        data.to_sql(name="dpe_logement", con=db, if_exists="replace", index=False)

    # Close the database connection
    db.close()

def cleanup_local_data():
    """
    Cleanup local data
    """
    # Get all files in the data directory
    files = os.listdir(f'{DOWNLOADED_FILES_PATH}/*.json')
    files = [f for f in files if os.path.isfile(f)]

    # Loop through all files and delete them
    for file in files:
        os.remove(file)

# Chargement des données dans la base de données PostgreSQL
def save_postgresdb():
    """ Save PostgreSQL database"""
    assert os.path.isfile(RESULTS_FILE_LOAD_DATA)

    # Lire la sortie de l'appel API précédent
    with open(RESULTS_FILE_LOAD_DATA, encoding="utf-8") as file:
        data = json.load(file)

    data = pd.DataFrame(data["results"])

    # Renommer les colonnes
    new_columns = rename_columns(data.columns)
    data.columns = new_columns
    data = data.astype(str).replace("nan", "")

    pg_password = Variable.get("AZURE_PG_PASSWORD")

    # Connecter à PostgreSQL
    db = init_db_connection(pg_password)

    # Insérer les données dans la base de données
    db.to_sql(name="dpe_logement", con=db, if_exists="replace", index=False)

    # Fermer la connexion à la base de données
    db.close()

# Définition du DAG
with DAG(
    "load_data",
    default_args={
        "depends_on_past": False,
        "email": ["guillaume.dupuy@ynov.com"],
        "email_on_failure": True,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    description="Get data from ADEME API, save to postgres, then clean up",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ademe"],
) as dag:

    # Vérifier la configuration de l'environnement
    check_environment_setup_task = PythonOperator(
        task_id="check_environment_setup",
        python_callable=check_environment_setup,
    )

    ademe_api_task = PythonOperator(
        task_id="ademe_api",
        python_callable=ademe_api,
    )

    process_results_task = PythonOperator(
        task_id="process_results",
        python_callable=process_results,
    )

    # Tâche pour charger les données dans PostgreSQL
    save_postgresdb_task = PythonOperator(
        task_id="save_postgresdb",
        python_callable=save_postgresdb,
    )

    drop_duplicates_task = PythonOperator(
        task_id="drop_duplicates",
        python_callable=drop_duplicates,
    )

    cleanup_local_data_task = PythonOperator(
        task_id="cleanup_local_data",
        python_callable=cleanup_local_data,
    )

    print(check_environment_setup_task >> ademe_api_task >> process_results_task >>
            save_postgresdb_task >> drop_duplicates_task >> cleanup_local_data_task)