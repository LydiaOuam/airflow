from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

import glob
import json
import os
import time
from urllib.parse import parse_qs, urlparse

import pandas as pd
import requests
from azure.storage.blob import BlobServiceClient
import re
from db_utils import Database


DATA_PATH = f"/opt/airflow/data/"
URL_FILE = os.path.join(DATA_PATH, "api","url.json")
RESULTS_FILE = os.path.join(DATA_PATH,"api","results.json")
ACCOUNT_NAME = "mlopsstorage2024"
CONTAINER_NAME = "datamlops2024"
print(URL_FILE)


from airflow.models import Variable

try:
    ACCOUNT_KEY = Variable.get("STORAGE_BLOB_ADEME_MLOPS")
except:
    ACCOUNT_KEY = os.environ.get("STORAGE_BLOB_ADEME_MLOPS")



def rename_columns(columns):

    """

    rename columns

    """


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


def check_environment_setup():
    """vérifiez que tous les fichiers sont dispo et les variables d'environnement bien déclarées
    """
    logger.info("--" * 20)
    logger.info(f"[info logger] cwd: {os.getcwd()}")
    assert os.path.isfile(URL_FILE)
    assert os.path.isfile(RESULTS_FILE)
    logger.info(f"[info logger] URL_FILE: {URL_FILE}")
    logger.info(f"[info logger] RESULTS_FILE: {RESULTS_FILE}")
    logger.info("--" * 20)

def ademe_api():
    """get data from ademe API (version logement neufs pas DPE tertiaire)
    """
    assert os.path.isfile(URL_FILE), f"URL file not found: {URL_FILE}"
    # open url file
    with open(URL_FILE, encoding="utf-8") as file:
        url = json.load(file)
    assert url.get("url") is not None
    assert url.get("payload") is not None

    # make GET requests
    results = requests.get(url.get("url"), params=url.get("payload"), timeout=5)
    assert results.raise_for_status() is None
    data = results.json()

    # save results to file
    with open(RESULTS_FILE, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=4, ensure_ascii=False)


def process_results():

    """get next url"""

    # read previous API call output
    with open(RESULTS_FILE, encoding="utf-8") as file:
        data = json.load(file)

    # new url is same as old url
    base_url = data.get("next").split("?")[0]

    # extract payload as dict
    parsed_url = urlparse(data.get("next"))
    query_params = parse_qs(parsed_url.query)
    new_payload = {k: v[0] if len(v) == 1 else v for k, v in query_params.items()}

    # save new url (same as old url) with new payload into url.json
    new_url = {"url": base_url, "payload": new_payload}

    with open(URL_FILE, "w", encoding="utf-8") as file:
        json.dump(new_url, file, indent=4, ensure_ascii=False)

    # saves data to data file
    # append current timestamp (up to the second to the filename)
    timestamp = int(time.time())
    data_filename = os.path.join(DATA_PATH, f"data_{timestamp}.json")

    with open(data_filename, "w", encoding="utf-8") as file:
        json.dump(data["results"], file, indent=4, ensure_ascii=False)






def save_postgresdb():
    """
    Save fetched data into PostgreSQL database.
    """
    assert os.path.isfile(RESULTS_FILE), f"Results file not found: {RESULTS_FILE}"
    with open(RESULTS_FILE, encoding="utf-8") as file:
        data = json.load(file)

    df = pd.DataFrame(data["results"])
    new_columns = rename_columns(df.columns)
    df.columns = new_columns
    df = df.astype(str).replace("nan", "")
    df['payload'] = df.apply(lambda row: json.dumps(row.to_dict()), axis=1)
    df = df[['n_dpe', 'payload']]

    db = Database()
    try:
        df.to_sql(name="dpe_logement", con=db.engine, if_exists="append", index=False)
        logger.info("Data successfully stored to the database.")
    except Exception as e:
        logger.error("An error occurred while storing data: %s", e)
    finally:
        db.close()



def drop_duplicates():
    """
    Drop duplicate rows in the PostgreSQL table `dpe_logement`.
    Duplicates are identified based on `n_dpe` and `payload` columns.
    """
    db = Database()
    try:
        with db.engine.connect() as conn:
            # Identify and delete duplicate rows, keeping the first occurrence
            conn.execute(text("""
                DELETE FROM dpe_logement
                WHERE ctid NOT IN (
                    SELECT min(ctid)
                    FROM dpe_logement
                    GROUP BY n_dpe, payload
                );
            """))
            logger.info("Duplicates successfully dropped from the database.")
    except Exception as e:
        logger.error(f"An error occurred while dropping duplicates: {e}")
    finally:
        db.close()




default_args = {
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "load_data",
    default_args=default_args,
    description="Get ademe data",
    schedule='*/5 * * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:


    check_environment_setup = PythonOperator(
        task_id="check_environment_setup",
        python_callable=check_environment_setup,
    )

    ademe_api = PythonOperator(
        task_id="ademe_api",
        python_callable=ademe_api,
    )
    process_results = PythonOperator(
        task_id="process_results",
        python_callable=process_results,
    )
    save_postgresdb = PythonOperator(
        task_id="save_postgresdb",
        python_callable=save_postgresdb,
    )
    
    
    drop_duplicates = PythonOperator(
        task_id="drop_duplicates",
        python_callable=drop_duplicates,
    )
        

    check_environment_setup >> ademe_api >> process_results >> save_postgresdb >> drop_duplicates 
