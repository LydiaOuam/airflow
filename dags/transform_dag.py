"""
### Tutorial Documentation
Documentation that goes along with the Airflow tutorial located
[here](https://airflow.apache.org/tutorial.html)
"""

from datetime import datetime, timedelta
import logging
import pandas as pd
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from db_utils import Database
import numpy as np
from features import FeatureProcessor, FeatureSets
import json

logger = logging.getLogger(__name__)

# ---------------------------------------------
# instanciate class
# ---------------------------------------------


def transform():
    db = Database()
    columns = ",".join(FeatureSets.input_columns)
    query = f"""
        select {columns}
        from  logement
        where n_dpe not in (select n_dpe from db_ademe)
        order by id desc
        limit 200
    """
    data = pd.read_sql(query, con=db.engine)
    # handled by the DAG
    fp = FeatureProcessor(data, "etiquette_dpe")
    fp.process()
    print(fp.data.head())
    # save to db

    fp.data = fp.data.astype(str)
    for i, d in fp.data.iterrows():
        fp.data.loc[i, "payload"] = json.dumps(dict(d))

    fp.data[["n_dpe", "payload"]].to_sql(
        name="logement", con=db.engine, if_exists="append", index=False
    )

    db.close()


def drop_duplicates():
    query = """
        DELETE FROM logement
        WHERE n_dpe IN (
        SELECT n_dpe
        FROM (
            SELECT n_dpe, ROW_NUMBER() OVER (PARTITION BY n_dpe ORDER BY id DESC) AS rn
            FROM  logement
        ) t
        WHERE t.rn > 1
        );
    """

    db = Database()
    db.execute(query)
    db.close()


# ---------------------------------------------
#  DAG
# ---------------------------------------------

default_args = {
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "ademe_transform_data",
    default_args=default_args,
    description="Get raw data from dpe-tertiaire, transform and store into training_data",
    schedule='*/3 * * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:

    transform_data_task = PythonOperator(
        task_id="transform_data_task",
        python_callable=transform
    )

    drop_duplicates_task = PythonOperator(
        task_id="drop_duplicates_task",
        python_callable=drop_duplicates
    )


    transform_data_task >> drop_duplicates_task
