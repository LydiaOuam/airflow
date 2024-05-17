"""
### Tutorial Documentation
Documentation that goes along with the Airflow tutorial located
[here](https://airflow.apache.org/tutorial.html)
"""
import json
from datetime import datetime, timedelta
import pandas as pd

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

from src import FeatureSets, Database, FeatureProcessor

# ---------------------------------------------
# instanciate class
# ---------------------------------------------

def transform():
    """Transform data"""
    db = Database()
    columns = ",".join(FeatureSets.input_columns)
    query = f"""
        select {columns}
        from dpe_tertiaire
        where n_dpe not in (select n_dpe from dpe_training)
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
        name="dpe_training", con=db.engine, if_exists="append", index=False
    )

    db.close()

def drop_duplicates():
    """Drop duplicates"""
    query = """
        DELETE FROM dpe_training
        WHERE n_dpe IN (
        SELECT n_dpe
        FROM (
            SELECT n_dpe, ROW_NUMBER() OVER (PARTITION BY n_dpe ORDER BY id DESC) AS rn
            FROM dpe_training
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
with DAG(
    "ademe_transform_data",
    default_args={
        "retries": 0,
        "retry_delay": timedelta(minutes=10),
    },
    description="Get raw data from dpe-tertiaire, transform and store into training_data",
    schedule="*/3 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ademe"],
) as dag:
    transform_data_task = PythonOperator(task_id="transform_data_task", python_callable=transform)
    drop_duplicates_task = PythonOperator(
        task_id="drop_duplicates_task", python_callable=drop_duplicates
    )

    print(transform_data_task >> drop_duplicates_task)