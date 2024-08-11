"""
--------Train Model----------- 
"""

import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import precision_score, recall_score
from sklearn.model_selection import train_test_split, GridSearchCV, KFold
from sklearn.preprocessing import LabelEncoder
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
from db_utils import Database
import random
import mlflow
from mlflow.tracking import MlflowClient

#--------------Logger setup
import logging
logger = logging.getLogger(__name__)

# --------------------------------------------------
# set up MLflow
# --------------------------------------------------

mlflow.set_tracking_uri("http://13.82.178.239:5001/")
mlflow.set_experiment("dpe_logement")
mlflow.sklearn.autolog()

#------Constants
TRAIN_COLUMNS = ["version_dpe", "type_energie_principale_chauffage", "type_energie_n_1"]

class NotEnoughSamples(ValueError):
    """Exception when there are not enough samples to train a model."""
    pass

def load_data_for_inference(n_samples):
    db = Database()
    query = f"select * from dpe_training limit {n_samples}"
    df = pd.read_sql(query, con=db.engine)
    db.close()
    # dump payload into new dataframe
    df["payload"] = df["payload"].apply(lambda d: json.loads(d))
    data = pd.DataFrame(list(df.payload.values))
    # data = data.astype(int)
    le = LabelEncoder()

    # Apply LabelEncoder to each column in the DataFrame
    for col in data.columns:
        data[col] = le.fit_transform(data[col])

    data.reset_index(inplace=True, drop=True)
    y = data["etiquette_ges"]
    X = data[TRAIN_COLUMNS]

    return X, y


def load_data_for_training(n_samples):
    db = Database()
    # SQL Query
    query = f"SELECT payload FROM dpe_training LIMIT {n_samples}"
    df = pd.read_sql(query, con=db.engine)
    db.close()
    # Convert the 'payload' JSON into a DataFrame
    df['payload'] = df['payload'].apply(json.loads)
    data = pd.DataFrame(df['payload'].tolist())
    le = LabelEncoder()
    for col in data.columns:
        data[col] = le.fit_transform(data[col])
    data.reset_index(inplace=True, drop=True)
    print(data.head())
    return data


def train_model():
    """Trains a RandomForest model and logs the model with MLflow."""
    data = load_data_for_training(200)  # Passing the sample size directly
    if data.shape[0] < 200:
        raise NotEnoughSamples("Not enough samples to train the model.")

    X = data[TRAIN_COLUMNS]
    y = data["etiquette_ges"]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
    param_grid = {
        "n_estimators": sorted([random.randint(1, 20) * 10 for _ in range(2)]),
        "max_depth": [random.randint(3, 10)],
        "min_samples_leaf": [random.randint(2, 5)],       
    }
    cv = KFold(n_splits=3, shuffle=True, random_state=42)
    grid_search = GridSearchCV(RandomForestClassifier(), param_grid, cv=cv, scoring="accuracy")
    grid_search.fit(X_train, y_train)

    y_pred = grid_search.predict(X_test)
    precision = precision_score(y_test, y_pred, average='weighted')
    recall = recall_score(y_test, y_pred, average='weighted')

    logger.info(f"Precision: {precision}, Recall: {recall}")

def create_champion():
    client = MlflowClient()
    model_name = "dpe_champion"  # Define the model name directly
    try:
        results = client.search_registered_models(filter_string=f"name='{model_name}'")
        if len(results) == 0:
            print("Champion model not found, creating one.")
            client.create_registered_model(model_name)
        else:
            print("Champion model already exists.")
    except Exception as e:
        print(f"An error occurred: {e}")


def promote_model():
    client = MlflowClient()
    champion_model_name = "dpe_champion"  # Define the model name directly, assuming this is consistent in your setup
    challenger_model_name = "dpe_challenger"  
    X, y = load_data_for_inference(200)
    # inference challenger and champion
    # load model & inference
    chl = mlflow.sklearn.load_model(f"models:/{challenger_model_name}/Staging")
    yhat = chl.best_estimator_.predict(X)
    challenger_precision = precision_score(y, yhat, average="weighted")
    challenger_recall = recall_score(y, yhat, average="weighted")
    print(f"\t challenger_precision: {np.round(challenger_precision, 2)}")
    print(f"\t challenger_recall: {np.round(challenger_recall, 2)}")

    # inference on production model
    champ = mlflow.sklearn.load_model(f"models:/{champion_model_name}/Staging")
    yhat = champ.best_estimator_.predict(X)
    champion_precision = precision_score(y, yhat, average="weighted")
    champion_recall = recall_score(y, yhat, average="weighted")
    print(f"\t champion_precision: {np.round(champion_precision, 2)}")
    print(f"\t champion_recall: {np.round(champion_recall, 2)}")

    # if performance 5% above current champion: promote
    if challenger_precision > champion_precision:
        print(f"{challenger_precision} > {champion_precision}")
        print("Promoting new model to champion ")
        champion_model = client.copy_model_version(
            src_model_uri=f"models:/{challenger_model_name}/Staging",
            dst_name=champion_model_name,
        )

        client.transition_model_version_stage(
            name=champion_model_name, version=champion_model_name.version, stage="Staging"
        )
    else:
        print(f"{challenger_precision} < {champion_precision}")
        print("champion remains undefeated ")


# Define the DAG
dag = DAG(
    "model_training_promotion",
    default_args={
        "owner": "airflow",
        "retries": 0,
        "retry_delay": timedelta(minutes=10),
        "start_date": datetime(2024, 1, 1),
        "catchup": False
    },
    description="Model training and promotion",
    schedule_interval=timedelta(days=1),
    tags=["ademe"]
)

train_model = PythonOperator(
    task_id="train_model", python_callable=train_model, dag=dag
    )

create_champion = PythonOperator(
    task_id="create_champion", python_callable=create_champion, dag=dag
    )

promote_model = PythonOperator(
    task_id="promote_model", python_callable=promote_model, dag=dag
    )

train_model >> create_champion >> promote_model

# comment
fkfdkfk

