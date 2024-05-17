"""
Training the model
"""
import json
from datetime import datetime, timedelta
import random
import pandas as pd
import numpy as np


from sklearn.metrics import precision_score, recall_score
from sklearn.model_selection import train_test_split, GridSearchCV, KFold
from sklearn.ensemble import RandomForestClassifier
import mlflow
from mlflow import MlflowClient

from airflow.operators.python import PythonOperator
from airflow.models import DAG

from src import FeatureSets, Database

class NotEnoughSamples(ValueError):
    """NES"""

# --------------------------------------------------
# TrainDPE class
# --------------------------------------------------


class TrainDPE:
    """TrainDPE Class"""
    minimum_training_samples = 500
    def __init__(self, data, target="etiquette_dpe"):
        """Construct"""
        # drop samples with no target
        data = data[data[target] >= 0].copy()
        data.reset_index(inplace=True, drop=True)
        if data.shape[0] < TrainDPE.minimum_training_samples:
            raise NotEnoughSamples(
                "data has {data.shape[0]} samples, which is not enough to train a model."
                    + " min required {TrainDPE.minimum_training_samples}"
            )

        self.data = data
        print(f"training on {data.shape[0]} samples")
        self.target = target
        self.model = RandomForestClassifier()
        self.params = {}
        self.train_score = 0.0
        self.probabilities = [0.0, 0.0]

    def main(self):
        """Train a model"""

        param_grid = {
            "n_estimators": sorted([random.randint(1, 20) * 10 for _ in range(2)]),
            "max_depth": [random.randint(3, 10)],
            "min_samples_leaf": [random.randint(2, 5)],
        }
        n_splits = 3
        test_size = 0.3
        precision = 0.0
        recall = 0.0

        # shuffle

        feature = self.data[FeatureSets.train_columns].copy()  # Features
        target = self.data[self.target].copy()  # Target variable

        # Split the data into training and testing sets
        features_train, feature_test, y_train, y_test = train_test_split(
            feature, target, test_size=test_size, random_state=808
        )

        # Setup GridSearchCV with k-fold cross-validation
        cv = KFold(n_splits=n_splits, random_state=42, shuffle=True)

        grid_search = GridSearchCV(
            estimator=self.model, param_grid=param_grid, cv=cv, scoring="accuracy"
        )

        # Fit the model
        grid_search.fit(features_train, y_train)

        self.model = grid_search.best_estimator_
        self.params = grid_search.best_params_
        self.train_score = grid_search.best_score_

        yhat = grid_search.predict(feature_test)
        precision = precision_score(y_test, yhat, average="weighted")
        recall = recall_score(y_test, yhat, average="weighted")
        self.probabilities = np.max(grid_search.predict_proba(feature_test), axis=1)
        return precision, recall

    def report(self, precision, recall):
        """Report the model"""
        # Best parameters and best score
        print("--" * 20, "Best model")
        print(f"\tparameters: {self.params}")
        print(f"\tcross-validation score: {self.train_score}")
        print(f"\tmodel: {self.model}")
        print("--" * 20, "performance")
        print(f"\tprecision_score: {np.round(precision, 2)}")
        print(f"\trecall_score: {np.round(recall, 2)}")
        print(f"\tmedian(probabilities): {np.round(np.median(self.probabilities), 2)}")
        print(f"\tstd(probabilities): {np.round(np.std(self.probabilities), 2)}")


# --------------------------------------------------
# set up MLflow
# --------------------------------------------------


EXPERIMENT_NAME = "dpe_tertiaire"

# mlflow.set_tracking_uri("http://host.docker.internal:5001")
# mlflow.set_tracking_uri("http://localhost:9090")

mlflow.set_tracking_uri("http://mlflow:5000")


print("--" * 40)
print("mlflow set experiment")
print("--" * 40)
mlflow.set_experiment(EXPERIMENT_NAME)

mlflow.sklearn.autolog()

# --------------------------------------------------
# load data
# --------------------------------------------------


def load_data_for_inference(n_samples):
    """Load data for inference"""
    db = Database()
    query = f"select * from dpe_training order by created_at desc limit {n_samples}"
    df = pd.read_sql(query, con=db.engine)
    db.close()
    # dump payload into new dataframe
    df["payload"] = df["payload"].apply(json.loads)
    data = pd.DataFrame(list(df.payload.values))

    data.drop(columns="n_dpe", inplace=True)
    data = data.astype(int)
    data = data[data.etiquette_dpe > 0].copy()
    data.reset_index(inplace=True, drop=True)
    print(data.shape)
    y = data["etiquette_dpe"]
    feature = data[FeatureSets.train_columns]

    return feature, y


def load_data_for_training(n_samples):
    """Load data for training"""
    # simply load payload not all columns
    db = Database()
    query = f"select * from dpe_training order by random() limit {n_samples}"
    df = pd.read_sql(query, con=db.engine)
    db.close()
    # dump payload into new dataframe
    df["payload"] = df["payload"].apply(json.loads)
    data = pd.DataFrame(list(df.payload.values))
    data.drop(columns="n_dpe", inplace=True)
    data = data.astype(int)
    data = data[data.etiquette_dpe > 0].copy()
    data.reset_index(inplace=True, drop=True)
    return data


# ---------------------------------------------
#  tasks
# ---------------------------------------------
CHALLENGER_MODEL_NAME = "dpe_challenger"
CHAMPION_MODEL_NAME = "dpe_champion"
client = MlflowClient()


def train_model():
    """Train"""
    data = load_data_for_training(n_samples=2000)
    with mlflow.start_run() as run:
        train = TrainDPE(data)
        train.main()
        train.report(precision=0.0, recall=0.0)
        try:
            client.get_registered_model(CHALLENGER_MODEL_NAME)
        except ImportError as e:
            print("model does not exist", str(e))
            print("registering new model", CHALLENGER_MODEL_NAME)
            client.create_registered_model(
                CHALLENGER_MODEL_NAME, description="sklearn random forest for dpe_tertiaire"
            )

        # set version and stage
        run_id = run.info.run_id
        model_uri = f"runs:/{run_id}/model"
        model_version = client.create_model_version(
            name=CHALLENGER_MODEL_NAME, source=model_uri, run_id=run_id
        )

        client.transition_model_version_stage(
            name=CHALLENGER_MODEL_NAME, version=model_version.version, stage="Staging"
        )


def create_champion():
    """
    if there is not champion yet, creates a champion from current challenger
    """
    results = client.search_registered_models(filter_string=f"name='{CHAMPION_MODEL_NAME}'")
    # if not exists: promote current model
    if len(results) == 0:
        print("champion model not found, promoting challenger to champion")

        champion_model = client.copy_model_version(
            src_model_uri=f"models:/{CHALLENGER_MODEL_NAME}/Staging",
            dst_name=CHAMPION_MODEL_NAME,
        )
        client.transition_model_version_stage(
            name=CHAMPION_MODEL_NAME, version=champion_model.version, stage="Staging"
        )

        # reload champion and print info
        results = client.search_registered_models(filter_string=f"name='{CHAMPION_MODEL_NAME}'")
        print(results[0].latest_versions)


def promote_model():
    """Promote model"""
    feature, y = load_data_for_inference(1000)
    # inference challenger and champion
    # load model & inference
    chl = mlflow.sklearn.load_model(f"models:/{CHALLENGER_MODEL_NAME}/Staging")
    yhat = chl.best_estimator_.predict(feature)
    challenger_precision = precision_score(y, yhat, average="weighted")
    challenger_recall = recall_score(y, yhat, average="weighted")
    print(f"\t challenger_precision: {np.round(challenger_precision, 2)}")
    print(f"\t challenger_recall: {np.round(challenger_recall, 2)}")

    # inference on production model
    champ = mlflow.sklearn.load_model(f"models:/{CHAMPION_MODEL_NAME}/Staging")
    yhat = champ.best_estimator_.predict(feature)
    champion_precision = precision_score(y, yhat, average="weighted")
    champion_recall = recall_score(y, yhat, average="weighted")
    print(f"\t champion_precision: {np.round(champion_precision, 2)}")
    print(f"\t champion_recall: {np.round(champion_recall, 2)}")

    # if performance 5% above current champion: promote
    if challenger_precision > champion_precision:
        print(f"{challenger_precision} > {champion_precision}")
        print("Promoting new model to champion ")
        champion_model = client.copy_model_version(
            src_model_uri=f"models:/{CHALLENGER_MODEL_NAME}/Staging",
            dst_name=CHAMPION_MODEL_NAME,
        )

        client.transition_model_version_stage(
            name=CHAMPION_MODEL_NAME, version=champion_model.version, stage="Staging"
        )
    else:
        print(f"{challenger_precision} < {champion_precision}")
        print("champion remains undefeated ")


# ---------------------------------------------
#  DAG
# ---------------------------------------------
with DAG(
    "ademe_models",
    default_args={
        "retries": 0,
        "retry_delay": timedelta(minutes=10),
    },
    description="Model training and promotion",
    schedule="*/15 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ademe"],
) as dag:
    train_model_task = PythonOperator(task_id="train_model_task", python_callable=train_model)

    create_champion_task = PythonOperator(
        task_id="create_champion_task", python_callable=create_champion
    )

    promote_model_task = PythonOperator(task_id="promote_model_task", python_callable=promote_model)

    print(train_model_task >> create_champion_task >> promote_model_task) # pylint: disable=pointless-statement