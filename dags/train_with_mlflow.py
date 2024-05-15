"""
Training the model
"""
import json
import pandas as pd
import numpy as np

pd.options.display.max_columns = 100
pd.options.display.max_rows = 60
pd.options.display.max_colwidth = 100
pd.options.display.precision = 10
pd.options.display.width = 160

from sklearn.model_selection import train_test_split, GridSearchCV, KFold
from sklearn.metrics import precision_score, recall_score
from sklearn.ensemble import RandomForestClassifier
from db_utils import Database
import time
import random
import mlflow
from mlflow import MlflowClient

# local = True
# if local:
remote_server_uri = "http://13.82.178.239:5001/"
experiment_name = "dpe_logement"

# else:
#     # Configure MLflow to communicate with a Databricks-hosted tracking server
#     experiment_name = "/Users/alexis.perrier@skatai.com/dpe_tertiaire"
#     remote_server_uri = "databricks"

mlflow.set_tracking_uri(remote_server_uri)
mlflow.set_experiment(experiment_name)

mlflow.sklearn.autolog()

# load data

def load_data_inference(n_samples):
    db = Database()
    query = f"""
select * from logement
order by created_at desc
limit {n_samples}
"""
    df = pd.read_sql(query, con=db.engine)
    db.close()
    # dump payload into new dataframe
    df["payload"] = df["payload"].apply(lambda d: json.loads(d))

    data = pd.DataFrame(list(df.payload.values))
    data.drop(columns="n_dpe", inplace=True)
    data = data.astype(int)
    data = data[data.etiquette_dpe >0].copy()
    data.reset_index(inplace = True, drop = True)
    return data

def load_data(n_samples):
    db = Database()
    query = f"""
select * from logement
order by random()
limit {n_samples}
"""
    df = pd.read_sql(query, con=db.engine)
    db.close()
    # dump payload into new dataframe
    df["payload"] = df["payload"].apply(lambda d: json.loads(d))

    data = pd.DataFrame(list(df.payload.values))
    data.drop(columns="n_dpe", inplace=True)
    data = data.astype(int)
    data = data[data.etiquette_dpe >0].copy()
    data.reset_index(inplace = True, drop = True)
    return data


class NotEnoughSamples(ValueError):
    pass


class TrainDPE:
    param_grid = {
        "n_estimators": sorted([random.randint(1, 20)*10 for _ in range(2)]),
        "max_depth": [random.randint(3, 10)],
        "min_samples_leaf": [random.randint(2, 5)],
    }
    n_splits = 3
    test_size = 0.3
    minimum_training_samples = 500

    def __init__(self, data, target="etiquette_dpe"):
        # drop samples with no target
        data = data[data[target] >= 0].copy()
        data.reset_index(inplace=True, drop=True)
        if data.shape[0] < TrainDPE.minimum_training_samples:
            raise NotEnoughSamples(
                f"data has {data.shape[0]} samples, which is not enough to train a model. min required {TrainDPE.minimum_training_samples}"
            )

        self.data = data
        print(f"training on {data.shape[0]} samples")

        self.model = RandomForestClassifier()
        self.target = target
        self.params = {}
        self.train_score = 0.0

        self.precision_score = 0.0
        self.recall_score =  0.0
        self.probabilities =  [0.0, 0.0]


    def main(self):
        # shuffle

        X = self.data[FeatureSets.train_columns].copy()  # Features
        y = self.data[self.target].copy()  # Target variable

        # Split the data into training and testing sets
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=TrainDPE.test_size, random_state=808
        )

        # Setup GridSearchCV with k-fold cross-validation
        cv = KFold(n_splits=TrainDPE.n_splits, random_state=42, shuffle=True)

        grid_search = GridSearchCV(
            estimator=self.model, param_grid=TrainDPE.param_grid, cv=cv, scoring="accuracy"
        )

        # Fit the model
        grid_search.fit(X_train, y_train)

        self.model = grid_search.best_estimator_
        self.params = grid_search.best_params_
        self.train_score = grid_search.best_score_

        yhat = grid_search.predict(X_test)
        self.precision_score = precision_score(y_test, yhat, average="weighted")
        self.recall_score = recall_score(y_test, yhat, average="weighted")
        self.probabilities = np.max(grid_search.predict_proba(X_test), axis=1)

    def report(self):
        # Best parameters and best score
        print("--"*20, "Best model")
        print(f"\tparameters: {self.params}")
        print(f"\tcross-validation score: {self.train_score}")
        print(f"\tmodel: {self.model}")
        print("--"*20, "performance")
        print(f"\tprecision_score: {np.round(self.precision_score, 2)}")
        print(f"\trecall_score: {np.round(self.recall_score, 2)}")
        print(f"\tmedian(probabilities): {np.round(np.median(self.probabilities), 2)}")
        print(f"\tstd(probabilities): {np.round(np.std(self.probabilities), 2)}")



if __name__ == "__main__":
    ctime = int(time.time())
    data = load_data(n_samples = 2000)
    with mlflow.start_run() as run:
        # Your training code here

        # Access the run_id

        train = TrainDPE(data)
        train.main()
        train.report()



