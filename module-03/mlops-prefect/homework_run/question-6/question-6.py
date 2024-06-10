import pandas as pd
from sklearn.feature_extraction import DictVectorizer
from sklearn.linear_model import LinearRegression
from prefect import Flow, task
import mlflow
import mlflow.sklearn

# Initialize MLFlow tracking
mlflow.set_tracking_uri("http://localhost:5000")  # Change the URI as per your MLFlow setup

@task
def read_data(filename: str) -> pd.DataFrame:
    """Read data into DataFrame"""
    df = pd.read_parquet(filename)

    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)

    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df.duration = df.duration.apply(lambda td: td.total_seconds() / 60)

    df = df[(df.duration >= 1) & (df.duration <= 60)]

    categorical = ["PULocationID", "DOLocationID"]
    df[categorical] = df[categorical].astype(str)

    return df

@task
def train_model(df: pd.DataFrame, feature_column: str) -> (LinearRegression, float):
    # Extract the feature column
    features = df[feature_column].astype(str)

    # Convert to list of dictionaries
    feature_dicts = features.apply(lambda x: {'location': x}).tolist()

    # Fit a DictVectorizer
    vec = DictVectorizer(sparse=True)  # Use sparse matrix
    X = vec.fit_transform(feature_dicts)

    # Train a linear regression model
    model = LinearRegression()
    model.fit(X, df['total_amount'])

    # Log the model with MLFlow
    with mlflow.start_run():
        mlflow.sklearn.log_model(model, "model")
        mlflow.log_param("intercept", model.intercept_)

    return model, model.intercept_

file_path = r'D:\github\mlops-zoomcamp-course\module-03\mlops-prefect\homework_run\data\yellow_tripdata_2023-03.parquet'

with Flow("train-linear-regression") as flow:
    data = read_data(file_path)
    model, _ = train_model(data, 'PULocationID')

# Run the flow
flow.run()
