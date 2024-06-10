import pandas as pd
from sklearn.feature_extraction import DictVectorizer
from sklearn.linear_model import LinearRegression
from prefect import flow, task

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
def train_model(df: pd.DataFrame, feature_column: str) -> float:
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

    # Print intercept of the model
    print("Intercept of the model:", model.intercept_)
    return model.intercept_

@flow
def main_flow():
    file_path = r'D:\github\mlops-zoomcamp-course\module-03\mlops-prefect\homework_run\data\yellow_tripdata_2023-03.parquet'

    # Read data
    data = read_data(file_path)

    # Train the model using pick up location
    intercept_pickup = train_model(data, 'PULocationID')

    # Train the model using drop off location
    intercept_dropoff = train_model(data, 'DOLocationID')

if __name__ == "__main__":
    main_flow()
