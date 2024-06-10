import pandas as pd
import prefect
from prefect import flow, task


@task(retries=4, retry_delay_seconds=0.1, log_prints=True)
def read_dataframe(filename):
    df = pd.read_parquet(filename)

    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)

    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df.duration = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)]

    categorical = ['PULocationID', 'DOLocationID']
    df[categorical] = df[categorical].astype(str)
    print(df.shape)

    return df

@flow
def read_flow():

    # Define the relative path to the Parquet file
    file_path = r'D:\github\mlops-zoomcamp-course\module-03\mlops-prefect\homework_run\data\yellow_tripdata_2023-03.parquet'
    read_dataframe(file_path)

if __name__ == "__main__":
    read_flow()
