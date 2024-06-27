import datetime
import time
import random
import logging 
import uuid
import pytz
import pandas as pd
import io
import psycopg
import joblib

from prefect import task, flow

from evidently.report import Report
from evidently import ColumnMapping
from evidently.metrics import ColumnDriftMetric, ColumnQuantileMetric

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s")

SEND_TIMEOUT = 10
rand = random.Random()

create_table_statement = """
drop table if exists hw-q4;
create table hw-q4(
	timestamp timestamp,
	quantile float,
	prediction_drift float
)
"""

reference_data = pd.read_parquet('data/reference.parquet')
with open('models/hw-q4.bin', 'rb') as f_in:
	model = joblib.load(f_in)

raw_data = pd.read_parquet('data/green_tripdata_2024-03.parquet')

begin = datetime.datetime(2024, 3, 1, 0, 0)
num_features = ['passenger_count', 'trip_distance', 'fare_amount', 'total_amount']
cat_features = ['PULocationID', 'DOLocationID']
column_mapping = ColumnMapping(
    prediction='fare_amount',
    numerical_features=num_features,
    categorical_features=cat_features,
    target=None
)

#evidently report
report = Report(metrics=[
    ColumnQuantileMetric(column_name="fare_amount", quantile=0.5),  # Median
    ColumnDriftMetric(column_name="fare_amount")  # Drift detection
])

@task
def prep_db():
	with psycopg.connect("host=localhost port=5432 user=postgres password=example", autocommit=True) as conn:
		res = conn.execute("SELECT 1 FROM pg_database WHERE datname='hw-q4'")
		if len(res.fetchall()) == 0:
			conn.execute("create database hw-q4;")
		with psycopg.connect("host=localhost port=5432 dbname=hw-q4 user=postgres password=example") as conn:
			conn.execute(create_table_statement)

@task
def calculate_metrics_postgresql(curr, i):
	current_data = raw_data[(raw_data.lpep_pickup_datetime >= (begin + datetime.timedelta(i))) &
		(raw_data.lpep_pickup_datetime < (begin + datetime.timedelta(i + 1)))]

	#current_data.fillna(0, inplace=True)
	current_data['prediction'] = model.predict(current_data[num_features + cat_features].fillna(0))

	report.run(reference_data = reference_data, current_data = current_data,
		column_mapping=column_mapping)

	result = report.as_dict()
	quantile = result['metrics'][0]['result']['current']['value']
	prediction_drift = result['metrics'][1]['result']['drift_score']

	curr.execute(
		"insert into dummy_metrics(timestamp, quantile, prediction_drift) values (%s, %s, %s)",
		(begin + datetime.timedelta(i), quantile, prediction_drift)
	)

@flow
def batch_monitoring_backfill():
	prep_db()
	last_send = datetime.datetime.now() - datetime.timedelta(seconds=10)
	with psycopg.connect("host=localhost port=5432 dbname=test user=postgres password=example", autocommit=True) as conn:
		for i in range(0, 27):
			with conn.cursor() as curr:
				calculate_metrics_postgresql(curr, i)

			new_send = datetime.datetime.now()
			seconds_elapsed = (new_send - last_send).total_seconds()
			if seconds_elapsed < SEND_TIMEOUT:
				time.sleep(SEND_TIMEOUT - seconds_elapsed)
			while last_send < new_send:
				last_send = last_send + datetime.timedelta(seconds=10)
			logging.info("data sent")

if __name__ == '__main__':
	batch_monitoring_backfill()
