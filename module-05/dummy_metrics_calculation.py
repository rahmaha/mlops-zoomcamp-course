import datetime
import time
import random
import logging 
import uuid
import pytz
import pandas as pd
import io
import psycopg

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s")

SEND_TIMEOUT = 10
rand = random.Random()

create_table_statement = """
drop table if exists dummy_metrics;
create table dummy_metrics(
	timestamp timestamp,
	value1 integer,
	value2 varchar,
	value3 float
)
"""

def prep_db():
	with psycopg.connect("host=localhost port=5432 user=postgres password=example", autocommit=True) as conn:
		res = conn.execute("SELECT 1 FROM pg_database WHERE datname='test'")
		if len(res.fetchall()) == 0:
			conn.execute("create database test;")
		with psycopg.connect("host=localhost port=5432 dbname=test user=postgres password=example") as conn:
			conn.execute(create_table_statement)

def calculate_dummy_metrics_postgresql(curr):
	value1 = rand.randint(0, 1000)
	value2 = str(uuid.uuid4())
	value3 = rand.random()

	curr.execute(
		"insert into dummy_metrics(timestamp, value1, value2, value3) values (%s, %s, %s, %s)",
		(datetime.datetime.now(pytz.timezone('Europe/London')), value1, value2, value3)
	)

def main():
    prep_db()  # Prepare the database and table
    last_send = datetime.datetime.now() - datetime.timedelta(seconds=10)  # Set last_send to 10 seconds before now
    with psycopg.connect("host=localhost port=5432 dbname=test user=postgres password=example", autocommit=True) as conn:
        for i in range(0, 100):
            with conn.cursor() as curr:
                calculate_dummy_metrics_postgresql(curr)  # Insert a row of dummy data

            new_send = datetime.datetime.now()  # Get the current time
            seconds_elapsed = (new_send - last_send).total_seconds()  # Calculate time elapsed since last_send
            if seconds_elapsed < SEND_TIMEOUT:
                time.sleep(SEND_TIMEOUT - seconds_elapsed)  # Sleep if less than 10 seconds have passed

            while last_send < new_send:
                last_send = last_send + datetime.timedelta(seconds=10)  # Update last_send to be exactly 10 seconds ahead

            logging.info("data sent")  # Log data sent message


if __name__ == '__main__':
	main()