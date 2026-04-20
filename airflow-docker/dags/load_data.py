import pandas as pd 
from sqlalchemy import create_engine

engine = create_engine("postgresql://airflow:airflow@postgres:5432/airflow")

customers = pd.read_sql("SELECT * FROM customers", engine)
orders = pd.read_sql("SELECT * FROM orders", engine)

# Insert sql data into database
customers.to_sql("customers", engine, if_exists="append", index=False)
orders.to_sql("orders", engine, if_exists="append", index=False)

print("Data loaded successfully!")