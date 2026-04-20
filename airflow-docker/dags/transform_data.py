import pandas as pd 
from sqlalchemy import create_engine

engine = create_engine("postgresql://airflow:airflow@postgres:5432/airflow")

# read data from database
orders = pd.read_sql("SELECT * FROM orders", engine)

orders["status"] = orders["status"].str.lower()

# select other data except cancelled orders
orders_cleaned = orders[orders["status"] != "cancelled"].copy()

# add new column(amount > 300)
orders_cleaned.loc[:, "high_value"] = orders_cleaned["amount"] > 300

# save to database
orders_cleaned.to_sql("orders_cleaned", engine, if_exists="replace", index=False)
print("Data transformed and saved successfully!")