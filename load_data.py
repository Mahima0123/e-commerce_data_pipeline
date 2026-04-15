import pandas as pd 
from sqlalchemy import create_engine

engine = create_engine("postgresql://postgres@localhost:5432/ecommerce_db")

customers = pd.read_csv("customers.csv")
orders = pd.read_csv("orders.csv")

# Insert dcsv data into database
customers.to_sql("customers", engine, if_exists="append", index=False)
orders.to_sql("orders", engine, if_exists="append", index=False)

print("Data loaded successfully!")