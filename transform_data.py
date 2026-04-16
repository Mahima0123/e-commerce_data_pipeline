import pandas as pd 
from sqlalchemy import create_engine

engine = create_engine("postgresql://postgres@localhost:5432/ecommerce_db")

# read data from database
orders = pd.read_sql("SELECT * FROM orders", engine)

# select other data except cancelled orders
orders_cleaned = orders[orders["status"] != "cancelled"].copy()

# standardize to lowercase
orders_cleaned.loc[:, "status"] = orders_cleaned["status"].str.lower()

# add new column(amount > 300)
orders_cleaned.loc[:, "high_value"] = orders_cleaned["amount"] > 300