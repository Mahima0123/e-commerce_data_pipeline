from faker import Faker
import pandas as pd 
import random
from sqlalchemy import create_engine

fake = Faker()
engine = create_engine("postgresql://airflow:airflow@postgres:5432/airflow")

# customers data
customers = []
for _ in range(100):
    customers.append({
        "customer_id": fake.uuid4(),
        "name": fake.name(),
        "email": fake.email(),
        "city": fake.city()
    })
customers_df = pd.DataFrame(customers)

# orders data
orders = []
for _ in range(200):
    orders.append({
        "order_id": fake.uuid4(),
        "customer_id": random.choice(customers_df["customer_id"].tolist()),
        "amount": round(random.uniform(10, 500), 2),
        "status": random.choice(["delivered", "pending", "cancelled"])
    })
orders_df = pd.DataFrame(orders)

# load to postgres
customers_df.to_sql("customers", engine, if_exists="replace", index=False)
orders_df.to_sql("orders", engine, if_exists="replace", index=False)

# save to CSV
customers_df.to_csv("customers.csv", index=False)
orders_df.to_csv("orders.csv", index=False)