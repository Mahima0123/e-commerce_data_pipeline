from faker import Faker
import pandas as pd 
import random

fake = Faker()

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

# save to CSV
customers_df.to_csv("customers.csv", index=False)
orders_df.to_csv("orders.csv", index=False)