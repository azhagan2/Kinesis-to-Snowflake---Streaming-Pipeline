

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
from snowflake.snowpark import Session
import pandas as pd
import random

def load_dim_tables_to_snowflake(session):

    # 2. Generate synthetic dim_store data
    dim_store = [
        {
            "store_id": str(295 + i),
            "store_name": f"Store_{295 + i}",
            "city": random.choice(["New York", "Los Angeles", "Chicago", "Houston", "Miami"]),
            "region": random.choice(["East", "West", "Midwest", "South"]),
            "country": "USA",
            "is_active": random.choice([True, True, False])
        }
        for i in range(10)
    ]
    df_store = pd.DataFrame(dim_store)

    # 3. Generate improved synthetic dim_product data
    categories = {
        "Electronics": ["Phones", "Laptops", "Tablets", "Accessories"],
        "Home": ["Furniture", "Appliances", "Decor"],
        "Books": ["Fiction", "Non-fiction", "Comics", "Educational"]
    }
    brands = ["Sony", "Nike", "Apple", "Samsung", "Adidas", "Ikea", "Hasbro", "Penguin"]

    fixed_prices = {
        f"P{str(i).zfill(3)}": price
        for i, price in enumerate(
            [49.99, 59.99, 69.99, 79.99, 89.99, 99.99, 109.99, 119.99, 129.99], start=1
        )
            }

    dim_product = []
    for i in range(1, 10):
        product_id = f"P{str(i).zfill(3)}"
        category = random.choice(list(categories.keys()))
        sub_category = random.choice(categories[category])
        brand = random.choice(brands)
        price = fixed_prices.get(product_id, 99.99)  # fallback price if not found

        dim_product.append({
            "product_id": product_id,
            "product_name": f"{brand} {sub_category} {product_id}",
            "category": category,
            "sub_category": sub_category,
            "brand": brand,
            "price": price
        })
    df_product = pd.DataFrame(dim_product)

    # 4. Load into Snowflake using Snowpark
    df_store.columns = [col.upper() for col in df_store.columns]
    df_product.columns = [col.upper() for col in df_product.columns]
    session.create_dataframe(df_store).write.mode("overwrite").save_as_table("retail_demo_db.silver.dim_store")
    session.create_dataframe(df_product).write.mode("overwrite").save_as_table("retail_demo_db.silver.dim_product")

    print("dim_store and dim_product successfully loaded.")

    # Optional: Close session
    session.close()
    return session.create_dataframe(df_store)


connection_parameters = {
    "account": "DKV-DR26",
    "user": "user_id_here", # insert user id here
    "password": "############", # insert password here
    "role": "accountadmin",
    "warehouse": "COMPUTE_WH",
    "database": "retail_demo_db",
    "schema": "silver"
}
def main(session: snowpark.Session): 
    dataframe=load_dim_tables_to_snowflake(session)
    return dataframe


