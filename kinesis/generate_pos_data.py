import boto3
import json
import random
import time
from datetime import datetime, timezone

session = boto3.Session(profile_name="new_account")
client = session.client('kinesis', region_name='us-east-1') 

# Fixed mapping of 10 store IDs to 10 cities
store_city_mapping = {
    "295": "New York",
    "296": "Los Angeles",
    "298": "Chicago",
    "299": "Houston",
    "297": "Miami",
    "300": "San Francisco",
    "301": "Seattle",
    "302": "Boston",
    "303": "Denver",
    "304": "Atlanta"
}
products = [
    {"product_id": "P001", "price": 52.99},
    {"product_id": "P002", "price": 63.99},
    {"product_id": "P003", "price": 72.99},
    {"product_id": "P004", "price": 83.99},
    {"product_id": "P005", "price": 92.99},
    {"product_id": "P006", "price": 104.99},
    {"product_id": "P007", "price": 114.99},
    {"product_id": "P008", "price": 116.99},
    {"product_id": "P009", "price": 124.99}
]

store_ids = list(store_city_mapping.keys())



def generate_transaction():
    store_id = random.choice(store_ids)
    print(store_id,end=" \n")
    city = store_city_mapping[store_id]
    print(city,end=" \n")
    transaction_id = f"txn_{random.randint(10000, 99999)}"
    print(transaction_id,end=" \n")
    timestamp = datetime.now(timezone.utc).isoformat()
    print(timestamp,end=" \n")
    #city = random.choice(cities)
    
    # Select random number of items per transaction
    num_items = random.randint(1, 5)
    print(num_items,end=" \n")
    items = random.sample(products, num_items)
    print(items,end=" \n")
    for item in items:
        item["quantity"] = random.randint(1, 3)
        print(item,end=" \n")
    
    total_amount = sum(item["price"] * item["quantity"] for item in items)
    payment_method = random.choice(["Credit Card", "Debit Card", "Cash", "Mobile Payment"])
    
    # Randomly decide if the transaction includes a refund or a canceled item
    refund = None
    cancel_item = None
    
    if random.random() < 0.2:  # 20% chance of refund
        refund = {
            "transaction_id": transaction_id,
            "refund_amount": round(total_amount * random.uniform(0.1, 1), 2),
            "refund_method": payment_method,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    
    if random.random() < 0.1 and items:  # 10% chance of canceling an item
        cancel_item = random.choice(items)
        cancel_item["canceled"] = True
        total_amount -= cancel_item["price"] * cancel_item["quantity"]
    
    transaction = {
        "store_id": store_id,
        "city": city,
        "transaction_id": transaction_id,
        "timestamp": timestamp,
        "items": items,
        "total_amount": round(total_amount, 2),
        "payment_method": payment_method,
        "refund": refund,
        "cancel_item": cancel_item
    }
    
    return transaction

# Send transactions to Kinesis
stream_name = "demo1"

#send json data
data = generate_transaction()
response = client.put_record(
        StreamName=stream_name,
        Data=json.dumps(data),
        PartitionKey=data["store_id"]
    )

print(f"data  : {data}")