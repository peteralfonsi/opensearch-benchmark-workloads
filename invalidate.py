from opensearchpy import OpenSearch
import time
import random 
from create_second_index import get_new_doc, get_time, get_client, index_name

def create_new_doc(client): 
    new_doc = {
        "tip_amount": random.randint(0, 11), 
        "dropoff_datetime": get_time(), 
        "rate_code_id": "1", 
        "extra": 0.0, 
        "tolls_amount": random.randint(0, 11), 
        "mta_tax": 0.5, 
        "pickup_location": [-73.95059204101562, 40.68623352050781], 
        "improvement_surcharge": 0.3, 
        "trip_distance": random.randint(0, 30), 
        "dropoff_location": [-73.9159164428711, 40.69511413574219], 
        "trip_type": "1", 
        "pickup_datetime": get_time(), 
        "passenger_count": random.randint(1, 5), 
        "store_and_fwd_flag": "N", 
        "fare_amount": random.randint(0, 11), 
        "vendor_id": "2", 
        "payment_type": "2", 
        "total_amount": random.randint(0, 11)
        }
    client.index(index="nyc_taxis", body=new_doc, refresh=False)

interval = 60 
client = get_client()
print("Indexing new document every {} seconds".format(interval))
i = 0
while True: 
    if i % 2 == 1: 
        print("Indexing nyc_taxis doc")
        create_new_doc(client)
        print("done")
    else: 
        print("Indexing {} doc".format(index_name))
        client.index(index=index_name, body=get_new_doc(), refresh=False)
        print("done")
    i += 1
    time.sleep(interval)