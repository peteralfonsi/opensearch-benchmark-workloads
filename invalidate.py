from opensearchpy import OpenSearch
import time
import random 
import datetime

client = OpenSearch(
    hosts = [{'host': "localhost", 'port': 9200}],
    http_compress = True, # enables gzip compression for request bodies
    http_auth = ("admin", "admin"),
    use_ssl = False,
    verify_certs = False,
    ssl_assert_hostname = False,
    ssl_show_warn = False
)

def get_time(): 
    beginning = datetime.datetime(2015, 1, 1)
    end = datetime.datetime(2015, 1, 15)
    min_timestamp = datetime.datetime.timestamp(beginning)
    max_timestamp = datetime.datetime.timestamp(end)
    timestamp = min_timestamp + random.random() * (max_timestamp - min_timestamp)
    res = datetime.datetime.fromtimestamp(timestamp)
    return res.strftime("%Y-%m-%d %H:%M:%S")

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
    client.index(index="nyc_taxis", body=new_doc, refresh=True)
    print("Document indexed!")

interval = 120 
print("Indexing new document every {} seconds".format(interval))
while True: 
    create_new_doc(client)
    time.sleep(interval)