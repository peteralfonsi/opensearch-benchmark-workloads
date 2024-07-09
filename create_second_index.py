from opensearchpy import OpenSearch
from opensearchpy.helpers import bulk
import random
from string import ascii_uppercase
import datetime

def get_client(): 
    client = client = OpenSearch(
        hosts = [{'host': "localhost", 'port': 9200}],
        http_compress = True, # enables gzip compression for request bodies
        http_auth = ("admin", "admin"),
        use_ssl = False,
        verify_certs = False,
        ssl_assert_hostname = False,
        ssl_show_warn = False
    )   
    return client

def get_time(): 
    beginning = datetime.datetime(2015, 1, 1)
    end = datetime.datetime(2015, 1, 15)
    min_timestamp = datetime.datetime.timestamp(beginning)
    max_timestamp = datetime.datetime.timestamp(end)
    timestamp = min_timestamp + random.random() * (max_timestamp - min_timestamp)
    res = datetime.datetime.fromtimestamp(timestamp)
    return res.strftime("%Y-%m-%d %H:%M:%S")

def create_index(client, name): 
    body = {
        "mappings": {
            "properties": {
            "time":    { "type" : "date", "format":"yyyy-MM-dd HH:mm:ss" },
            "value": {"type": "integer"}, 
            "string": {"type": "text"} 
            }
        }
    }
    return client.indices.create(name, body=body)

max_value = 100_000
def get_random_value(): 
    return random.randint(0, max_value)

def get_random_string(): 
    length = 8
    return "".join([random.choice(ascii_uppercase) for i in range(length)])

def get_new_doc(): 
    return { 
            "time":get_time(),
            "value":get_random_value(),
            "string":get_random_string()
        }

def populate_index(client, index_name, num_docs): 
    chunk_size = 10_000
    bulk_data = []
    for i in range(num_docs): 
        doc = get_new_doc()
        bulk_data.append({"_index":index_name, "_source":doc})
        if i % chunk_size == chunk_size - 1: 
            bulk(client, bulk_data, refresh="true")
            print("Done indexing doc {}/{}".format(i, num_docs))
            bulk_data = []

index_name = "second_index"
num_docs = 1_000_000

if __name__ == "__main__":
    client = get_client() 
    print("Creating index...")
    create_index(client, index_name)
    print("Populating index...")
    populate_index(client, index_name, num_docs)
    print("Done")