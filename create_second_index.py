from opensearchpy import OpenSearch
from opensearchpy.helpers import bulk
from invalidate import get_time, get_client
import random
from string import ascii_uppercase

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
    return random.randint(max_value)

def get_random_string(length): 
    return "".join([random.choice(ascii_uppercase) for i in range(length)])

def get_new_doc(): 
    return { 
            "time":get_time(),
            "value":get_random_value(),
            "string":get_random_string(8)
        }

def populate_index(client, index_name, num_docs): 
    bulk_data = []
    for i in range(num_docs): 
        doc = get_new_doc()
        bulk_data.append({"_index":index_name, "_source":doc})
    bulk(client, bulk_data)

index_name = "second_index"
num_docs = 1_000_000
client = get_client() 
print("Creating index...")
create_index(client, index_name)
print("Populating index...")
populate_index(client, index_name, num_docs)
print("Done")