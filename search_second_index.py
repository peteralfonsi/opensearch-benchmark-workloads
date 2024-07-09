from opensearchpy import OpenSearch
from create_second_index import get_random_value, get_random_string, max_value, index_name, get_time, get_client
import random
import time

def get_time_query(): 
    return {
            "query": {
                "range": {
                    "time": {
                        "gte": get_time(),
                        "lt": get_time()
                    }
                }
            },
            "size": 0
        }

def get_value_query(): 
    gt = get_random_value()
    lt = min(max_value, get_random_value())
    return {
            "query": {
                "range": {
                    "value": {
                        "gte": gt,
                        "lt": lt
                    }
                }
            },
            "size": 0
        }

def get_string_query(): 
    return { 
        "query": { 
            "match": { 
                "string": get_random_string()
            }
        }
    }

def get_query(): 
    query_fn = random.choice([get_time_query, get_value_query, get_string_query]) 
    return query_fn()

def search_index(client): 
    query = get_query()
    response = client.search(
        body = query,
        index = index_name,
        params={"request_cache":"true"}
    )

initial_delay = 3 * 60 * 60 # three hours approximately matches nyc taxis indexing time 
delay = 0.2
client = get_client() 
print("Waiting for {} seconds".format(initial_delay))
time.sleep(initial_delay) 
print("Beginning searches with {} sec delay between each".format(delay))
while True: 
    search_index(client) 
    time.sleep(delay)
