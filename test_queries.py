from opensearchpy import OpenSearch
import opensearchpy
import requests 
import datetime
import json

client = OpenSearch(
    hosts = [{'host': "localhost", 'port': 9200}],
    http_compress = True, # enables gzip compression for request bodies
    http_auth = ("admin", "admin"),
    use_ssl = False,
    verify_certs = False,
    ssl_assert_hostname = False,
    ssl_show_warn = False
)

saved_queries = "modified_nyc_taxis/saved_queries.txt"
out_fp = "modified_nyc_taxis/test_query_responses.txt"

with open(out_fp, "w") as f: 
    f.write("")

expensive_4_query = query = {
        "size": 100,
        "query": {
        "range": {
            "pickup_datetime": {
            "gte": "2015-01-01 12:45:45",
            "lte": "2015-07-07 12:01:11"
            }
        }
        },
        "aggs": {
        "vendor_id_terms": {
            "terms": {
            "field": "vendor_id",
            "size": 100
            },
            "aggs": {
            "trip_type_terms": {
                "terms": {
                "field": "trip_type",
                "size": 100
                },
                "aggs": {
                "payment_type_terms": {
                    "terms": {
                    "field": "payment_type",
                    "size": 100
                    },
                    "aggs": {
                    "avg_fare_amount": {
                        "avg": {
                        "field": "fare_amount"
                        }
                    }
                    }
                }
                }
            }
            }
        }
        }
        }

medium_query = query = {
        "size": 100,
        "query": {
        "range": {
            "pickup_datetime": {
            "gte": "2015-01-01 12:45:45",
            "lte": "2015-07-07 12:01:11"
            }
        }
        },
        "aggs": {
        "vendor_id_terms": {
            "terms": {
            "field": "vendor_id",
            "size": 10
            },
        }
        }
        }

def send_test_query(): 
    
    now = datetime.datetime.now().timestamp()
    response = client.search(
        body = medium_query,
        index = "nyc_taxis",
        request_timeout=120
    )
    elapsed = datetime.datetime.now().timestamp() - now 
    print("Time took: {}".format(elapsed))


send_test_query()
