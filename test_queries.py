from opensearchpy import OpenSearch
import opensearchpy
import requests 

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

def send_test_query(query_source): 
    '''dummy_params = {"repeat_freq":1.0}
    query = query_source(None, dummy_params)
    '''

    query = {
        "body": {
                "size": 0,
                "query": {
                    "range": {
                        "pickup_datetime": {
                            "gte": "2015-01-01 12:45:45",
                            "lte": "2015-07-07 12:01:01"
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
            },
        "index": 'nyc_taxis',
        "request-cache": True,
        "request-timeout": 60
    }

    response = client.search(
        body = query,
        index = "nyc_taxis"
        #timeout=120
    )
    
    with open(out_fp, "a+") as f: 
        f.write("****Test query for {}****".format(query_source))
        f.write(response)
        f.write("\n\n")

'''sources = [
    workload.expensive_4, 
    workload.expensive_4_no_cache
]

for query_source in sources: 
    send_test_query(query_source)'''
