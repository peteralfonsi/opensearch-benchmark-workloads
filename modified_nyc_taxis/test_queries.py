from opensearchpy import OpenSearch
import opensearchpy
import requests 
from .workload import *
from .query_value_providers import fn_names, fn_value_generators

client = OpenSearch(
    hosts = [{'host': "localhost", 'port': 9200}],
    http_compress = True, # enables gzip compression for request bodies
    http_auth = ("admin", "admin"),
    use_ssl = False,
    verify_certs = False,
    ssl_assert_hostname = False,
    ssl_show_warn = False
)

out_fp = "test_query_responses.txt"

with open(out_fp, "w") as f: 
    f.write("")

def send_test_query(query_source): 
    dummy_params = {"repeat_freq":1.0}
    query = query_source(None, dummy_params)
    response = client.search(
        body = query,
        index = "nyc_taxis"
        #timeout=120
    )
    with open(out_fp, "a+") as f: 
        f.write("****Test query for {}****".format(query_source))
        f.write(response)
        f.write("\n\n")

sources = [
    expensive_4, 
    expensive_4_no_cache
]

for query_source in sources: 
    send_test_query(query_source)
