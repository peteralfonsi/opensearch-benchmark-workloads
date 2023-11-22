import random
import json
from .query_value_providers import fn_names, fn_value_generators
import os


async def delete_snapshot(opensearch, params):
    await opensearch.snapshot.delete(repository=params["repository"], snapshot=params["snapshot"])

# Global objects below

standard_fn_values = {} # keeps a list of the standard values for each fn, for use in repeated queries
fn_name_counters = {} # keeps track of how many times we have pulled from the standard values, for each fn

for fn_name in fn_names: 
    fn_name_counters[fn_name] = 0
    try:
        fp = os.path.dirname(os.path.realpath(__file__)) + "/" + "standard_values/{}_values.json".format(fn_name)
        with open(fp, "r") as f: 
            standard_fn_values[fn_name] = json.load(f)
    except FileNotFoundError: 
        raise Exception("Must generate standard values for {} using generate_standard_random_values.py!".format(fn_name))

# A helper function used by all param sources to decide whether to create new values or pull from standard values
def get_values(repeat_freq, fn_name): 
    if random.random() < repeat_freq: 
        upper_bound = min(max(1, fn_name_counters[fn_name]), len(standard_fn_values[fn_name]))
        index = random.randrange(0, upper_bound)
        fn_name_counters[fn_name] += 1
        return standard_fn_values[fn_name][index]
    return fn_value_generators[fn_name]()

def get_basic_range_query(field, gte, lte): 
    return {
        "body": {
            "size": 0,
            "query": {
                "range": {
                    "passenger_count": {
                        "gte": gte,
                        "lte": lte
                    }
                }
            }
        },
        "index": 'nyc_taxis',
        "request-cache": True
    }

# Each specific query has a wrapper, which is actually used a param source, and a function that provides values, 
# which lives in query_value_providers.py. 
# This provider fn is also used to generate standard random values in the first place. 

def cheap_passenger_count(workload, params, **kwargs): 
    repeat_freq = params["repeat_freq"]
    val_dict = get_values(repeat_freq, "cheap_passenger_count")
    # based on random_passenger_count from https://github.com/kiranprakash154/opensearch-benchmark-workloads/blob/kp/custom-workload/nyc_taxis/workload.py
    return get_basic_range_query("passenger_count", val_dict["gte"], val_dict["lte"])

def cheap_passenger_count_no_cache(workload, params, **kwargs): 
    query = cheap_passenger_count(workload, params, **kwargs)
    query["request-cache"] = False
    return query

def cheap_tip_amount(workload, params, **kwargs): 
    # based on Kiran's random_tip_amount
    val_dict = get_values(params["repeat_freq"], "cheap_tip_amount")
    return get_basic_range_query("tip_amount", val_dict["gte"], val_dict["lte"])

def cheap_tip_amount_no_cache(workload, params, **kwargs): 
    query = cheap_tip_amount(workload, params, **kwargs)
    query["request-cache"] = False
    return query



def register(registry):
    registry.register_param_source("cheap-passenger-count-param-source", cheap_passenger_count)
    registry.register_param_source("cheap-passenger-count-no-cache-param-source", cheap_passenger_count_no_cache)
    registry.register_param_source("cheap-tip-amount-param-source", cheap_tip_amount)
    registry.register_param_source("cheap-tip-amount-no-cache-param-source", cheap_tip_amount_no_cache)
    registry.register_runner("delete-snapshot", delete_snapshot, async_runner=True)