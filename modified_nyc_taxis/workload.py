import random
import json
from .query_value_providers import fn_names, fn_value_generators
import os


async def delete_snapshot(opensearch, params):
    await opensearch.snapshot.delete(repository=params["repository"], snapshot=params["snapshot"])

# Global objects below

standard_fn_values = {} # keeps a list of the standard values for each fn, for use in repeated queries

for fn_name in fn_names: 
    #fn_name_counters[fn_name] = 0
    try:
        fp = os.path.dirname(os.path.realpath(__file__)) + "/" + "standard_values/{}_values.json".format(fn_name)
        with open(fp, "r") as f: 
            standard_fn_values[fn_name] = json.load(f)
    except FileNotFoundError: 
        raise Exception("Must generate standard values for {} using generate_standard_random_values.py!".format(fn_name))

# Make the list of all query types so we can keep track of counters for each
query_types = fn_names
query_type_counters = {} # keeps track of how many times we have pulled from the standard values, for each query type

for query_type in query_types: 
    query_type_counters[query_type] = 0

# A helper function used by all param sources to decide whether to create new values or pull from standard values
# fn_name_list should contain all the fn_names you want values for in this query type. 
# For all values, the decision whether to use existing value is the same, and 
# the same counter value is used for all of them. (Otherwise, queries with multiple sources would almost never be cached)
def get_values(params, fn_name_list, query_type): 
    if random.random() < params["repeat_freq"]: 
        # We should return standard (repeatable) values 
        # First, pick a value in [0, current counter value]
        i = random.randrange(0, query_type_counters[query_type] + 1)
        # Make sure this value doesn't exceed the length of the shortest list of standard function values which we are using
        shortest_standard_list = min([len(standard_fn_values[fn_name]) for fn_name in fn_name_list]) 
        i = min(i, shortest_standard_list-1)
        # If the selected value is equal to the current counter value, this is the first time we've used this value. 
        # Increment counter value accordingly so we know it's been used in the next round
        counter_buffer = 35
        if i > query_type_counters[query_type] - counter_buffer:  
            query_type_counters[query_type] += 1
        # Return the i-th standard value pair for all the functions in our list
        return [standard_fn_values[fn_name][i] for fn_name in fn_name_list]
    # Otherwise, return a new completely random value
    return [fn_value_generators[fn_name]() for fn_name in fn_name_list]

def get_basic_range_query(field, gte, lte): 
    return {
        "body": {
            "size": 0,
            "query": {
                "range": {
                    field: {
                        "gte": gte,
                        "lte": lte
                    }
                }
            }
        },
        "index": 'nyc_taxis'
    }

def get_autohisto_agg(gte, lte): 
    return {
      "body": {
        "size": 0,
        "query": {
          "range": {
            "dropoff_datetime": {
              "gte": gte,
              "lte": lte
            }
          }
        },
        "aggs": {
          "dropoffs_over_time": {
            "auto_date_histogram": {
              "field": "dropoff_datetime",
              "buckets": 20
            }
          }
        }
      },
      "index":"nyc_taxis"
    }

# Each specific query has a wrapper, which is actually used a param source, and a function that provides values, 
# which lives in query_value_providers.py. 
# This provider fn is also used to generate standard random values in the first place. 


def ps_1d(workload, params, **kwargs): 
    val_dict = get_values(params, ["ps_1d"], "ps_1d")[0]
    return get_autohisto_agg(val_dict["gte"], val_dict["lte"])

def ps_2d(workload, params, **kwargs): 
    val_dict = get_values(params, ["ps_2d"], "ps_2d")[0]
    return get_autohisto_agg(val_dict["gte"], val_dict["lte"])

def ps_4d(workload, params, **kwargs): 
    val_dict = get_values(params, ["ps_4d"], "ps_4d")[0]
    return get_autohisto_agg(val_dict["gte"], val_dict["lte"])

def ps_1w(workload, params, **kwargs): 
    val_dict = get_values(params, ["ps_1w"], "ps_1w")[0]
    return get_autohisto_agg(val_dict["gte"], val_dict["lte"])

def ps_2w(workload, params, **kwargs): 
    val_dict = get_values(params, ["ps_2w"], "ps_2w")[0]
    return get_autohisto_agg(val_dict["gte"], val_dict["lte"])

def ps_3w(workload, params, **kwargs): 
    val_dict = get_values(params, ["ps_3w"], "ps_3w")[0]
    return get_autohisto_agg(val_dict["gte"], val_dict["lte"])

def ps_1m(workload, params, **kwargs): 
    val_dict = get_values(params, ["ps_1m"], "ps_1m")[0]
    return get_autohisto_agg(val_dict["gte"], val_dict["lte"])

def ps_6w(workload, params, **kwargs): 
    val_dict = get_values(params, ["ps_6w"], "ps_6w")[0]
    return get_autohisto_agg(val_dict["gte"], val_dict["lte"])

def ps_2m(workload, params, **kwargs): 
    val_dict = get_values(params, ["ps_2m"], "ps_2m")[0]
    return get_autohisto_agg(val_dict["gte"], val_dict["lte"])


def register(registry):
    registry.register_param_source("1d-param-source", ps_1d)
    registry.register_param_source("2d-param-source", ps_2d)
    registry.register_param_source("4d-param-source", ps_4d)
    registry.register_param_source("1w-param-source", ps_1w)
    registry.register_param_source("2w-param-source", ps_2w)
    registry.register_param_source("3w-param-source", ps_2w)
    registry.register_param_source("1m-param-source", ps_1m)
    registry.register_param_source("6w-param-source", ps_6w)
    registry.register_param_source("2m-param-source", ps_2m)
    registry.register_runner("delete-snapshot", delete_snapshot, async_runner=True)