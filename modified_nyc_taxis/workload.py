import random
import json
#from .query_value_providers import fn_names, fn_value_generators
import query_value_providers
import os


async def delete_snapshot(opensearch, params):
    await opensearch.snapshot.delete(repository=params["repository"], snapshot=params["snapshot"])

# Global objects below

standard_fn_values = {} # keeps a list of the standard values for each fn, for use in repeated queries
fn_name_counters = {} # keeps track of how many times we have pulled from the standard values, for each fn

for fn_name in query_value_providers.fn_names: 
    fn_name_counters[fn_name] = 0
    try:
        fp = os.path.dirname(os.path.realpath(__file__)) + "/" + "standard_values/{}_values.json".format(fn_name)
        with open(fp, "r") as f: 
            standard_fn_values[fn_name] = json.load(f)
    except FileNotFoundError: 
        raise Exception("Must generate standard values for {} using generate_standard_random_values.py!".format(fn_name))

# A helper function used by all param sources to decide whether to create new values or pull from standard values
# fn_name_list should contain all the fn_names you want values for. 
# For all values, the decision whether to use existing value is the same, and 
# the same counter value is used for all of them. (Otherwise, queries with multiple sources would almost never be cached)
def get_values(params, fn_name_list): 
    if random.random() < params["repeat_freq"]: 
        # return standard (repeatable) values 
        # pick an upper bound that works for all functions used
        lowest_counter_value = None
        selected_fn = None
        for fn_name in fn_name_list: 
            if lowest_counter_value is None or fn_name_counters[fn_name] < lowest_counter_value: 
                lowest_counter_value = fn_name_counters[fn_name]
                selected_fn = fn_name 
        
        upper_bound = min(max(1, lowest_counter_value), len(standard_fn_values[selected_fn]))
        index = random.randrange(0, upper_bound)

        return [standard_fn_values[fn_name][index] for fn_name in fn_name_list]
    return [query_value_providers.fn_value_generators[fn_name]() for fn_name in fn_name_list]

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
        "index": 'nyc_taxis',
        "request-cache": True
    }

# Each specific query has a wrapper, which is actually used a param source, and a function that provides values, 
# which lives in query_value_providers.py. 
# This provider fn is also used to generate standard random values in the first place. 

def cheap_passenger_count(workload, params, **kwargs): 
    val_dict = get_values(params, ["cheap_passenger_count"])[0]
    # based on random_passenger_count from https://github.com/kiranprakash154/opensearch-benchmark-workloads/blob/kp/custom-workload/nyc_taxis/workload.py
    return get_basic_range_query("passenger_count", val_dict["gte"], val_dict["lte"])

def cheap_passenger_count_no_cache(workload, params, **kwargs): 
    query = cheap_passenger_count(workload, params, **kwargs)
    query["request-cache"] = False
    return query

def cheap_tip_amount(workload, params, **kwargs): 
    # based on Kiran's random_tip_amount
    val_dict = get_values(params, ["cheap_tip_amount"])[0]
    return get_basic_range_query("tip_amount", val_dict["gte"], val_dict["lte"])

def cheap_tip_amount_no_cache(workload, params, **kwargs): 
    query = cheap_tip_amount(workload, params, **kwargs)
    query["request-cache"] = False
    return query

def cheap_fare_amount(workload, params, **kwargs): 
    val_dict = get_values(params, ["cheap_fare_amount"])[0]
    return get_basic_range_query("fare_amount", val_dict["gte"], val_dict["lte"])

def cheap_fare_amount_no_cache(workload, params, **kwargs):
    query = cheap_fare_amount(workload, params, **kwargs) 
    query["request-cache"] = False
    return query

def cheap_total_amount(workload, params, **kwargs): 
    val_dict = get_values(params, ["cheap_total_amount"])[0]
    return get_basic_range_query("total_amount", val_dict["gte"], val_dict["lte"])

def cheap_total_amount_no_cache(workload, params, **kwargs): 
    query = cheap_total_amount(workload, params, **kwargs) 
    query["request-cache"] = False
    return query

def cheap_pickup(workload, params, **kwargs): 
    val_dict = get_values(params, ["cheap_pickup"])[0]
    return get_basic_range_query("pickup_datetime", val_dict["gte"], val_dict["lte"])

def cheap_pickup_no_cache(workload, params, **kwargs): 
    query = cheap_pickup(workload, params, **kwargs) 
    query["request-cache"] = False
    return query

def cheap_dropoff(workload, params, **kwargs): 
    val_dict = get_values(params, ["cheap_dropoff"])[0]
    return get_basic_range_query("dropoff_datetime", val_dict["gte"], val_dict["lte"])

def cheap_dropoff_no_cache(workload, params, **kwargs): 
    query = cheap_dropoff(workload, params, **kwargs) 
    query["request-cache"] = False
    return query


# modified from Kiran's expensive_1 
# (see https://github.com/kiranprakash154/opensearch-benchmark-workloads/blob/kp/custom-workload/nyc_taxis/workload.py)
def expensive_1(workload, params, **kwargs):
    fn_names_list = [
        "cheap_pickup",
        "cheap_dropoff"
    ]
    vals = get_values(params, fn_names_list)
    return {
        "body": {
            "size": 0,
            "query": {
                "bool": {
                    "filter": [
                        {
                            "range": {
                                "pickup_datetime": {
                                    "gte": vals[0]["gte"],
                                    "lte": vals[0]["lte"]
                                }
                            }
                        },
                        {
                            "range": {
                                "dropoff_datetime": {
                                    "gte": vals[1]["gte"],
                                    "lte": vals[1]["lte"]
                                }
                            }
                        }
                    ],
                    "must_not": [
                        {
                            "term": {
                                "vendor_id": "Vendor XYZ"
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "avg_surcharge": {
                    "avg": {
                        "field": "surcharge"
                    }
                },
                "sum_total_amount": {
                    "sum": {
                        "field": "total_amount"
                    }
                },
                "vendor_id_terms": {
                    "terms": {
                        "field": "vendor_id",
                        "size": 100
                    },
                    "aggs": {
                        "avg_tip_per_vendor": {
                            "avg": {
                                "field": "tip_amount"
                            }
                        }
                    }
                },
                "pickup_location_grid": {
                    "geohash_grid": {
                        "field": "pickup_location",
                        "precision": 5
                    },
                    "aggs": {
                        "avg_tip_per_location": {
                            "avg": {
                                "field": "tip_amount"
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


def expensive_1_no_cache(workload, params, **kwargs):
    query = expensive_1(workload, params, **kwargs) 
    query["request-cache"] = False
    return query

def expensive_2(workload, params, **kwargs): 
    val_dict = get_values(params, ["cheap_pickup"])[0]
    return {
        "body": {
                "size": 0,
                "query": {
                    "range": {
                        "pickup_datetime": {
                            "gte": val_dict["gte"],
                            "lte": val_dict["lte"]
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
                            "avg_total_amount": {
                                "avg": {
                                    "field": "total_amount"
                                }
                            },
                            "vendor_name_terms": {
                                "terms": {
                                    "field": "vendor_name.keyword",
                                    "size": 100
                                },
                                "aggs": {
                                    "avg_tip_per_vendor_name": {
                                        "avg": {
                                            "field": "tip_amount"
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
def expensive_2_no_cache(workload, params, **kwargs): 
    query = expensive_2(workload, params, **kwargs) 
    query["request-cache"] = False 
    return query

def expensive_3(workload, params, **kwargs): 
    val_dict = get_values(params, ["cheap_pickup"])[0]
    return {
        "body": {
                "size": 0,
                "query": {
                    "range": {
                        "pickup_datetime": {
                            "gte": val_dict["gte"],
                            "lte": val_dict["lte"]
                        }
                    }
                },
                "aggs": {
                    "sum_total_amount": {
                        "sum": {
                            "field": "total_amount"
                        }
                    },
                    "sum_tip_amount": {
                        "sum": {
                            "field": "tip_amount"
                        }
                    }
                }
        },
        "index": 'nyc_taxis',
        "request-cache": True,
        "request-timeout": 60
    }

def expensive_3_no_cache(workload, params, **kwargs): 
    query = expensive_3(workload, params, **kwargs) 
    query["request-cache"] = False 
    return query

def expensive_4(workload, params, **kwargs): 
    val_dict = get_values(params, ["cheap_pickup"])[0]
    return {
        "body": {
                "size": 0,
                "query": {
                    "range": {
                        "pickup_datetime": {
                            "gte": val_dict["gte"],
                            "lte": val_dict["lte"]
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

def expensive_4_no_cache(workload, params, **kwargs): 
    query = expensive_4(workload, params, **kwargs) 
    query["request-cache"] = False 
    return query



def register(registry):
    registry.register_param_source("cheap-passenger-count-param-source", cheap_passenger_count)
    registry.register_param_source("cheap-passenger-count-no-cache-param-source", cheap_passenger_count_no_cache)
    registry.register_param_source("cheap-tip-amount-param-source", cheap_tip_amount)
    registry.register_param_source("cheap-tip-amount-no-cache-param-source", cheap_tip_amount_no_cache)
    registry.register_param_source("cheap-fare-amount-param-source", cheap_fare_amount)
    registry.register_param_source("cheap-fare-amount-no-cache-param-source", cheap_fare_amount_no_cache)
    registry.register_param_source("cheap-total-amount-param-source", cheap_total_amount)
    registry.register_param_source("cheap-total-amount-no-cache-param-source", cheap_total_amount_no_cache)
    registry.register_param_source("cheap-pickup-param-source", cheap_pickup)
    registry.register_param_source("cheap-pickup-no-cache-param-source", cheap_pickup_no_cache)
    registry.register_param_source("cheap-dropoff-param-source", cheap_dropoff)
    registry.register_param_source("cheap-dropoff-no-cache-param-source", cheap_dropoff_no_cache)
    registry.register_param_source("expensive-1-param-source", expensive_1)
    registry.register_param_source("expensive-1-no-cache-param-source", expensive_1_no_cache)
    registry.register_param_source("expensive-2-param-source", expensive_2)
    registry.register_param_source("expensive-2-no-cache-param-source", expensive_2_no_cache)
    registry.register_param_source("expensive-3-param-source", expensive_3)
    registry.register_param_source("expensive-3-no-cache-param-source", expensive_3_no_cache)
    registry.register_param_source("expensive-4-param-source", expensive_4)
    registry.register_param_source("expensive-4-no-cache-param-source", expensive_4_no_cache)
    registry.register_runner("delete-snapshot", delete_snapshot, async_runner=True)