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
query_types = fn_names + ["expensive_1", "expensive_2", "expensive_3", "expensive_4"]
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
        i = random.randrange(0, query_type_counters[query_type])
        # Make sure this value doesn't exceed the length of the shortest list of standard function values which we are using
        shortest_standard_list = min([len(standard_fn_values[fn_name]) for fn_name in fn_name_list]) 
        i = min(i, shortest_standard_list-1)
        # If the selected value is equal to the current counter value, this is the first time we've used this value. 
        # Increment counter value accordingly so we know it's been used in the next round
        if i == query_type_counters[query_type]: 
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

# Each specific query has a wrapper, which is actually used a param source, and a function that provides values, 
# which lives in query_value_providers.py. 
# This provider fn is also used to generate standard random values in the first place. 

def cheap_passenger_count(workload, params, **kwargs): 
    val_dict = get_values(params, ["cheap_passenger_count"], "cheap_passenger_count")[0]
    # based on random_passenger_count from https://github.com/kiranprakash154/opensearch-benchmark-workloads/blob/kp/custom-workload/nyc_taxis/workload.py
    return get_basic_range_query("passenger_count", val_dict["gte"], val_dict["lte"])

def cheap_tip_amount(workload, params, **kwargs): 
    val_dict = get_values(params, ["cheap_tip_amount"], "cheap_tip_amount")[0]
    return get_basic_range_query("tip_amount", val_dict["gte"], val_dict["lte"])


def cheap_fare_amount(workload, params, **kwargs): 
    val_dict = get_values(params, ["cheap_fare_amount"], "cheap_fare_amount")[0]
    return get_basic_range_query("fare_amount", val_dict["gte"], val_dict["lte"])

def cheap_total_amount(workload, params, **kwargs): 
    val_dict = get_values(params, ["cheap_total_amount"], "cheap_total_amount")[0]
    return get_basic_range_query("total_amount", val_dict["gte"], val_dict["lte"])

def cheap_pickup(workload, params, **kwargs): 
    val_dict = get_values(params, ["cheap_pickup"], "cheap_pickup")[0]
    return get_basic_range_query("pickup_datetime", val_dict["gte"], val_dict["lte"])

def cheap_dropoff(workload, params, **kwargs): 
    val_dict = get_values(params, ["cheap_dropoff"], "cheap_dropoff")[0]
    return get_basic_range_query("dropoff_datetime", val_dict["gte"], val_dict["lte"])

# modified from Kiran's expensive_1 
# (see https://github.com/kiranprakash154/opensearch-benchmark-workloads/blob/kp/custom-workload/nyc_taxis/workload.py)
def expensive_1(workload, params, **kwargs):
    fn_names_list = [
        "cheap_pickup",
        "cheap_dropoff"
    ]
    vals = get_values(params, fn_names_list, "expensive_1")
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
        "request-timeout": 60
    }

def expensive_2(workload, params, **kwargs): 
    val_dict = get_values(params, ["cheap_pickup"], "expensive_2")[0]
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

def expensive_3(workload, params, **kwargs): 
    val_dict = get_values(params, ["cheap_pickup"], "expensive_3")[0]
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

def expensive_4(workload, params, **kwargs): 
    val_dict = get_values(params, ["cheap_pickup"], "expensive_4")[0]
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



def register(registry):
    registry.register_param_source("cheap-passenger-count-param-source", cheap_passenger_count)
    registry.register_param_source("cheap-tip-amount-param-source", cheap_tip_amount)
    registry.register_param_source("cheap-fare-amount-param-source", cheap_fare_amount)
    registry.register_param_source("cheap-total-amount-param-source", cheap_total_amount)
    registry.register_param_source("cheap-pickup-param-source", cheap_pickup)
    registry.register_param_source("cheap-dropoff-param-source", cheap_dropoff)
    registry.register_param_source("expensive-1-param-source", expensive_1)
    registry.register_param_source("expensive-2-param-source", expensive_2)
    registry.register_param_source("expensive-3-param-source", expensive_3)
    registry.register_param_source("expensive-4-param-source", expensive_4)
    registry.register_runner("delete-snapshot", delete_snapshot, async_runner=True)