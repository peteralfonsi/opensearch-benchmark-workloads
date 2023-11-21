# Value providers for queries live in this file

import random

def cheap_passenger_count_provider(): 
    gte = random.randrange(1, 5) # assuming passenger count is 1 to 4?
    lte = random.randrange(gte, 5)
    return {
        "gte": gte, 
        "lte": lte
    }


# name for each specific query
fn_names = [
    "cheap_passenger_count",
]

# the value generator for each specific query
fn_value_generators = {
    "cheap_passenger_count":cheap_passenger_count_provider
} 

