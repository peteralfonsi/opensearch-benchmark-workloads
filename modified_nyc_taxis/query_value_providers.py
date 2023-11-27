# Value providers for queries live in this file

import random

# common helper functions 

def random_money_values(max_value): 
    gte_cents = random.randrange(0, max_value*100)
    lte_cents = random.randrange(gte_cents, max_value*100)
    return {
        "gte":gte_cents/100,
        "lte":lte_cents/100
    }


# actual provider functions

def cheap_passenger_count_provider(): 
    gte = random.randrange(1, 5) # assuming passenger count is 1 to 4?
    lte = random.randrange(gte, 5)
    return {
        "gte": gte, 
        "lte": lte
    }

def cheap_tip_amount_provider(): 
    # random dollar + cents values between 0.00 and 10.99
    return random_money_values(10.99)

def cheap_fare_amount_provider(): 
    # random dollar + cents values between 0.00 and 100.99
    return random_money_values(100.99)
    


# name for each specific query
fn_names = [
    "cheap_passenger_count",
    "cheap_tip_amount",
    "cheap_fare_amount"
]

# the value generator for each specific query
fn_value_generators = {
    "cheap_passenger_count":cheap_passenger_count_provider,
    "cheap_tip_amount":cheap_tip_amount_provider,
    "cheap_fare_amount":cheap_fare_amount_provider
} 

