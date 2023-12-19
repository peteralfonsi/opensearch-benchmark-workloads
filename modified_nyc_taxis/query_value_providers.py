# Value providers for queries live in this file

import random
import datetime

# common helper functions 

def format_date(datetime_obj): 
    return datetime_obj.strftime("%Y-%m-%d %H:%M:%S") # check this

def random_dates_with_constant_range(separation_days): 
    jan1 = datetime.datetime(2015, 1, 1)
    dec31 = datetime.datetime(2015, 1, 15)
    min_lowerbound_timestamp = datetime.datetime.timestamp(jan1) # timestamps are in seconds, not ns
    max_lowerbound_timestamp = datetime.datetime.timestamp(dec31) - (24 * 3600 * separation_days)
    lowerbound_fraction = random.uniform(0, 1)
    diff = max_lowerbound_timestamp - min_lowerbound_timestamp
    base_lowerbound_timestamp = min_lowerbound_timestamp + int(lowerbound_fraction * diff)
    base_upperbound_timestamp = base_lowerbound_timestamp + (24 * 3600 * separation_days)

    fuzz_max_seconds = 3600 # one hour of fuzz in either direction, to make extra sure we dont get unwanted collisions
    lowerbound_fuzz = random.uniform(-fuzz_max_seconds, fuzz_max_seconds)
    upperbound_fuzz = random.uniform(-fuzz_max_seconds, fuzz_max_seconds)

    lowerbound_date = datetime.datetime.fromtimestamp(base_lowerbound_timestamp + lowerbound_fuzz) 
    upperbound_date = datetime.datetime.fromtimestamp(base_upperbound_timestamp + upperbound_fuzz)

    return {
        "gte":format_date(lowerbound_date), 
        "lte":format_date(upperbound_date)
    }


# actual provider functions
def ps_1d_provider(): 
    return random_dates_with_constant_range(1)

def ps_2d_provider(): 
    return random_dates_with_constant_range(2)

def ps_4d_provider(): 
    return random_dates_with_constant_range(4)

def ps_1w_provider(): 
    return random_dates_with_constant_range(7)

def ps_2w_provider(): 
    return random_dates_with_constant_range(14)
    
def ps_3w_provider(): 
    return random_dates_with_constant_range(21)

def ps_1m_provider(): 
    return random_dates_with_constant_range(30)

def ps_6w_provider(): 
    return random_dates_with_constant_range(45)

def ps_2m_provider(): 
    return random_dates_with_constant_range(60)


# name for each specific query
fn_names = [
    "ps_1d",
    "ps_2d",
    "ps_4d",
    "ps_1w", 
    "ps_2w", 
    "ps_3w", 
    "ps_1m", 
    "ps_6w", 
    "ps_2m"
]

# the value generator for each specific query
fn_value_generators = {
    "ps_1d":ps_1d_provider,
    "ps_2d":ps_2d_provider,
    "ps_4d":ps_4d_provider,
    "ps_1w":ps_1w_provider, 
    "ps_2w":ps_2w_provider, 
    "ps_3w":ps_3w_provider, 
    "ps_1m":ps_1m_provider, 
    "ps_6w":ps_6w_provider, 
    "ps_2m":ps_2m_provider
} 

