import random

def bounding_box_source(): 
    top_longitude = random.uniform(-180, 180)
    top_latitude = random.uniform(-90, 90)

    bottom_longitude = random.uniform(top_longitude, 180)
    bottom_latitude = random.uniform(-90, top_latitude)
    return { 
        "top_left":[top_longitude, top_latitude],
        "lower_right":[bottom_longitude, bottom_latitude]
    }

def register(registry):
    # Register standard value sources for range queries defined in operations/default.json. 
    # These are only used if --randomization-enabled is present. 
    registry.register_standard_value_source("bbox", "location", bounding_box_source)
    registry.register_target_keys_info("bbox", "geo_bounding_box", [["top_left"], ["lower_right"]], [])