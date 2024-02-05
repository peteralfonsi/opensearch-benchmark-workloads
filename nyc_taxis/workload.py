import random
async def delete_snapshot(opensearch, params):
    await opensearch.snapshot.delete(repository=params["repository"], snapshot=params["snapshot"])

def total_amount_std_value_source():
    gte = random.randrange(1, 4)
    lte = random.randrange(gte, 7)
    return {"gte":gte, "lte":lte}

def trip_distance_std_value_source():
    gte = random.randrange(1, 7)
    lte = random.randrange(gte, 15)
    return {"gte":gte, "lte":lte}

def dropoff_datetime_std_value_source():
    #return {"gte":"2015-01-01 00:00:00", "lte":"2015-01-04 12:00:00"}
    return {"gte":"03/01/2015", "lte":"05/01/2015", "format":"dd/MM/yyyy"}

def register(registry):
    registry.register_runner("delete-snapshot", delete_snapshot, async_runner=True)
    registry.register_standard_value_source("range", "total_amount", total_amount_std_value_source)
    registry.register_standard_value_source("distance_amount_agg", "trip_distance", trip_distance_std_value_source)
    registry.register_standard_value_source("autohisto_agg", "dropoff_datetime", dropoff_datetime_std_value_source)
    registry.register_standard_value_source("date_histogram_agg", "dropoff_datetime", dropoff_datetime_std_value_source)
