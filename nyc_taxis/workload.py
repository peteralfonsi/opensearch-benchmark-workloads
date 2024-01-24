async def delete_snapshot(opensearch, params):
    await opensearch.snapshot.delete(repository=params["repository"], snapshot=params["snapshot"])

def total_amount_std_value_source():
    return {"gte":1, "lte":4}

def trip_distance_std_value_source():
    return {"gte":5, "lte":7}

def dropoff_datetime_std_value_source():
    #return {"gte":"2015-01-01 00:00:00", "lte":"2015-01-04 12:00:00"}
    return {"gte":"03/01/2015", "lte":"05/01/2015"}

def register(registry):
    registry.register_runner("delete-snapshot", delete_snapshot, async_runner=True)
    registry.register_standard_value_source("total_amount", total_amount_std_value_source)
    registry.register_standard_value_source("trip_distance", trip_distance_std_value_source)
    registry.register_standard_value_source("dropoff_datetime", dropoff_datetime_std_value_source)
