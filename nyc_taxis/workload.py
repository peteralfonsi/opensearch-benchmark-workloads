async def delete_snapshot(opensearch, params):
    await opensearch.snapshot.delete(repository=params["repository"], snapshot=params["snapshot"])

def total_amount_std_value_source():
    return {"gte":1, "lte":4}

def register(registry):
    registry.register_runner("delete-snapshot", delete_snapshot, async_runner=True)
    registry.register_standard_value_source("total_amount", total_amount_std_value_source)
