# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

from .runners import register as register_runners
from random import uniform

dimensions = 128 
# TODO: Refine this based on the distribution of base vectors in corpus. 
min_value = -100 
max_value = 100
def vector_source(): 
    v = [] 
    for i in range(dimensions): 
        v.append(uniform(min_value, max_value))
    return {
        "vector":v
    }

def register(registry):
    # note: hardcoded for field name to be "target_field" (here and in operation definition)
    registry.register_standard_value_source("randomized-vector-search", "target_field", vector_source)
    registry.register_query_randomization_info("randomized-vector-search", "knn", [["vector"]], [])
    register_runners(registry)
