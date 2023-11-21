from .query_value_providers import fn_value_generators
import json
from os import path

NUM_VALUES = 10000 # number of standard queries to generate
for fn_name in fn_value_generators: 
    print("Generating values for {}".format(fn_name))
    fp = path.relpath("standard_values/{}_values.json".format(fn_name))
    val_dict = []
    for i in range(NUM_VALUES): 
        val_dict.append(fn_value_generators[fn_name]())
    with open(fp, "w") as f:
        f.write(json.dumps(val_dict))
