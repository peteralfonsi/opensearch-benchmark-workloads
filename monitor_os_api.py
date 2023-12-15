import subprocess
import json
import datetime
import time

node_endpoint = "http://localhost:9200"
freq = 15 # seconds. increase to 60 later
num_same_before_stopping = 5 # if cache stats are unchanged (and > 0) for this many iterations, stop running
tiered_feature_flag_enabled = True

global num_same_counter
global last_heap_entry_number
num_same_counter = 0
last_heap_entry_number = 0

dump_path = "dump"
out_path_hot_threads = dump_path + "/hot_threads"
out_path_search_queue = dump_path + "/search_queue"
out_path_cache_stats = dump_path + "/cache_stats"

def formatted_now(): 
    return datetime.datetime.now().strftime("%Y-%m-%d-%H:%M:%S")

def run_hot_threads(): 
    cmd = "curl -XGET \"{}/_nodes/hot_threads?pretty\"".format(node_endpoint)
    resp = str(subprocess.run(cmd, shell=True, capture_output=True).stdout)
    fp = out_path_hot_threads + "/" + formatted_now() + ".json"
    with open(fp, "w") as f: 
        f.write(resp)

def run_search_queue(): 
    cmd = "curl -XGET \"{}/_nodes/thread_pool?pretty\"".format(node_endpoint)
    resp = json.loads(subprocess.run(cmd, shell=True, capture_output=True).stdout)
    fp = out_path_search_queue + "/" + formatted_now() + ".json"
    with open(fp, "w") as f: 
        json.dump(resp, f)

def do_stop_loop(resp): 
    node_name = next(iter(resp["nodes"]))
    rc_info = resp["nodes"][node_name]["indices"]["request_cache"]
    if tiered_feature_flag_enabled: 
        print(rc_info)
        heap_entries = int(rc_info["entries"])
    else: 
        heap_entries = rc_info["memory_size_in_bytes"] # dont have entries on main
    if heap_entries == last_heap_entry_number and heap_entries > 0: 
        num_same_counter += 1
    last_heap_entry_number = heap_entries
    if num_same_counter >= num_same_before_stopping: 
        return True

def run_cache_stats(): 
    cmd = "curl -XGET \"{}/_nodes/stats/indices/request_cache?pretty\"".format(node_endpoint)
    resp = json.loads(subprocess.run(cmd, shell=True, capture_output=True).stdout)
    fp = out_path_search_queue + "/" + formatted_now() + ".json"
    with open(fp, "w") as f: 
        json.dump(resp, f)
    return resp

while True: 
    run_hot_threads() 
    run_search_queue()
    cache_stats = run_cache_stats()
    if do_stop_loop(cache_stats): 
        break
    time.sleep(freq)

# zip up dump folder for easy scp
zip_cmd = "zip -r dump.zip dump"
subprocess.run(zip_cmd, shell=True, capture_output=True)
