import requests 
import time 
import datetime

output = "/home/ec2-user/cluster_resource_usage.csv"

with open(output, "a") as f: 
    f.write("Time, CPU, JVM pressure, JVM max (B), Pct of time on old GC, Pct of time on young GC,Queued vthread count,Mounted vthread count,Parallelism,Pool size,Search active,Search queued,Searcher active,Searcher queued\n")

last_gc_old = -1
last_gc_young = -1

sleep_time = 15

i = 0
while True: 
    cpu_result = requests.get("http://localhost:9200/_nodes/stats/process?pretty").json()
    jvm_result = requests.get("http://localhost:9200/_nodes/stats/jvm?pretty").json()
    vthreads_result = requests.get("http://localhost:9200/_nodes/stats/virtual_thread_scheduler?pretty").json()
    cat_search_threadpool_result = requests.get("http://localhost:9200/_cat/thread_pool/search?pretty&format=json").json()[0]
    cat_searcher_threadpool_result= requests.get("http://localhost:9200/_cat/thread_pool/index_searcher?pretty&format=json").json()[0]

    node_id = list(cpu_result["nodes"].keys())[0]
    if i == 0: 
        print("Found node ID = ", node_id)

    cpu_usage = cpu_result["nodes"][node_id]["process"]["cpu"]["percent"]
    jvm_pressure = jvm_result["nodes"][node_id]["jvm"]["mem"]["heap_used_percent"]
    jvm_max = jvm_result["nodes"][node_id]["jvm"]["mem"]["heap_max_in_bytes"]

    total_gc_old = jvm_result["nodes"][node_id]["jvm"]["gc"]["collectors"]["old"]["collection_time_in_millis"] 
    total_gc_young = jvm_result["nodes"][node_id]["jvm"]["gc"]["collectors"]["young"]["collection_time_in_millis"] 
    gc_old_percent = (total_gc_old - last_gc_old) / (1000 * sleep_time)
    gc_young_percent = (total_gc_young - last_gc_young) / (1000 * sleep_time)
    last_gc_old = total_gc_old
    last_gc_young = total_gc_young

    vthreads_obj = vthreads_result["nodes"][node_id]["virtual_thread_scheduler"]
    queued_virtual_thread_count = vthreads_obj["queued_virtual_thread_count"]
    mounted_virtual_thread_count = vthreads_obj["mounted_virtual_thread_count"]
    parallelism = vthreads_obj["parallelism"]
    pool_size = vthreads_obj["pool_size"]

    search_active = cat_search_threadpool_result["active"]
    search_queued = cat_search_threadpool_result["queue"]
    searcher_active = cat_searcher_threadpool_result["active"]
    searcher_queued = cat_searcher_threadpool_result["queue"]
    


    now = datetime.datetime.now()
    line = "{},{},{},{},{},{},{},{},{},{}\n".format(now, cpu_usage, jvm_pressure, jvm_max, gc_old_percent, gc_young_percent, queued_virtual_thread_count, mounted_virtual_thread_count, parallelism, pool_size, search_active, search_queued, searcher_active, searcher_queued)
    with open(output, "a") as f: 
        f.write(line) 
    time.sleep(sleep_time)
    i+=1