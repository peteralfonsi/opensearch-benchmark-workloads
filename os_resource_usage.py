import requests 
import time 
import datetime

output = "/home/ec2-user/os_resource_usage.csv"

with open(output, "a") as f: 
    f.write("Time, CPU, JVM pressure, JVM max (B)")

    while True: 
        cpu_result = requests.get("http://localhost:9200/_nodes/stats/process?pretty").json()
        jvm_result = requests.get("localhost:9200/_nodes/stats/jvm?pretty").json()

        node_id = cpu_result["nodes"].keys()[0]
        print("Found node ID = ", node_id)

        cpu_usage = cpu_result["nodes"][node_id]["process"]["cpu"]["percent"]
        jvm_pressure = jvm_result["nodes"][node_id]["jvm"]["mem"]["heap_used_percent"]
        jvm_max = jvm_result["nodes"][node_id]["jvm"]["mem"]["heap_max_in_bytes"]
        now = datetime.datetime.now()
        f.write("{},{},{},{}".format(now, cpu_usage, jvm_pressure, jvm_max))
        time.sleep(60)