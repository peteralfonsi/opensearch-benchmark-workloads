import requests
import time 

start = time.time()
r = requests.post("http://localhost:9200/_cache/clear")
end = time.time() 

print("Elapsed time = {} sec".format(end - start))
