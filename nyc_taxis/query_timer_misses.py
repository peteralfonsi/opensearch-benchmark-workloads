# import argparse
# import requests
# from opensearchpy import OpenSearch
# import matplotlib.pyplot as plt
# import numpy as np
#
# # Notify IFTTT when script is done
# def send_ifft_notification(api_key):
#     event_name = 'script_done'  # Event name from your IFTTT applet
#     url = f'https://maker.ifttt.com/trigger/{event_name}/with/key/{api_key}'
#
#     response = requests.post(url)
#     if response.status_code == 200:
#         print("IFTTT notification sent successfully.")
#     else:
#         print("Failed to send IFTTT notification.")
#
# # Expensive query to be used
# def expensive_1(day, cache, **kwargs):
#     return {
#         "body": {
#             "size": 0,
#             "query": {
#                 "bool": {
#                     "filter": [
#                     {
#                         "range": {
#                             "pickup_datetime": {
#                                 "gte": '2015-01-01 00:00:00',
#                                 "lte": f"2015-01-{day:02d} 11:59:59"
#                             }
#                         }
#                     },
#                     {
#                         "range": {
#                             "dropoff_datetime": {
#                                 "gte": '2015-01-01 00:00:00',
#                                 "lte": f"2015-01-{day:02d} 11:59:59"
#                             }
#                         }
#                     }
#                 ],
#                 "must_not": [
#                     {
#                         "term": {
#                             "vendor_id": "Vendor XYZ"
#                         }
#                     }
#                 ]
#             }
#         },
#         "aggs": {
#             "avg_surcharge": {
#                 "avg": {
#                     "field": "surcharge"
#                 }
#             },
#             "sum_total_amount": {
#                 "sum": {
#                     "field": "total_amount"
#                 }
#             },
#             "vendor_id_terms": {
#                 "terms": {
#                     "field": "vendor_id",
#                     "size": 100
#                 },
#                 "aggs": {
#                     "avg_tip_per_vendor": {
#                         "avg": {
#                             "field": "tip_amount"
#                         }
#                     }
#                 }
#             },
#             "pickup_location_grid": {
#                 "geohash_grid": {
#                     "field": "pickup_location",
#                     "precision": 5
#                 },
#                 "aggs": {
#                     "avg_tip_per_location": {
#                         "avg": {
#                             "field": "tip_amount"
#                         }
#                     }
#                 }
#             }
#         }
#       },
#         "index": 'nyc_taxis',
#         "request-cache" : cache,
#         "request-timeout": 60
#     }
#
#
# # Function to send the query and measure the response time
# def send_query_and_measure_time(day, hit_count, endpoint, username, password, cache):
#     query = expensive_1(day, cache)
#
#     # Connect to the OpenSearch domain using the provided endpoint and credentials
#     os = OpenSearch(
#         [endpoint],
#         http_auth=(username, password),
#         port=443,
#         use_ssl=True,
#     )
#
#     # Send the query to the OpenSearch domain
#     response = os.search(index=query['index'], body=query['body'], request_timeout=60, request_cache=cache)
#     took_time = response['took']
#
#     return took_time
#
# # Function to retrieve the cache stats to check hit counts
# def get_request_cache_stats(endpoint, username, password):
#     url = f"{endpoint}/_nodes/stats/indices/request_cache"
#     response = requests.get(url, auth=(username, password))
#
#     if response.status_code == 200:
#         return response.json()
#     else:
#         print("Failed to retrieve request cache stats.")
#         return None
#
# def clearcache(args):
#     # Clear cache and verify response
#     url = f"{args.endpoint}/nyc_taxis/_cache/clear"
#     response = requests.post(url, auth=(args.username, args.password))
#
#     if response.status_code == 200:
#         print("Request cache cleared successfully.")
#     else:
#         print("Failed to clear request cache." + str(response.status_code))
#
# def main():
#     parser = argparse.ArgumentParser(description='OpenSearch Query Response Time Plotter')
#     parser.add_argument('--endpoint', help='OpenSearch domain endpoint (https://example.com)')
#     parser.add_argument('--username', help='Username for authentication')
#     parser.add_argument('--password', help='Password for authentication')
#     parser.add_argument('--days',     help='Number of days in the range to keep increasing to')
#     parser.add_argument('--cache',    help='True for cache enabled and false otherwise, defaults to FALSE.', default='false')
#     parser.add_argument('--apikey', help='IFTTT API key for notifying when the script is finished.')
#     args = parser.parse_args()
#
#     # Clear cache
#     clearcache(args)
#
#     # Get baseline hit count
#     data = get_request_cache_stats(args.endpoint, args.username, args.password)
#     hit_count = next(iter(data['nodes'].values()))['indices']['request_cache']['hit_count']
#
#     num_queries = 50 # Number of times to execute the query for each date range
#     save_path = '/home/ec2-user/opensearch-benchmark-workloads/nyc_taxis/results'  # Path to save results
#
#     miss_took_times = []
#     daily_averages = []
#     daily_p99_latencies = []
#
#     # Execute the query multiple times and measure the response time
#     for day in range(1, int(args.days) + 1):
#         print(f"Starting iterations for range: Jan 1 00:00:00 to Jan {day} 11:59:59")
#         response_times = []
#         for x in range(1, num_queries + 1):
#             response_time = send_query_and_measure_time(day, hit_count, args.endpoint, args.username, args.password, args.cache) # Get took time for query
#             new_hits = next(iter(get_request_cache_stats(args.endpoint, args.username, args.password)['nodes'].values()))[
#                 'indices']['request_cache']['hit_count'] # Check new number of hits
#
#             if new_hits > hit_count: # If hit count increased
#                 print(f"Hit. Took time: {response_time}")
#                 hit_count = new_hits
#                 isHit = True
#             else:
#                 miss_took_times.append(response_time)
#                 print(f"Miss. Took time: {response_time}")
#                 isHit = False
#
#             # Append a tuple with response time and hit/miss status
#             response_times.append(response_time)
#             print(f"Response {x} received.")
#
#         average_response_time = sum(response_times) / (num_queries - 1)
#         p99_latency = np.percentile(response_times, 99)
#         p90_latency = np.percentile(response_times, 90)
#
#         # collating the data
#         daily_averages.append(average_response_time)
#         daily_p99_latencies.append(p99_latency)
#
#         # Save the figure to the specified folder
#         save_filename = 'PoC_time_50hits_plot_until_jan' + str(day) + '.png'  # You can change the filename if needed
#         save_full_path = f'{save_path}/{save_filename}'
#         figure = plt.gcf()
#         figure.savefig(save_full_path)
#         plt.close(figure)
#         plt.close()
#         print("Average response time: ", average_response_time)
#         print("File saved to ", save_path)
#
#     # print items in tabular
#     print("All Average response times: ")
#     for avg_time in enumerate(daily_averages, start=1):
#         print(f"{avg_time}")
#
#     print("All Miss took times: ")
#     for miss_took_time in enumerate(miss_took_times, start=1):
#         print(f"{miss_took_time}")
#
#     print("All p99 response times:")
#     for daily_p99_latency in enumerate(daily_p99_latencies, start=1):
#         print(f"{daily_p99_latency}")
#
#     plt.figure(figsize=(10, 6))
#     plt.plot(range(1, int(args.days) + 1), daily_averages, 'r-', label='Average Response Time')
#     plt.plot(range(1, int(args.days) + 1), daily_p99_latencies, 'b-', label='p99 Latency')
#     plt.xlabel('Day of the Month')
#     plt.ylabel('Time (milliseconds)')
#     plt.title('OpenSearch Query Response Time and p99 Latency for the Month')
#     plt.legend()
#
#     # Save the cumulative figure
#     save_full_path_cumulative = f'{save_path}/cumulative_plot.png'
#     plt.savefig(save_full_path_cumulative)
#     plt.close()
#
#     print("Cumulative file saved to ", save_path)
#
#     send_ifft_notification(args.apikey)
#
# if __name__ == '__main__':
#     main()