import argparse
import requests
from opensearchpy import OpenSearch
import matplotlib.pyplot as plt
import numpy as np
import datetime
import csv
import time

# Notify IFTTT when script is done
def send_ifft_notification(webhook, type):
    slackurl = webhook

    data = {
        "value1": type
    }

    response = requests.post(slackurl, json=data)
    if response.status_code == 200:
        print("Slack notification sent successfully.")
    else:
        print("Failed to send Slack notification.")

# Expensive query to be used
def expensive_1(day, cache, **kwargs):
    return {
        "body": {
            "size": 0,
            "query": {
                "bool": {
                    "filter": [
                    {
                        "range": {
                            "pickup_datetime": {
                                "gte": '2015-01-01 00:00:00',
                                "lte": f"2015-01-{day:02d} 11:59:59"
                            }
                        }
                    },
                    {
                        "range": {
                            "dropoff_datetime": {
                                "gte": '2015-01-01 00:00:00',
                                "lte": f"2015-01-{day:02d} 11:59:59"
                            }
                        }
                    }
                ],
                "must_not": [
                    {
                        "term": {
                            "vendor_id": "Vendor XYZ"
                        }
                    }
                ]
            }
        },
        "aggs": {
            "avg_surcharge": {
                "avg": {
                    "field": "surcharge"
                }
            },
            "sum_total_amount": {
                "sum": {
                    "field": "total_amount"
                }
            },
            "vendor_id_terms": {
                "terms": {
                    "field": "vendor_id",
                    "size": 100
                },
                "aggs": {
                    "avg_tip_per_vendor": {
                        "avg": {
                            "field": "tip_amount"
                        }
                    }
                }
            },
            "pickup_location_grid": {
                "geohash_grid": {
                    "field": "pickup_location",
                    "precision": 5
                },
                "aggs": {
                    "avg_tip_per_location": {
                        "avg": {
                            "field": "tip_amount"
                        }
                    }
                }
            }
        }
      },
        "index": 'nyc_taxis',
        "request-cache" : cache,
        "request-timeout": 60
    }


# Function to send the query and measure the response time
def send_query_and_measure_time(day, hit_count, endpoint, username, password, cache):
    query = expensive_1(day, cache)

    # Connect to the OpenSearch domain using the provided endpoint and credentials
    os = OpenSearch(
        [endpoint],
        http_auth=(username, password),
        port=443,
        use_ssl=True,
    )

    # Send the query to the OpenSearch domain
    response = os.search(index=query['index'], body=query['body'], request_timeout=60, request_cache=cache)
    took_time = response['took']

    return took_time

# Function to retrieve the cache stats to check hit counts
def get_request_cache_stats(endpoint, username, password):
    url = f"{endpoint}/_nodes/stats/indices/request_cache"
    response = requests.get(url, auth=(username, password))

    if response.status_code == 200:
        return response.json()
    else:
        print("Failed to retrieve request cache stats.")
        return None

def clearcache(args):
    # Clear cache and verify response
    url = f"{args.endpoint}/nyc_taxis/_cache/clear"
    response = requests.post(url, auth=(args.username, args.password))

    if response.status_code == 200:
        print("Request cache cleared successfully.")
    else:
        print("Failed to clear request cache." + str(response.status_code))

def main():
    parser = argparse.ArgumentParser(description='OpenSearch Query Response Time Plotter')
    parser.add_argument('--endpoint', help='OpenSearch domain endpoint (https://example.com)')
    parser.add_argument('--username', help='Username for authentication')
    parser.add_argument('--password', help='Password for authentication')
    parser.add_argument('--days',     help='Number of days in the range to keep increasing to')
    parser.add_argument('--cache',    help='True for cache enabled and false otherwise, defaults to FALSE.', default='false')
    parser.add_argument('--type', help='Type of cache we are using, for logging purposes')
    parser.add_argument('--webhook', help='Slack webhook for notifying when the script is finished.')
    args = parser.parse_args()

    # Get baseline hit count
    data = get_request_cache_stats(args.endpoint, args.username, args.password)
    hit_count = next(iter(data['nodes'].values()))['indices']['request_cache']['hit_count']

    num_queries = 250 # Number of times to execute the query for each date range
    save_path = 'results/'  # Path to save results

    miss_took_times = []
    daily_averages = []
    daily_p99_latencies = []
    daily_p95_latencies = []
    daily_p90_latencies = []

    # Get the current date and time
    current_datetime = datetime.datetime.now()

    # Format the datetime as a string for the filename
    formatted_datetime = current_datetime.strftime("%Y-%m-%d_%H-%M-%S")

    # Create a filename using the formatted datetime
    filename = f"results_{formatted_datetime}.csv"

    # Execute the query multiple times and measure the response time
    for day in range(1, int(args.days) + 1):
        clearcache(args) # clear cache to start
        print(f"Starting iterations for range: Jan 1 00:00:00 to Jan {day} 11:59:59")
        response_times = []
        for x in range(1, num_queries + 1):
            response_time = send_query_and_measure_time(day, hit_count, args.endpoint, args.username, args.password, args.cache) # Get took time for query
            new_hits = next(iter(get_request_cache_stats(args.endpoint, args.username, args.password)['nodes'].values()))['indices']['request_cache']['hit_count'] # Check new number of hits

            if new_hits > hit_count: # If hit count increased
                print(f"Hit. Took time: {response_time}")
                hit_count = new_hits
                isHit = True
            else:
                miss_took_times.append(response_time)
                print(f"Miss. Took time: {response_time}")
                isHit = False

            # Append a tuple with response time and hit/miss status
            response_times.append(response_time)
            print(f"Response {x} received.")

        median = np.median(response_times[1:])
        average_response_time = sum(response_times[1:]) / (num_queries - 1) # Average response time for num_queries - 1 hits (first was a miss before it got written to the cache)
        p99_latency = np.percentile(response_times[1:], 99) # Calculate p99 latency
        p95_latency = np.percentile(response_times[1:], 95) # Calculate p95
        p90_latency = np.percentile(response_times[1:], 90) # Calculate p90


        # Collect the data
        daily_averages.append(average_response_time)
        daily_p99_latencies.append(p99_latency)
        daily_p95_latencies.append(p95_latency)
        daily_p90_latencies.append(p90_latency)
        
        with open(save_path + filename, 'a') as csv_file:
            csv_file.write(f'Jan 1 to Jan {str(day)} using cache of type: {args.type} \n')
            for value in response_times:
                csv_file.write(str(value) + '\n')
            csv_file.write(f"Average response time: {average_response_time} \n")
            csv_file.write(f"Median response time: {median} \n")
            csv_file.write(f"p99 latency: {round(p99_latency, 3)} \n")
            csv_file.write(f"p95 latency: {round(p95_latency, 3)} \n")
            csv_file.write(f"p90 latency: {round(p90_latency, 3)} \n ")
            csv_file.write(f"Minimum: {min(response_times[1:])} \n ")
            csv_file.write(f"Maximum: {max(response_times[1:])} \n ")
            csv_file.write("\n")
        
        print(f"Results for Jan 1 to Jan {str(day)} appended to {filename}.")

    # print items in tabular
    print(f"Results for cache of type {args.type}")
    print("All average response times: ")
    for avg_time in enumerate(daily_averages, start=1):
        print(f"{avg_time}")

    print("All Miss took times: ")
    for miss_took_time in enumerate(miss_took_times, start=1):
        print(f"{miss_took_time}")

    print("All p99 response times:")
    for daily_p99_latency in enumerate(daily_p99_latencies, start=1):
        print(f"{daily_p99_latency}")

    send_ifft_notification(args.webhook, args.type)

if __name__ == '__main__':
    main()