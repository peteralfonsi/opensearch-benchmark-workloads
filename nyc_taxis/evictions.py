import argparse
import requests
from opensearchpy import OpenSearch
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime, timedelta
import csv
import time
import random

miss_took_times = []
hit_took_times = []
daily_averages = []
daily_p99_latencies = []
daily_p95_latencies = []
daily_p90_latencies = []
daily_medians = []
daily_mins = []
daily_max = []


# Notify Slack when script is done
def send_slack_notification(webhook, payload):
    slackurl = webhook

    data = {
        "value1": payload
    }

    response = requests.post(slackurl, json=data)
    print(f"slack response : {response}")
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
        "request-cache": cache,
        "request-timeout": 60
    }


def distance_amount_agg(cache):  # 1
    max_distance = random.uniform(0, 100)  # Change the range as needed
    min_distance = random.uniform(0, max_distance)
    interval = random.uniform(0, 20)
    return {
        "body": {
            "size": 0,
            "query": {
                "bool": {
                    "filter": {
                        "range": {
                            "trip_distance": {
                                "lt": max_distance,
                                "gte": min_distance
                            }
                        }
                    }
                }
            },
            "aggs": {
                "distance_histo": {
                    "histogram": {
                        "field": "trip_distance",
                        "interval": interval
                    },
                    "aggs": {
                        "total_amount_stats": {
                            "stats": {
                                "field": "total_amount"
                            }
                        }
                    }
                }
            }
        },
        "index": 'nyc_taxis',
        "request-cache": cache,
        "request-timeout": 120
    }


def rangeQuery(cache):  # 2
    max_distance = random.uniform(0, 100)  # Change the range as needed
    min_distance = random.uniform(0, max_distance)

    return {
        "body": {
            "query": {
                "range": {
                    "total_amount": {
                        "gte": min_distance,
                        "lt": max_distance
                    }
                }
            }
        },
        "index": 'nyc_taxis',
        "request-cache": cache,
        "request-timeout": 120
    }


def autohisto_agg(cache):  # 3
    month_gte = random.randint(1, 2)
    day_gte = random.randint(1, 28)
    month_lte = random.randint(month_gte, 2)
    day_lte = random.randint(day_gte, 28)
    year = 2015
    return {
        "body": {
            "size": 0,
            "query": {
                "range": {
                    "dropoff_datetime": {
                        "gte": f"{day_gte:02}/{month_gte:02}/{year}",
                        "lte": f"{day_lte:02}/{month_lte:02}/{year}",
                        "format": "dd/MM/yyyy"
                    }
                }
            },
            "aggs": {
                "dropoffs_over_time": {
                    "auto_date_histogram": {
                        "field": "dropoff_datetime",
                        "buckets": 20
                    }
                }
            }
        },
        "index": 'nyc_taxis',
        "request-cache": cache,
        "request-timeout": 120
    }


def date_histogram_agg(cache, month):  # 4
    year = 2015
    day_gte = random.randint(1, 28)
    day_lte = random.randint(day_gte, 28)
    return {
        "body": {
            "size": 0,
            "query": {
                "range": {
                    "dropoff_datetime": {
                        "gte": f"{day_gte:02}/{month:02}/{year}",
                        "lte": f"{day_lte:02}/{month:02}/{year}",
                        "format": "dd/MM/yyyy"
                    }
                }
            },
            "aggs": {
                "dropoffs_over_time": {
                    "date_histogram": {
                        "field": "dropoff_datetime",
                        "calendar_interval": "day"
                    }
                }
            }
        },
        "index": 'nyc_taxis',
        "request-cache": cache,
        "request-timeout": 120
    }


def date_histogram_calendar_interval(cache):  # 5
    month_gte = random.randint(1, 12)
    month_lte = random.randint(month_gte, 12)
    year = 2015
    return {
        "body": {
            "size": 0,
            "query": {
                "range": {
                    "dropoff_datetime": {
                        "gte": f"{year}-{month_gte:02}-01 00:00:00",
                        "lt": f"{year}-{month_lte:02}-01 00:00:00"
                    }
                }
            },
            "aggs": {
                "dropoffs_over_time": {
                    "date_histogram": {
                        "field": "dropoff_datetime",
                        "calendar_interval": "month"
                    }
                }
            }
        },
        "index": 'nyc_taxis',
        "request-cache": cache,
        "request-timeout": 120
    }


def date_histogram_fixed_interval_with_metrics(cache, month):  # 6
    # Generate random start and end dates
    start_date = datetime(2015, 1, 1)
    end_date = datetime(2016, 1, 1)

    # Generate random number of days to add to the start date
    random_days = random.randint(30, (end_date - start_date).days)  # Randomize within a reasonable range

    # Calculate the end date based on the random days
    random_end_date = start_date + timedelta(days=random_days)

    # Calculate the time range in days
    time_range_days = (random_end_date - start_date).days

    # Calculate the appropriate fixed_interval based on the time range
    if time_range_days <= 30:
        fixed_interval = "1d"  # Daily interval for shorter time ranges
    elif time_range_days <= 180:
        fixed_interval = "7d"  # Weekly interval for medium time ranges
    else:
        fixed_interval = "30d"  # Monthly interval for longer time ranges
    return {
        "body": {
            "size": 0,
            "query": {
                "range": {
                    "dropoff_datetime": {
                        "gte": start_date.strftime("%Y-%m-%d %H:%M:%S"),
                        "lt": random_end_date.strftime("%Y-%m-%d %H:%M:%S")
                    }
                }
            },
            "aggs": {
                "dropoffs_over_time": {
                    "date_histogram": {
                        "field": "dropoff_datetime",
                        "fixed_interval": fixed_interval
                    },
                    "aggs": {
                        "total_amount": {"stats": {"field": "total_amount"}},
                        "tip_amount": {"stats": {"field": "tip_amount"}},
                        "trip_distance": {"stats": {"field": "trip_distance"}}
                    }
                }
            }
        },
        "index": 'nyc_taxis',
        "request-cache": cache,
        "request-timeout": 120
    }


# Function to send the query and measure the response time
def send_query_and_measure_time(endpoint, username, password, cache, query):
    # query = expensive_1(day, cache)
    print(f"{query}")
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


def runQuery(args, query_name, query):
    # Get baseline hit count
    data = get_request_cache_stats(args.endpoint, args.username, args.password)
    hit_count = next(iter(data['nodes'].values()))['indices']['request_cache']['hit_count']

    # for x in range(1, numberOfQueries + 1):
    response_time = send_query_and_measure_time(args.endpoint, args.username, args.password, args.cache, query)  # Get took time for query
    new_hits = next(iter(get_request_cache_stats(args.endpoint, args.username, args.password)['nodes'].values()))['indices']['request_cache']['hit_count']  # Check new number of hits
    if new_hits > hit_count:  # If hit count increased
        print(f"Hit. Took time: {response_time}")
        hit_took_times.append((response_time))
    else:
        print(f"Miss. Took time: {response_time}")
        miss_took_times.append(response_time)


def main():
    start_time = time.time()
    parser = argparse.ArgumentParser(description='OpenSearch Query Response Time Plotter')
    parser.add_argument('--endpoint', help='OpenSearch domain endpoint (https://example.com)')
    parser.add_argument('--username', help='Username for authentication')
    parser.add_argument('--password', help='Password for authentication')
    parser.add_argument('--days', help='Number of days in the range to keep increasing to')
    parser.add_argument('--cache', help='True for cache enabled and false otherwise, defaults to FALSE.',default='true')
    parser.add_argument('--type', help='Type of cache we are using, for logging purposes')
    parser.add_argument('--webhook', help='Slack webhook for notifying when the script is finished.')
    parser.add_argument('--num_queries', help='Num of queries to run.', default=2)
    parser.add_argument('--note', help='Optional note to add to the test.', default="")
    args = parser.parse_args()
    num_queries = int(args.num_queries)

    save_path = 'results/'  # Path to save results

    # Get the current date and time
    current_datetime = datetime.now()

    # Format the datetime as a string for the filename
    formatted_datetime = current_datetime.strftime("%Y-%m-%d_%H-%M-%S")

    # Create a filename using the formatted datetime
    filename = f"results_{formatted_datetime}_{args.type}_{args.note}.csv"

    # Execute the query multiple times and measure the response time
    # clearcache(args)  # clear cache to start
    print("Starting date_histogram_calendar_interval")
    start_time = time.time()
    for x in range(1, num_queries + 1):
        runQuery(args, 'date_histogram_calendar_interval', date_histogram_calendar_interval(args.cache))
        print(f"running date_histogram_calendar_interval {x}/{num_queries}")
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Total time Taken for date_histogram_calendar_interval : ", elapsed_time)

    print("Starting date_histogram_agg")
    start_time = time.time()
    for x in range(1, num_queries + 1):
        runQuery(args, 'date_histogram_agg', date_histogram_agg(args.cache, random.randint(1,12)))
        print(f"running date_histogram_agg {x}/{num_queries}")
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Total time Taken for date_histogram_agg : ", elapsed_time)

    print("Starting autohisto_agg")
    start_time = time.time()
    for x in range(1, num_queries + 1):
        runQuery(args, 'autohisto_agg', autohisto_agg(args.cache))
        print(f"running autohisto_agg {x}/{num_queries}")
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Total time Taken for autohisto_agg : ", elapsed_time)

    print("Starting range")
    start_time = time.time()
    for x in range(1, num_queries + 1):
        runQuery(args, 'range', rangeQuery(args.cache))
        print(f"running range {x}/{num_queries}")
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Total time Taken for range : ", elapsed_time)

    print("Starting distance_amount_agg")
    start_time = time.time()
    for x in range(1, num_queries + 1):
        runQuery(args, 'distance_amount_agg', distance_amount_agg(args.cache))
        print(f"running range {x}/{num_queries}")
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Total time Taken for distance_amount_agg : ", elapsed_time)

    print("Starting date_histogram_fixed_interval_with_metrics")
    start_time = time.time()
    for x in range(1, num_queries + 1):
        runQuery(args, 'date_histogram_fixed_interval_with_metrics', date_histogram_fixed_interval_with_metrics(args.cache,random.randint(1,12)))
        print(f"running date_histogram_fixed_interval_with_metrics {x}/{num_queries}")
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Total time Taken for date_histogram_fixed_interval_with_metrics : ", elapsed_time)

    print(f"All hits : {hit_took_times}")
    print(f"All miss : {miss_took_times}")

    # calculate the stats for hits
    average_response_time_hits = sum(hit_took_times) / int(args.num_queries)
    median_hits = np.median(hit_took_times)
    p99_latency_hits = np.percentile(hit_took_times, 99)  # Calculate p99 latency
    p95_latency_hits = np.percentile(hit_took_times, 95)  # Calculate p95
    p90_latency_hits = np.percentile(hit_took_times, 90)  # Calculate p90

    # calculate the stats for misses
    average_response_time_miss = sum(miss_took_times) / int(args.num_queries)
    median_miss = np.median(miss_took_times)
    p99_latency_miss = np.percentile(miss_took_times, 99)  # Calculate p99 latency
    p95_latency_miss = np.percentile(miss_took_times, 95)  # Calculate p95
    p90_latency_miss = np.percentile(miss_took_times, 90)  # Calculate p90

    with open(save_path + filename, 'a') as csv_file:
        csv_file.write(f'All Hit took times\n')
        # for value in hit_took_times:
        #     csv_file.write(str(value) + '\n')
        csv_file.write(f"Average hits response time: {average_response_time_hits} \n")
        csv_file.write(f"Median hits response time: {median_hits} \n")
        csv_file.write(f"p99 hits latency: {round(p99_latency_hits, 3)} \n")
        csv_file.write(f"p95 hits latency: {round(p95_latency_hits, 3)} \n")
        csv_file.write(f"p90 hits latency: {round(p90_latency_hits, 3)} \n ")
        csv_file.write(f"Minimum hits : {min(hit_took_times)} \n ")
        csv_file.write(f"Maximum hits : {max(hit_took_times)} \n ")
        csv_file.write("\n")

        csv_file.write(f'All Miss took times\n')
        # for value in miss_took_times:
        #     csv_file.write(str(value) + '\n')
        csv_file.write(f"Average Miss response time: {average_response_time_miss} \n")
        csv_file.write(f"Median Miss response time: {median_miss} \n")
        csv_file.write(f"p99 Miss latency: {round(p99_latency_miss, 3)} \n")
        csv_file.write(f"p95 Miss latency: {round(p95_latency_miss, 3)} \n")
        csv_file.write(f"p90 Miss latency: {round(p90_latency_miss, 3)} \n ")
        csv_file.write(f"Minimum Miss : {min(miss_took_times)} \n ")
        csv_file.write(f"Maximum Miss : {max(miss_took_times)} \n ")
        csv_file.write("\n")

    send_slack_notification(args.webhook, args.type)
    end_time = time.time()
    elapsed_time = end_time - start_time
    print("Elapsed time for the entire code: ", elapsed_time)

# # print items in tabular
# print(f"Results for cache of type {args.type}")
# print("All average response times: ")
# for avg_time in enumerate(daily_averages, start=1):
#     print(f"{avg_time}")

# print("All Miss took times: ")
# for miss_took_time in enumerate(miss_took_times, start=1):
#     print(f"{miss_took_time}")

# print("All p90 response times:")
# for daily_p90_latency in enumerate(daily_p90_latencies, start=1):
#     print(f"{daily_p90_latency}")

if __name__ == '__main__':
    main()
