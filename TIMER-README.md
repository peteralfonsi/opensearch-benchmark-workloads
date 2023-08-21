
# OpenSearch Query Timer

This tool will allow you to send a query of your choosing to a running OpenSearch domain any number of times. The program has two main outputs:

• A csv file that contains all the raw took times for each query

• An Excel file with average, median, p99, p95, p90, minimum, and maximum took times




## Installation

1. Clone this repository or download the script directly.

2. Install the required Python packages using the following command: 

```bash
pip install argparse requests opensearch-py numpy openpyxl pytz
```
    
## Usage/Examples

Run the script with the required arguments to execute the queries, measure response times, and save the results to an Excel file. The script accepts the following command-line arguments:

```
--endpoint: OpenSearch domain endpoint (https://example.com)
--username: Username for authentication
--password: Password for authentication
--days: Number of days in the range to keep increasing to
--cache: True for cache enabled and false otherwise (defaults to true)
--type: Type of cache used (for logging purposes)
--webhook: Slack webhook for notifying when the script is finished (optional)
--numOfQueries: Number of queries to make in each run (default: 250)
--note: Optional note to add to the test (default: "")
```

For example:
```
python query_timer_hits.py --endpoint https://example.com --username user --password password --days 10 --cache true --type all --webhook https://slack.webhook.url
```



## License

[MIT](https://choosealicense.com/licenses/mit/)

