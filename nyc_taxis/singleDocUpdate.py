import argparse
from opensearchpy import OpenSearch
import time


def toggle_payment_type(args):
    # Fetch the specific document
    os = OpenSearch(
        [args.endpoint],
        http_auth=(args.username, args.password),
        port=443,
        use_ssl=True,
    )

    doc = os.get(index=args.index, id=args.docId)

    # Toggle payment_type value
    current_payment_type = doc['_source'].get('payment_type')
    if current_payment_type == "1":
        new_payment_type = "10"
    else:
        new_payment_type = "1"

    # Update the document with the new payment_type
    os.update(
        index=args.index,
        id=args.docId,
        body={
            "doc": {
                "payment_type": new_payment_type
            }
        }
    )


def main():
    parser = argparse.ArgumentParser(description='OpenSearch Query Response Time Plotter')
    parser.add_argument('--endpoint', help='OpenSearch domain endpoint (https://example.com)')
    parser.add_argument('--username', help='Username for authentication')
    parser.add_argument('--password', help='Password for authentication')
    parser.add_argument('--index', help='Optional note to add to the test.', default="nyc_taxis")
    parser.add_argument('--docId', help='Doc Id to update',)
    args = parser.parse_args()

    while True:
        toggle_payment_type(args)
        time.sleep(1)


if __name__ == '__main__':
    main()