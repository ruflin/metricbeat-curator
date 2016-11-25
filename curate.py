from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import dateutil.parser
import time
import argparse


# Build it specfic for metricbeat
# * This only removes events
# * No summary, no aggregation done
# * Done base on host and metricset


def curate(index, period, scroll_size, client):

    count_deleted_docs = 0
    count_scanned_docs = 0

    # Iterate over hits of each fet
    timestamps = {}

    # Fetch first batch of documents
    res = client.search(index=index, size=scroll_size, scroll="1m", sort=["@timestamp:asc"])
    scroll_id = res['_scroll_id']

    while len(res['hits']['hits']) > 0:

        remove_docs = []

        for hit in res['hits']['hits']:

            count_scanned_docs += 1

            # Load data from doc
            t = hit['_source']['@timestamp']

            # Combine module-metricset to create unique key
            metricset =  hit['_source']['metricset']['module'] + "-" + hit['_source']['metricset']['name']
            hostname = hit['_source']['beat']['hostname']

            # Convert timestamp
            t = dateutil.parser.parse(t)
            t = time.mktime(t.timetuple())

            # Init fields
            if hostname not in timestamps:
                timestamps[hostname] = {}

            # Init timestamp if not existent yet
            if metricset not in timestamps[hostname]:
                timestamps[hostname][metricset] = 0

            # Events from the same "iteration"
            # Current assumption is if events have exact same timestamp -> events belong together
            if timestamps[hostname][metricset] == t:
                continue

            # New interval reached
            if t > (timestamps[hostname][metricset] + period):
                timestamps[hostname][metricset] = t
                continue

            remove_docs.append({
                "_id": hit['_id'],
                "_index": hit['_index'],
                "_type": "metricsets",
                "_op_type": "delete"
            })


        # Delete collected ids
        helpers.bulk(client, remove_docs)

        count_deleted_docs += len(remove_docs)
        print "Deleted docs:" + str(count_deleted_docs)
        print "Scanned docs:" + str(count_scanned_docs)

        # Fetch next batch of results
        res = client.scroll(scroll_id=scroll_id, scroll="1m")



if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Curates your metricbeat data")
    parser.add_argument("-host", help="Elasticsearch host to connect to", default="localhost:9200")
    parser.add_argument("-index", help="Elasticsearch index name pattern", default="metricbeat-*")
    parser.add_argument("-period", help="New period in seconds", default=30)
    parser.add_argument("-scroll_size", help="Batch size for the scroll request", default=10000)

    args = parser.parse_args()

    client = Elasticsearch(args.host)

    # TODO: Add timestamp with
    #older_than = "30d"

    curate(index=args.index, period=int(args.period), scroll_size=int(args.scroll_size), client=client)
