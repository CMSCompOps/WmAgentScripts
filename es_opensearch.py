#!/usr/bin/python

# requirements: opensearch-py~=2.1
import datetime
import json
import logging
import time
from collections import Counter as collectionsCounter

from opensearchpy import OpenSearch

# After each mapping creation, mapping is stored in the json
_es_secret_file = 'Unified/secret_opensearch.txt'
_index_template = 'unified-logs'
_host = "es-unified1.cern.ch/es"

# Global Elastic and Open Search connections obj
_opensearch_client = None

# Global index cache, keep tracks of monthly indices that are already created with mapping for all clusters
_index_cache = set()


def get_es_client():
    """
    Creates ES and OpenSearch clients

    Use a global ES client and return it if connection still holds. Else create a new connection.
    """
    global _opensearch_client
    if not _opensearch_client:
        # reinitialize
        _opensearch_client = OpenSearchInterface()
    return _opensearch_client


class OpenSearchInterface(object):
    """Interface to elasticsearch

    secret_es.txt: "username:password"
    """

    def __init__(self, secret_file=_es_secret_file):
        try:
            logging.info("OpenSearch instance is initializing")

            username, password = open(secret_file).readline().split(':')
            username, password = username.strip(), password.strip()
            url = 'https://' + username + ':' + password + '@' + _host
            self.handle = OpenSearch(
                [url],
                verify_certs=True,
                use_ssl=True,
                ca_certs='/etc/pki/tls/certs/ca-bundle.trust.crt',
            )
        except Exception as e:
            logging.error("OpenSearchInterface initialization failed: " + str(e))

    def make_mapping(self, idx):
        """
        Creates mapping of the index

        idx: index name unified-logs-YYYY-MM-DD
        """
        body = json.dumps(self.get_index_schema())
        # Make mappings for all ES OpenSearch instances
        result = self.handle.indices.create(index=idx, body=body, ignore=400)
        if result.get("status") != 400:
            logging.warning("Creation of index %s: %s" % (idx, str(result)))
        elif "already exists" not in result.get("error", "").get("reason", ""):
            logging.error("Creation of index %s failed: %s" % (idx, str(result.get("error", ""))))

    def send_opensearch(self, idx, data, metadata=None):
        """
        Send ads in bulks to OpenSearch instance

        idx: index name unified-logs-YYYY-MM-DD
        data: can be a single document or list of documents to send
        """
        global _opensearch_client
        _opensearch_client = get_es_client()

        # If one document, make it list with one document
        if not isinstance(data, list):
            data = [data]

        body = self.make_es_body(data, metadata)
        result_n_failed = 0
        res = _opensearch_client.handle.bulk(body=body, index=idx, request_timeout=60)
        if res.get("errors"):
            result_n_failed += self.parse_errors(res)
        return result_n_failed

    @staticmethod
    def get_index(timestamp, template=_index_template):
        """
        Returns monthly index string and creates it if it does not exist.

        - It checks if index mapping is already created by checking _index_cache set.
        - And returns from _index_cache set if index exists
        - Else, it creates the index with mapping which happens in the first batch of the month ideally.
        """
        global _index_cache
        idx = time.strftime(
            "%s-%%Y-%%m" % template,
            datetime.datetime.utcfromtimestamp(timestamp).timetuple(),
        )
        if idx in _index_cache:
            return idx
        get_es_client().make_mapping(idx=idx)
        _index_cache.add(idx)
        return idx

    @staticmethod
    def make_es_body(bulk_list, metadata=None):
        """
        Prepares ES documents for bulk send by adding metadata part and separating with new line
        """
        metadata = metadata or {}
        body = ""
        for data in bulk_list:
            if metadata:
                data.setdefault("metadata", {}).update(metadata)
            body += json.dumps({"index": {}}) + "\n"
            body += json.dumps(data) + "\n"
        return body

    @staticmethod
    def parse_errors(result):
        """
        Parses bulk send result and finds errors to log
        """
        reasons = [
            d.get("index", {}).get("error", {}).get("reason", None) for d in result["items"]
        ]
        counts = collectionsCounter([_f for _f in reasons if _f])
        n_failed = sum(counts.values())
        logging.error(
            "Failed to index %d documents to ES: %s"
            % (n_failed, str(counts.most_common(3)))
        )
        return n_failed

    @staticmethod
    def get_index_schema():
        """
        Creates mapping dictionary for the unified-logs monthly index
        """
        return {
            "settings": {
                "index": {
                    "number_of_shards": "1",
                    "number_of_replicas": "1"
                }
            },
            "mappings": {
                "properties": {
                    "date": {
                        "type": "keyword"
                    },
                    "author": {
                        "type": "text"
                    },
                    "meta": {
                        "type": "keyword"
                    },
                    "subject": {
                        "type": "text"
                    },
                    "text": {
                        "type": "keyword"
                    },
                    "timestamp": {
                        "format": "epoch_second",
                        "type": "date"
                    }
                }
            }
        }

# How to use
#
# import es_opensearch
# doc = {
#     "author": "test",
#     "text": "[Release] releasing /RelValSingleElectronPt1000/CMSSW_9_4_0-94X_mc2017_realistic_v9-v1/DQMIO",
#     "meta": "level:test\n",
#     "timestamp": 1677628800,
#     "date": "Wed March 1 00:00:00 2023",
#     "subject": "test"
#   }
# opens_client = es_opensearch.get_es_client()
# idx = opens_client.get_index(doc['timestamp'])
# opens_client.send_opensearch(idx, doc)
# You can send multiple documents at the same time, just provide list of docs to send_opensearch method
