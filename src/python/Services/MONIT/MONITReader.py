import os
import json
from logging import Logger

from Utilities.Logging import getLogger
from Utilities.Decorators import runWithRetries

from typing import Optional


class MONITReader(object):
    """
    __MONITReader__
    General API for reading data from MONIT monitoring service
    """

    def __init__(self, logger: Optional[Logger] = None) -> None:
        try:
            super().__init__()
            self.logger = logger or getLogger(self.__class__.__name__)

            with open("config/monit_secret.json", "r") as file:
                self.config = json.load(file.read())

            self.ssbQuery = f'curl -s -X POST {self.config.get("url")} -H "Authorization: Bearer {self.config.get("token")}" -H "Content-Type: application/json" -d '

        except Exception as error:
            raise Exception(f"Error initializing MONITReader\n{str(error)}")

    def _getDashbssbTimestamp(self, path: str) -> dict:
        """
        The function to get the dashbssb timestamp
        :param path: path name
        :return: dash ssb timestamp
        """
        queryMain = {
            "search_type": "query_then_fetch",
            "ignore_unavailable": True,
            "index": ["monit_prod_cmssst_*", "monit_prod_cmssst_*"],
        }
        queryDetails = {
            "size": 1,
            "query": {
                "bool": {
                    "filter": [
                        {"range": {"metadata.timestamp": {"gte": "now-2d", "lte": "now", "format": "epoch_millis"}}},
                        {
                            "query_string": {
                                "analyze_wildcard": True,
                                "query": f"metadata.type: ssbmetric AND metadata.type_prefix:raw AND metadata.path: {path}",
                            }
                        },
                    ]
                }
            },
            "sort": {"metadata.timestamp": {"order": "desc", "unmapped_type": "boolean"}},
            "script_fields": {},
            "docvalue_fields": ["metadata.timestamp"],
        }

        with os.popen(self.ssbQuery + json.loads(queryMain) + json.loads(queryDetails)) as file:
            result = json.loads(file.read())

        return result["responses"][0]["hits"]["hits"][0]["_source"]["metadata"]["timestamp"]

    @runWithRetries(default={})
    def getDashbssb(self, path: str, metric: str) -> dict:
        """
        The function to get the dashbssb
        :param path: path name
        :param metric: ssb metric name
        :return: dash ssb timestamp
        """
        queryMain = {
            "search_type": "query_then_fetch",
            "ignore_unavailable": True,
            "index": ["monit_prod_cmssst_*", "monit_prod_cmssst_*"],
        }
        queryDetails = {
            "size": 500,
            "_source": {"includes": ["data.name", f"data.{metric}"]},
            "query": {
                "bool": {
                    "filter": [
                        {"range": {"metadata.timestamp": {"gte": "now-2d", "lte": "now", "format": "epoch_millis"}}},
                        {
                            "query_string": {
                                "analyze_wildcard": True,
                                "query": f"metadata.type: ssbmetric AND metadata.type_prefix:raw AND metadata.path: {path} AND metadata.timestamp: {self._getDashbssbTimestamp(path)}",
                            }
                        },
                    ]
                }
            },
            "sort": {"metadata.timestamp": {"order": "desc", "unmapped_type": "boolean"}},
            "script_fields": {},
            "docvalue_fields": ["metadata.timestamp"],
        }

        with os.popen(self.ssbQuery + json.dumps(queryMain) + json.dumps(queryDetails)) as file:
            result = json.loads(file.read())

        data = result["responses"][0]["hits"]["hits"]
        data = [item["_source"]["data"] for item in result]
        if data:
            return data

        raise Exception("getDashbssb returned an empty collection")
