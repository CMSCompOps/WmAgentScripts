import os
import json
import csv

from Cache.DocCacheBuilder import DocCacheBuilder


class SSBProdStatusDocCache(DocCacheBuilder):
    """
    __SSBProdStatusDocCache__
    General API for building the chaching document of key ssb_prod_status
    """

    def get(self) -> dict:
        return None  # TODO: implement get_dashbssb() or drop this


class SSBCoreMaxUsedDocCache(DocCacheBuilder):
    """
    __SSBCoreMaxUsedDocCache__
    General API for building the chaching document of key ssb_core_max_used
    """

    def get(self) -> list:
        return None  # TODO: implement get_dashbssb() or drop this


class SSBCoreProductionDocCache(DocCacheBuilder):
    """
    __SSBCoreProductionDocCache__
    General API for building the chaching document of key ssb_core_production
    """

    def get(self) -> list:
        return None  # TODO: implement get_dashbssb() or drop this


class SSBCoreCpuIntensiveDocCache(DocCacheBuilder):
    """
    __SSBCoreCpuIntensiveDocCache__
    General API for building the chaching document of key ssb_core_cpu_intensive
    """

    def get(self) -> list:
        return None  # TODO: implement get_dashbssb() or drop this


class GWMSMONTotalsDocCache(DocCacheBuilder):
    """
    __GWMSMONTotalsDocCache__
    General API for building the chaching document of key gwmsmon_totals
    """

    def get(self) -> dict:
        with os.popen("curl --retry 5 -s https://cms-gwmsmon.cern.ch/poolview/json/totals") as file:
            data = json.loads(file.read())
        return data


class GWMSMONProdSiteSummaryDocCache(DocCacheBuilder):
    """
    __GWMSMONProdSiteSummaryDocCache__
    General API for building the chaching document of key gwmsmon_prod_site_summary
    """

    def get(self) -> dict:
        with os.popen("curl --retry 5 -s https://cms-gwmsmon.cern.ch/prodview//json/site_summary") as file:
            data = json.loads(file.read())
        return data


class GWMSMONProdMaxUsedDocCache(DocCacheBuilder):
    """
    __GWMSMONProdMaxUsedDocCache__
    General API for building the chaching document of key gwmsmon_prod_maxused
    """

    def get(self) -> dict:
        with os.popen("curl --retry 5 -s https://cms-gwmsmon.cern.ch/prodview//json/maxusedcpus") as file:
            data = json.loads(file.read())
        return data


class MCoreReadyDocCache(DocCacheBuilder):
    """
    __MCoreReadyDocCache__
    General API for building the chaching document of key mcore_ready
    """

    def get(self) -> dict:
        with os.popen(
            "curl --retry 5 -s http://cmsgwms-frontend-global.cern.ch/vofrontend/stage/mcore_siteinfo.json"
        ) as file:
            data = json.loads(file.read())
        return data


class DetoxSitesDocCache(DocCacheBuilder):
    """
    __DetoxSitesDocCache__
    General API for building the chaching document of key detox_sites
    """

    def get(self) -> list:
        with os.popen("curl --retry 5 -s http://t3serv001.mit.edu/~cmsprod/IntelROCCS/Detox/SitesInfo.txt") as file:
            data = file.read().split("\n")
        return data


class SiteQueuesDocCache(DocCacheBuilder):
    """
    __SiteQueuesDocCache__
    General API for building the chaching document of key site_queues
    """

    def get(self) -> dict:
        return None  # TODO: implement getNodesQueue() or drop this


class SiteStorageDocCache(DocCacheBuilder):
    """
    __SiteStorageDocCache__
    General API for building the chaching document of key site_storage
    """

    def get(self) -> dict:
        return None  # TODO: implement getSiteStorage() or drop this


class FileInvalidationDocCache(DocCacheBuilder):
    """
    __FileInvalidationDocCache__
    General API for building the chaching document of key file_invalidation
    """

    def get(self) -> dict:
        with os.popen(
            'curl -s "https://docs.google.com/spreadsheets/d/11fFsDOTLTtRcI4Q3gXw0GNj4ZS8IoXMoQDC3CbOo_2o/export?format=csv"'
        ) as file:
            data = csv.reader(file)
        return list(set(row[3].split(":")[-1] for row in data))


class WMStatsDocCache(DocCacheBuilder):
    """
    __WMStatsDocCache__
    General API for building the chaching document of key wmstats
    """

    def get(self) -> dict:
        return None  # TODO: implement getWMStats() or drop this
