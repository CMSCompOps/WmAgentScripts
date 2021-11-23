import random
from logging import Logger
from collections import defaultdict

from Cache.DataCacheLoader import DataCacheLoader
from Utilities.Logging import getLogger
from Utilities.IteratorTools import mapValues, filterKeys
from Utilities.ConfigurationHandler import ConfigurationHandler
from Services.WMStats.WMStatsReader import WMStatsReader

from typing import List, Optional, Tuple, Union


class SiteController(object):
    """
    _SiteController_
    General API for controlling the sites info
    """

    def __init__(self, overrideGoodSites: Optional[List[str]] = None, logger: Optional[Logger] = None) -> None:
        try:
            super().__init__()
            self.logger = logger or getLogger(self.__class__.__name__)

            self.unifiedConfiguration = ConfigurationHandler("config/unifiedConfiguration.json")
            self.sitesConfiguration = ConfigurationHandler("config/sitesConfiguration.json")

            self.dataCache = DataCacheLoader()
            self.wmstatsReader = WMStatsReader()

            self._setSitesConfiguration(overrideGoodSites)
            self._setAddHocStorageConfiguration()
            self.cpuPledges, self.disk = self.getCpuPledgesAndDisk()

            self.syncToSSB()
            self.syncToDetox()
            self.syncToGWMSMon()

            self.vetoTransferSites = self.getVetoTransferSites()

        except Exception as error:
            raise Exception(f"Error initializing SiteController\n{str(error)}")

    def _setSitesConfiguration(self, overrideGoodSites: Optional[List[str]] = None) -> None:
        """
        The function to set the sites configuration
        :param overrideGoodSites: optional list of sites to override
        """
        self.bannedSites = set(self.sitesConfiguration.get("sites_banned"))
        self.autoApproveSites = set(self.sitesConfiguration.get("sites_auto_approve"))
        self.allSites = set(self.sitesConfiguration.get("sites_ready"))
        self.sitesReady = set(self.sitesConfiguration.get("sites_ready"))
        self.sitesNotReady = set()

        self.sitesReadyInAgent = self.getSitesReadyInAgent()

        self._updateSitesReadyBySSBStatus(overrideGoodSites)

        self.AAASites = self.sitesReady - set(self.sitesConfiguration.get("sites_non_AAA"))
        self.AAASites.update(self.sitesConfiguration.get("sites_good_for_lightweight"))
        self.AAASites.update(self.sitesConfiguration.get("sites_good_for_premix"))

        self.EOSSites = self._filterSitesReadyOnly(self.sitesConfiguration.get("sites_eos"))
        self.goodAAASites = self._filterSitesReadyOnly(self.sitesConfiguration.get("sites_with_goodAAA"))
        self.goodIOSites = self._filterSitesReadyOnly(self.sitesConfiguration.get("sites_with_goodIO"))
        self.hepCloudSites = self._filterSitesReadyOnly(self.sitesConfiguration.get("sites_HEPCloud"))
        self.mcoreSitesReady = self._filterSitesReadyOnly(self.dataCache.get("mcore_ready").get("sites_for_mcore", []))

        for n in range(4):
            setattr(self, f"T{n}Sites", self.getTierSites(n))
            setattr(self, f"allT{n}Sites", self.getTierSites(n, readyOnly=False))

    def _setAddHocStorageConfiguration(self) -> None:
        """
        The function to set the storage configuration
        """
        self.addHocStorage = self.sitesConfiguration.get("sites_addHocStorage")

        self.addHocStorageSites = mapValues(lambda x: set([x]), self.addHocStorage)
        self.addHocStorageSites = defaultdict(set, self.addHocStorageSites)

        self.mapSEToCE, self.mapCEToSE = defaultdict(set), defaultdict(set)
        for se, ce in self.dataCache.get("site_storage"):
            self.mapSEToCE[se].add(ce)
            self.mapCEToSE[ce].add(se)

            self.addHocStorageSites[ce].add(se)
            if self.SEToCE(se) != ce and ce != "T2_CH_CERN":
                self.addHocStorage[ce] = se

    def _filterSitesReadyOnly(self, sites: Union[list, set]) -> set:
        """
        The function to filter a given list of sites to only ready sites
        :param sites: list of sites
        :return: filtered list of sites, containing only ready sites
        """
        return set(sites) & self.sitesReady

    def _updateSitesReadyBySSBStatus(self, overrideGoodSites: Optional[List[str]] = None) -> None:
        """
        The function to update the list of sites ready and not ready based on SSB status
        :param overrideGoodSites: optional list of sites to override
        """
        for info in self.dataCache.get("ssb_prod_status"):
            site = info.get("name")
            self.allSites.add(site)

            override = overrideGoodSites and site in overrideGoodSites
            if not override and site in self.bannedSites:
                continue

            if override or site in self.sitesReadyInAgent:
                self.sitesReady.add(site)
            elif site not in self.sitesReadyInAgent:
                self.sitesNotReady.add(site)
            elif site.get("prod_status") == "enabled":
                self.sitesReady.add(site)
            else:
                self.sitesNotReady.add(site)

    def _pickOne(self, sites: list, weights: dict) -> str:
        """
        The function to pick a site from a given list
        :param sites: list of sites
        :param weights: dict of sites' weights
        :return: a site name
        """
        sitesWeights = {} if sites else weights
        for site in sites:
            sitesWeights[site] = weights.get(site, 0)

        return list(sitesWeights.keys())[self._chooseWeightedIndex(sitesWeights.values())]

    def _chooseWeightedIndex(self, weights: list) -> int:
        """
        The function to get a random index for the given weights
        :param weights: weights list
        :return: index
        """
        rand = random.random() * (sum(weights) or len(weights))

        for i, weight in enumerate(weights):
            rand -= weight
            if rand <= 0:
                return i

        self.logger.info("Could not make a choice from %s and %s", weights, rand)
        return None

    def getCpuPledgesAndDisk(self) -> Tuple[dict, dict]:
        """
        The function to get the sites' cpu pledges and disk
        :return: cpu pledges and disk
        """
        try:
            cpuPledges, disk = defaultdict(int), defaultdict(int)
            for site in self.allSites:
                cpuPledges[site] = 1
                disk[self.CEToSE(site)] = 0

            return cpuPledges, disk

        except Exception as error:
            self.logger.error("Failed to get cpu pledges and disk")
            self.logger.error(str(error))

    def getSitesReadyInAgent(self) -> set:
        """
        The function to get the sites ready in the production agents
        :return: sites ready in agent
        """
        try:
            sites = set()

            prodAgents = self.wmstatsReader.getProductionAgents() or {}
            for agent in prodAgents.values():
                if agent.get("status") != "ok":
                    continue

                for site, info in agent.get("WMBS_INFO", {}).get("thresholds", {}).items():
                    if info.get("state") == "Normal":
                        sites.add(site)

            return sites

        except Exception as error:
            self.logger.error("Failed to get sites ready in agent")
            self.logger.error(str(error))

    def getTierSites(self, n: int, readyOnly: bool = True) -> set:
        """
        The function to get the tier sites
        :param n: tier number
        :param readyOnly: if True include only sites ready, include all o/w
        :return: tier sites
        """
        try:
            sites = self.sitesReady if readyOnly else self.allSites
            return set(site for site in sites if site.startswith(f"T{n}_"))

        except Exception as error:
            self.logger.error("Failed to get tier %d sites", n)
            self.logger.error(str(error))

    def getVetoTransferSites(self) -> set:
        """
        The function to get the list of sites to veto transfer
        :return: veto transfer sites
        """
        try:
            vetoTransferSites = set()
            for site, free in self.disk.items():
                if free <= 0:
                    vetoTransferSites.add(site)

            return vetoTransferSites

        except Exception as error:
            self.logger.error("Failed to get veto transfer sites")
            self.logger.error(str(error))

    def getTotalDisk(self) -> int:
        """
        The function to get the total disk
        :return: total disk
        """
        try:
            total = 0
            for site in self.sitesReady:
                total += self.disk.get(self.CEToSE(site), 0)

            return total

        except Exception as error:
            self.logger.error("Failed to get total disk")
            self.logger.error(str(error))

    def getSitesByMemory(self, maxMemory: float, maxCore: int = 1) -> List[str]:
        """
        The function to get the sites for a given memory
        :param maxMemory: max memory
        :param maxCore: max cores
        :return: list of allowed sites
        """
        try:
            if not self.sitesMemory:
                self.logger.info("No memory information from GWMSMon")
                return None

            allowedSites = set()
            for site, memorySlots in self.sitesMemory.items():
                if any(slot["MaxMemMB"] >= maxMemory and slot["MaxCpus"] >= maxCore for slot in memorySlots):
                    allowedSites.add(site)

            return list(allowedSites)

        except Exception as error:
            self.logger.error("Failed to get the sites by memory")
            self.logger.error(str(error))

    def SEToCE(self, se: str) -> str:
        """
        The function to map SE to CE
        :param se: SE name
        :return: CE name
        """
        try:
            if se in self.mapSEToCE:
                return sorted(self.mapSEToCE[se])[0]
            if se.endswith("_Disk"):
                return se.replace("_Disk", "")
            if se.endswith("_MSS"):
                return se.replace("_MSS", "")
            return se

        except Exception as error:
            self.logger.error("Failed to map SE to CE")
            self.logger.error(str(error))

    def SEToCEs(self, se: str) -> list:
        """
        The function to map SE to list of CE
        :param se: SE name
        :return: CE names
        """
        try:
            if se in self.mapSEToCE:
                return [*sorted(self.mapSEToCE[se])]
            return [self.SEToCE(se)]

        except Exception as error:
            self.logger.error("Failed to map SE to CE")
            self.logger.error(str(error))

    def CEToSE(self, ce: str) -> str:
        """
        The function to map CE to SE
        :param ce: CE name
        :return: SE name
        """
        try:
            if (ce.startswith("T1") or ce.startswith("T0")) and not ce.endswith("_Disk"):
                return ce + "_Disk"
            if ce in self.mapCEToSE:
                return sorted(self.mapCEToSE[ce])[0]
            if ce in self.addHocStorage:
                return self.addHocStorage[ce]
            return ce

        except Exception as error:
            self.logger.error("Failed to map CE to SE")
            self.logger.error(str(error))

    def CEToSEs(self, ce: Union[str, list]) -> list:
        """
        The function to map CE to list of SE
        :param ce: CE name
        :return: SE names
        """
        try:
            if isinstance(ce, list) or isinstance(ce, set):
                return list(set(self.addHocStorageSites[item] for item in ce))
            return list(self.addHocStorageSites.get(ce, []))

        except Exception as error:
            self.logger.error("Failed to map CE to SE")
            self.logger.error(str(error))

    def pickSE(self, sites: list) -> str:
        """
        The function to get a SE
        :param sites: sites list
        :return: a random SE name
        """
        try:
            return self._pickOne(sites, self.disk)

        except Exception as error:
            self.logger.error("Failed to pick SE")
            self.logger.error(str(error))

    def pickCE(self, sites: list) -> str:
        """
        The function to get a CE
        :param sites: sites list
        :return: a random CE name
        """
        try:
            return self._pickOne(sites, self.cpuPledges)

        except Exception as error:
            self.logger.error("Failed to pick CE")
            self.logger.error(str(error))

    def syncToSSB(self) -> None:
        """
        The function to sync sites data to SSB
        """
        try:
            self.logger.info("================= syncToSSB started =================")
            ssbInfo = {}

            ssb = {"realCPU": "core_max_used", "prodCPU": "core_production", "CPUbound": "core_cpu_intensive"}
            for key, value in ssb.items():
                ssbInfo[key] = self.dataCache.get(f"ssb_{value}")

            infoBySite = defaultdict(dict)
            for key, info in ssbInfo.items():
                for item in info:
                    site = item.get("name")
                    if site.startswith("T3"):
                        continue
                    infoBySite[site][key] = item[ssb[key]]

            for site, info in infoBySite.items():
                cpuPledge = self.cpuPledges.get(site, 0)
                cpuBound = int(info.get("CPUbound", 0))

                if cpuPledge < cpuBound or self.cpuPledge > 1.5 * cpuBound:
                    self.cpuPledges[site] = cpuBound

                    self.logger.info("%s could use %s instead of %s for CPU", site, cpuBound, cpuPledge)

        except Exception as error:
            self.logger.error("Failed to sync SSB info")
            self.logger.error(str(error))

    def syncToDetox(self, bufferLevel: float = 0.8) -> None:
        """
        The function to sync to detox info
        """
        try:
            detoxInfo = self.dataCache.get("detox_sites")
            if len(detoxInfo) < 15:
                detoxInfo = self.dataCache.get("detox_sites", fresh=True)
                if len(detoxInfo) < 15:
                    self.logger.info("Detox info is gone")
                    return

            self.freeDisk = defaultdict(int)
            self.quota = defaultdict(int)
            self.locked = defaultdict(int)

            read = False
            overrideSiteSpace = self.unifiedConfiguration.get("sites_space_override")
            for info in detoxInfo:
                if "Partition:" in info:
                    read = "DataOps" in info
                    continue
                if info.startswith("#") or not read:
                    continue

                _, quota, _, locked, site = info.split()
                if "MSS" in site:
                    continue

                availableSpace = int(quota * bufferLevel) - int(locked)
                if availableSpace < 0:
                    availableSpace = 0

                freeSpace = int(quota) - int(locked) - availableSpace
                if freeSpace < 0:
                    freeSpace = 0

                self.quota[site] = int(quota)
                self.locked[site] = int(locked)
                self.disk[site] = availableSpace
                self.freeDisk[site] = freeSpace

                if overrideSiteSpace.get(site):
                    self.disk[site] = overrideSiteSpace.get(site)

        except Exception as error:
            self.logger.error("Failed to sync to detox dataops report")
            self.logger.error(str(error))

    def syncToGWMSMon(self) -> None:
        """
        The function to sync to GWMS Mon
        """
        try:
            self.sitesMemory = self.dataCache.get("gwmsmon_totals")
            self.sitesMemory = filterKeys(self.sitesReady, self.sitesMemory)

            maxUsedSites = self.dataCache.get("gwmsmon_prod_maxused")
            for site in filterKeys(maxUsedSites.keys(), self.cpuPledges):
                self.cpuPledges[site] = int(maxUsedSites[site]["sixdays"])

            self.sitesPressure = {}
            sitesSummary = self.dataCache.get("gwmsmon_prod_site_summary")
            for site in filterKeys(sitesSummary.keys(), self.cpuPledges):
                cpusInUse = float(sitesSummary[site].get("CpusInUse", 0))
                cpusPending = float(sitesSummary[site].get("CpusPending", 0))
                pressure = cpusPending / cpusInUse if cpusInUse else -1

                self.sitesPressure[site] = (cpusPending, cpusInUse, pressure)

        except Exception as error:
            self.logger.error("Failed to sync to GWMS Mon data")
            self.logger.error(str(error))
