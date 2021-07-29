import os
import json
import math
import logging
from logging import Logger
from collections import defaultdict
from time import mktime, gmtime

from Components.Campaign.CampaignController import CampaignController
from Components.Workload.WorkloadInterface import WorkloadInterface

from Services.ReqMgr.ReqMgrReader import ReqMgrReader
from Services.DBS.DBSReader import DBSReader
from Services.Rucio.RucioReader import RucioReader
from Services.WMStats.WMStatsReader import WMStatsReader
from Services.WorkQueue.WorkQueueReader import WorkQueueReader
from Services.ACDC.ACDCReader import ACDCReader
from Services.GWMSMon.GWMSMonReader import GWMSMonReader

from Utilities.ConfigurationHandler import ConfigurationHandler
from Utilities.IteratorTools import mapValues
from Utilities import DataTools


from typing import Optional, Tuple, List, Union


class WorkflowController(object):
    """
    __WorkflowController__
    General API for controlling the workflows info
    """

    def __init__(self, wf: str, logger: Optional[Logger] = None, **kwargs) -> None:
        try:
            super().__init__()
            self.unifiedConfiguration = ConfigurationHandler("unifiedConfiguration.json")

            configurationHandler = ConfigurationHandler()
            self.cacheDirectory = configurationHandler.get("cache_dir")

            self.acdcReader = ACDCReader()
            self.dbsReader = DBSReader()
            self.gwmsReader = GWMSMonReader()
            self.reqmgrReader = ReqMgrReader()
            self.rucioReader = RucioReader()
            self.wmstatsReader = WMStatsReader()
            self.wqReader = WorkQueueReader()

            self.request = WorkloadInterface(wf, kwargs.get("request"))
            self.campaignController = CampaignController()
            self.siteInfo = None  # TODO: implement siteInfo

            self.wf = wf
            self.spec = None if kwargs.get("noSpec") else self.reqmgrReader.getSpec(wf)
            self.workQueue = self.wqReader.getWorkQueue(wf) if kwargs.get("workQueue") else None
            self.wmstats = self.getWMStats(wf) if kwargs.get("wmstats") else None
            self.wmerrors = self.getWMErrors(wf) if kwargs.get("errors") else None

            self.recoveryDocs = []
            self.summary = None

            logging.basicConfig(level=logging.INFO)
            self.logger = logger or logging.getLogger(self.__class__.__name__)

        except Exception as error:
            raise Exception(f"Error initializing WorkflowController\n{str(error)}")

    def _getFromFile(self, filename: str) -> Optional[dict]:
        """
        The function to get data from cache file
        :param filename: cache file name
        :return: cached data if any, None o/w
        """
        try:
            filePath = f"{self.cacheDirectory}/{filename}"
            if os.path.isfile(filePath):
                self.logger.info("File cache hit %s", filePath)
                with open(filePath) as file:
                    cached = json.loads(file.read())
                return cached

            self.logger.info("File cache miss %s", filePath)
            return None

        except Exception as error:
            self.logger.error("Failed to get %s from file cache", filename)
            self.logger.error(str(error))

    def _saveInFile(self, filename: str, data: dict) -> None:
        """
        The function to save data in cache file
        :param filename: cache file name
        :param data: data to be cached
        """
        try:
            filePath = f"{self.cacheDirectory}/{filename}"
            with open(filePath, "w") as file:
                file.write(json.dumps(data))

        except Exception as error:
            self.logger.error("Failed to save data in file cache %s", filename)
            self.logger.error(str(error))

    def _getAllowedSites(self) -> set:
        """
        The function to get the allowed sites
        :return: site white list
        """
        allowedSites = set()

        lhe, _, _, secondaries = self.request.getIO()
        if lhe:
            return allowedSites + sorted(self.siteInfo.EOSSites)

        if secondaries and self.isHeavyToRead(secondaries):
            for secondary in secondaries:
                allowedSites.update(self.rucioReader.getDatasetLocationsByAccount(secondary, "wmcore_transferor"))
            return allowedSites

        sites = ["T0Sites", "T1Sites", "GoodAAASites" if secondaries else "T2Sites"]
        for site in sites:
            allowedSites.update(getattr(self.siteInfo, site))

        if self.request.includeHEPCloudInSiteWhiteList:
            allowedSites.update(self.siteInfo.HEPCloudSites)
            self.logger.info("Including HEPCloud in the site white list of %s", self.wf)

        return allowedSites

    def _restrictAllowedSitesByBlowUpFator(self, allowedSites: set) -> set:
        """
        The function to restrict a site white list by the blow up factor
        :param allowedSites: site white list
        :return: new site white list
        """
        blowUp = self.getBlowupFactor()
        maxBlowUp, neededCores = self.unifiedConfiguration.get("blow_up_limits")

        if blowUp > maxBlowUp:
            allowedSitesWithNeededCores = set(
                [site for site in allowedSites if self.siteInfo.cpuPledges[site] > neededCores]
            )

            if allowedSitesWithNeededCores:
                self.logger.info(
                    "Restricting site white list because of blow-up factors: %s > %s",
                    blowUp,
                    maxBlowUp,
                )

                return allowedSitesWithNeededCores

        return allowedSites

    def _restrictAllowedSitesByCampaign(self, allowedSites: set) -> Tuple[set, set]:
        """
        The function to restrict a site white list by the campaigns' site lists
        :param allowedSites: site white list
        :return: new site white list and site black list
        """
        notAllowedSites = set()

        for campaign in self.request.getCampaigns(details=False):
            campaignParam = self.campaignController.getCampaignParameters(campaign)

            allowedCampaignSites = campaignParam.get("SiteWhitelist")
            if allowedCampaignSites:
                self.logger.info("Restricting site white list by campaign %s", campaign)
                allowedSites = allowedSites & allowedCampaignSites or allowedSites

            notAllowedCampaignSites = campaignParam.get("SiteBlacklist")
            if notAllowedCampaignSites:
                self.logger.info("Restricting site white list by black list in campaign %s", campaign)
                notAllowedSites.update(sorted(notAllowedCampaignSites))
                allowedSites -= notAllowedSites

        return allowedSites, notAllowedSites

    def _parseOutputProcessingString(self, processingString: str) -> Tuple[str, str]:
        """
        The function to parse a processing string
        :param processing string: processing string
        :return: parsed acquisition era and processing string
        """
        parsedAcquisitionEra, parsedProcessingString = "*", "*"
        if processingString.count("-") == 2:
            parsedAcquisitionEra, parsedProcessingString, _ = processingString.split("-")
        elif processingString.count("-") == 3:
            parsedAcquisitionEra, _, parsedProcessingString, _ = processingString.split("-")

        if parsedAcquisitionEra in ["None", "FAKE"]:
            acquisitionEra = self.request.getAcquisitionEra()
            self.logger.info("%s has no acquisition era, using %s", processingString, acquisitionEra)
            parsedAcquisitionEra = acquisitionEra

        if parsedProcessingString == "None":
            self.logger.info("%s has no processing string, using wildcard char instead", processingString)
            parsedProcessingString = "*"

        return parsedAcquisitionEra, parsedProcessingString

    def _getVersionByWildcardPattern(self, version: Optional[int] = 0) -> Optional[int]:
        """
        The function to get the version by searching the datasets matching a wildcard pattern
        :param version: current version number if known
        :return: version number
        """
        outputDatasets = self.request.get("OutputDatasets", [])

        for dataset in outputDatasets:
            _, name, processingString, tier = dataset.split("/")
            acquisitionEra, processingString = self._parseOutputProcessingString(processingString)

            pattern = self.request.writeDatasetPatternName([name, acquisitionEra, processingString, "v*", tier])
            if not pattern:
                return None

            matches = self.dbsReader.getDatasetNames(pattern)
            self.logger.info("Found %s datasets matching %s", len(matches), pattern)
            for match in matches:
                _, _, matchProcessingString, _ = match.split("/")
                version = max(version, int(matchProcessingString.split("-")[-1].replace("v", "")))

        return version

    def _getVersionByConflictingWorkflows(self, version: Optional[int] = 0) -> int:
        """
        The function to get the version by searching through conflicting workflow versions
        :param version: current version if known
        :return: version number
        """
        outputDatasets = self.request.get("OutputDatasets", [])

        for dataset in outputDatasets:
            _, name, processingString, tier = dataset.split("/")
            acquisitionEra, processingString = self._parseOutputProcessingString(processingString)

            while True:
                expectedName = self.request.writeDatasetPatternName(
                    [name, acquisitionEra, processingString, f"v{version+1}", tier]
                )
                if not expectedName:
                    return version

                conflictingWfs = self.reqmgrReader.getWorkflowsByOutput(expectedName)
                conflictingWfs = [wf for wf in conflictingWfs if wf != self.wf]
                if not conflictingWfs:
                    break

                self.logger.info("There is an output conflict for %s with: %s", self.wf, conflictingWfs)
                version += 1

        return version

    def isHeavyToRead(self, secondaries: Union[list, dict]) -> bool:
        """
        The function to check if it is heavy to read the secondaries
        :param secondaries: secondaries dataset names
        :return: True if minbias appears in secondary, False o/w
        """
        return any("minbias" in secondary.lower() for secondary in secondaries)

    def getRecoveryBlocks(self, suffixTaskFilter: Optional[str] = None) -> Tuple[list, dict, dict, dict]:
        """
        The function to get the blocks needed for the recovery of a workflow
        :param suffixTaskFilter: filter tasks ending with given suffix
        :return: a list of blocks found in DBS, a dict of the blocks locations, a dict of the files locations
        whose blocks were found in DBS, and a dict of the files locations whose blocks were not found in DBS
        """
        try:
            if not self.recoveryDocs:
                self.recoveryDocs = self.acdcReader.getRecoveryDocs(self.wf) or []

            filesAndLocations = DataTools.filterRecoveryFilesAndLocations(self.recoveryDocs, suffixTaskFilter)
            filesAndLocations, filesAndLocationsWoBlocks = DataTools.filterFilesAndLocationsInDBS(filesAndLocations)

            blocks, blocksAndLocations = self.dbsReader.getRecoveryBlocksAndLocations(filesAndLocations)

            return blocks, blocksAndLocations, filesAndLocations, filesAndLocationsWoBlocks

        except Exception as error:
            self.logger.error("Failed to get recovery blocks")
            self.logger.error(str(error))

    def getRecoveryInfo(self) -> Tuple[dict, dict, dict]:
        """
        The function to get the recovery info
        :return: a dict of task locations, a dict of missing tasks to run, and a dict of missing tasks locations
        """
        try:
            if not self.recoveryDocs:
                self.recoveryDocs = self.acdcReader.getRecoveryDocs(self.wf) or []

            whereToRun = defaultdict(set)
            missingToRun = defaultdict(int)
            whereIsMissingToRun = defaultdict(lambda: defaultdict(int))

            for doc in self.recoveryDocs:
                task = doc.get("fileset_name", "")
                for filename, data in doc.get("files").items():
                    whereToRun[task].update(
                        self.request.get("SiteWhiteList")
                        if filename.startswith("MCFakeFile")
                        else data.get("locations", [])
                    )

                    missingToRun[task] += data.get("events")
                    for location in data.get("locations", []):
                        whereIsMissingToRun[task][location] += data.get("events")

            whereToRun = mapValues(list, whereToRun)
            whereIsMissingToRun = mapValues(dict, whereIsMissingToRun)

            return whereToRun, dict(missingToRun), whereIsMissingToRun

        except Exception as error:
            self.logger.error("Failed to get recovery info")
            self.logger.error(str(error))

    def getWMErrors(self, cacheLastUpdateLimit: int = 0) -> dict:
        """
        The function to get the WMErrors for the workflow
        :param cacheLastUpdateLimit: limit of seconds since a cache file creation for considering it valid
        :return: WMErrors
        """
        try:
            now = mktime(gmtime())
            cacheFile = f"{self.wf}.wmerror"

            if cacheLastUpdateLimit:
                cached = self._getFromFile(cacheFile)
                if cached and now - cached.get("timestamp") < cacheLastUpdateLimit:
                    self.logger.info("WMErrors taken from cache: %s", cacheFile)
                    return cached.get("data")

            wmerrors = self.wmerrors or self.wmstatsReader.getWMErrors(self.wf)
            self._saveInFile(cacheFile, {"timestamp": int(now), "data": wmerrors})

            return wmerrors

        except Exception as error:
            self.logger.error("Failed to get WMErrors")
            self.logger.error(str(error))

    def getWMStats(self, cacheLastUpdateLimit: int = 0) -> dict:
        """
        The function to get the WMStats for the workflow
        :param cacheLastUpdateLimit: limit of seconds since a cache file creation for considering it valid
        :return: WMStats
        """
        try:
            now = mktime(gmtime())
            cacheFile = f"{self.wf}.wmstats"

            if cacheLastUpdateLimit:
                cached = self._getFromFile(cacheFile)
                if cached and now - cached.get("timestamp") < cacheLastUpdateLimit:
                    self.logger.info("WMStats taken from cache: %s", cacheFile)
                    return cached.get("data")

            wmstats = self.wmstats or self.wmstatsReader.getWMStats(self.wf)
            self._saveInFile(cacheFile, {"timestamp": int(now), "data": wmstats})

            return wmstats

        except Exception as error:
            self.logger.error("Failed to get WMStats")
            self.logger.error(str(error))

    def getFamily(self, details: bool = True) -> list:
        """
        The function to get the request family
        :param details: return all family member data if True, o/w return a list of names
        :return: request family
        """
        try:
            family = self.reqmgrReader.getWorkflowsByPrepId(self.request.get("PrepID"), details=True)

            family = [
                member
                for member in family
                if member.get("RequestDate") >= self.request.get("RequestDate")
                and str(member.get("RequestStatus")) != "None"
            ]

            if details:
                return family
            return [member.get("RequestName") for member in family]

        except Exception as error:
            self.logger.error("Failed to get family")
            self.logger.error(str(error))

    def getTasks(self, **selectParam) -> list:
        """
        The function to get all tasks in the workflow
        :param selectParam: optional task selection params
        :return: list of tasks
        """
        try:
            spec = self.spec or self.reqmgrReader.getSpec(self.wf)

            allTasks = []
            for task in spec.tasks.tasklist:
                taskSpec = getattr(spec.tasks, task)
                allTasks.extend(DataTools.flattenTaskTree(taskSpec, **selectParam))

            return allTasks

        except Exception as error:
            self.logger.error("Failed to get tasks")
            self.logger.error(str(error))

    def getWorkTasks(self) -> list:
        """
        The function to get the work tasks in the workflow
        :return: list of work tasks
        """
        return self.getTasks(taskType=["Production", "Processing", "Skim"])

    def getFirstTask(self):
        """
        The function to get the first task
        :return: first task
        """
        return (self.spec or self.reqmgrReader.getSpec(self.wf)).tasks.tasklist[0]

    def getOutputDatasetsPerTask(self) -> dict:
        """
        The function to get the output datasets by task
        :return: a dict of dataset names by task names
        """
        return self.request.getOutputDatasetsPerTask(self.getWorkTasks())

    def getCampaignByTask(self, task: str) -> str:
        """
        The function to get the campaign for a given task
        :param task: task name
        :return: campaign
        """
        return self.request.getParamByTask("Campaign", task)

    def getMemoryByTask(self, task: str) -> int:
        """
        The function to get the memory used by task
        :param task: task name
        :return: memory
        """
        return int(self.request.getParamByTask("Memory", task) or 0)

    def getCoreByTask(self, task: str) -> int:
        """
        The function to get the cores used by task
        :param task: task name
        :return: number of cores
        """
        return int(self.request.getParamByTask("Multicore", task) or 1)

    def getFilterEfficiencyByTask(self, task: str) -> float:
        """
        The function to get the filter efficiency by task
        :param task: task name
        :return: filter efficiency
        """
        return float(self.request.getParamByTask("FilterEfficiency", task) or 1)

    def getLumiWhiteList(self) -> list:
        """
        The function to get the workflow's lumi white list
        :return: lumi white list
        """
        return self.request.getParamList("LumiList")

    def getBlockWhiteList(self) -> list:
        """
        The function to get the workflow's block white list
        :return: block white list
        """
        return self.request.getParamList("BlockWhitelist")

    def getRunWhiteList(self) -> list:
        """
        The function to get the workflow's run white list
        :return: run white list
        """
        return self.request.getParamList("RunWhitelist")

    def getSiteWhiteList(self, pickOne: bool = False) -> Tuple[list, list]:
        """
        The function to get the site white list
        :param pickOne: pick one site from CE list, o/w keep all sites
        :return: site white list, site black list
        """
        try:
            allowedSites = self._getAllowedSites()
            if pickOne:
                allowedSites = set(sorted(self.siteInfo.pickCE(allowedSites)))

            self.logger.info("Initially allow %s", allowedSites)

            allowedSites = self._restrictAllowedSitesByBlowUpFator(allowedSites)
            allowedSites, notAllowedSites = self._restrictAllowedSitesByCampaign(allowedSites)

            self.logger.info("Allowed sites: %s", allowedSites)
            self.logger.info("Not allowed sites: %s", notAllowedSites)
            return allowedSites, notAllowedSites

        except Exception as error:
            self.logger.error("Failed to get site white list")
            self.logger.error(str(error))

    def getPrepIds(self) -> list:
        """
        The function to get the workflow prep ids
        :return: list of prep ids
        """
        return self.request.getParamList("PrepID")

    def getScramArches(self) -> list:
        """
        The function to get the scram arches
        :return: scram arches
        """
        return self.request.getParamList("ScramArch")

    def getComputingTime(self, unit: str = "h") -> float:
        """
        The function to get the computing time
        :param unit: time unit — s, m, h or d. Non valid units will return computing time in seconds
        :return: computing time
        """
        try:
            div = 60.0 if unit == "m" else 3600.0 if unit == "h" else 86400.0 if unit == "d" else 1.0
            return self.request.getComputingTime() / div

        except Exception as error:
            self.logger.error("Failed to get computing time")
            self.logger.error(str(error))

    def getBlocks(self) -> List[str]:
        """
        The function to get all blocks
        :return: list of block names
        """
        try:
            _, primaries, _, _ = self.request.getIO()
            blocks = set(self.getBlockWhiteList())

            runs = self.getRunWhiteList()
            if runs:
                blocks.update(self.dbsReader.getDatasetBlockNamesByRuns(primary, runs) for primary in primaries)

            lumis = self.getLumiWhiteList()
            if lumis:
                blocks.update(self.dbsReader.getDatasetBlockNamesByLumis(primary, lumis) for primary in primaries)

            return list(blocks)

        except Exception as error:
            self.logger.error("Failed to get blocks")
            self.logger.error(str(error))

    def getAgents(self) -> dict:
        """
        The function to get the workflow's agents
        :return: agents
        """
        try:
            agents = defaultdict(lambda: defaultdict(int))

            workQueue = self.workQueue or self.wqReader.getWorkQueue(self.wf)
            workers = [worker.get(worker.get("type")) for worker in workQueue]

            for status in set([worker.get("Status") for worker in workers]):
                statusWorkers = [worker for worker in workers.get("Status") == status]
                for statusWorker in statusWorkers:
                    agents[status][statusWorker.get("ChildQueueUrl")] += 1

            return mapValues(dict, agents)

        except Exception as error:
            self.logger.error("Failed to get the agents")
            self.logger.error(str(error))

    def getSplittings(self) -> List[dict]:
        """
        The function to get the splittings for the workflow tasks
        :return: a list of dicts
        """
        try:
            keysToKeep = [
                "events_per_lumi",
                "events_per_job",
                "lumis_per_job",
                "halt_job_on_file_boundaries",
                "max_events_per_lumi",
                "halt_job_on_file_boundaries_event_aware",
            ]
            algorithmsToKeep = {
                "EventAwareLumiBased": {"halt_job_on_file_boundaries_event_aware": "True"},
                "LumiBased": {"halt_job_on_file_boundaries": "True"},
            }
            algorithmsToTranslate = {"EventAwareLumiBased": {"events_per_job": "avg_events_per_job"}}

            splittings = []
            for task in self.getWorkTasks():
                taskSplitting = task.input.splitting
                splitting = {"splittingAlgo": taskSplitting.algorithm, "splittingTask": task.pathName}

                if taskSplitting.algorithm in algorithmsToKeep:
                    splitting.update(algorithmsToKeep[taskSplitting.algorithm])

                for key in keysToKeep:
                    if hasattr(taskSplitting, key):
                        splittingsKey = key
                        if taskSplitting.algorithm in algorithmsToTranslate:
                            splittingsKey = algorithmsToTranslate[taskSplitting.algorithm].get(key) or key
                        splitting.update({splittingsKey: getattr(taskSplitting, key)})

                splittings.append(splitting)

            return splittings

        except Exception as error:
            self.logger.error("Failed to get splittings")
            self.logger.error(str(error))

    def getSplittingsSchema(self, strip: bool = False, allTasks: bool = False) -> List[dict]:
        """
        The function to get splittings schema for the workflow
        :param strip: if True it will drop some split params, o/w it will keep all params
        :param allTasks: if True it will keep all tasks types, o/w it will keep only production, processing and skim tasks
        :return: a list of dicts
        """
        try:
            splittings = self.reqmgrReader.getSplittingsSchema(self.wf)

            if not allTasks:
                splittings = DataTools.filterSplittingsTaskTypes(splittings)
            if strip:
                splittings = DataTools.filterSplittingsParam(splittings)

            return splittings

        except Exception as error:
            self.logger.error("Failed to get splittings schema")
            self.logger.error(str(error))

    def getConfigCacheID(self) -> dict:
        """
        The function to get the cache id configuration
        :return: cache id configuration
        """
        try:
            config = {}
            for task in self.getWorkTasks():
                name = task.pathName.split("/")[-1]
                configId = task.steps.cmsRun1.application.configuration.configId
                config[name] = configId

            return config

        except Exception as error:
            self.logger.error("Failed to get cache id configuration")
            self.logger.error(str(error))

    def getBlowupFactor(self) -> float:
        """
        The function to get the blow up factor
        :return: blow up
        """
        return self.request.getBlowupFactor(self.getSplittings())

    def getCompletionFraction(self) -> dict:
        """
        The function to get the completion fraction of the output datasets
        :return: a dict of dataset names by completion fraction
        """
        try:
            percentCompletion = defaultdict(float)

            expectedLumis = self.request.get("TotalInputLumis", 0)
            expectedEventsPerTask = self.request.getExpectedEventsPerTask()

            tasksPerOutput = self.request.getTasksPerOutputDatasets(self.getWorkTasks()) or {}

            for dataset in self.request.get("OutputDatasets", []):
                events, lumis = self.dbsReader.getDatasetEventsAndLumis(dataset)
                if expectedLumis:
                    percentCompletion[dataset] = lumis / expectedLumis
                    self.logger.info("%s with lumi completion of %s of %s", dataset, lumis, expectedLumis)

                datasetExpectedEvents = expectedEventsPerTask.get(tasksPerOutput.get(dataset, "NoTaskFound"))
                if datasetExpectedEvents:
                    eventFraction = events / datasetExpectedEvents
                    if eventFraction > percentCompletion[dataset]:
                        percentCompletion[dataset] = eventFraction
                        self.logger.info(
                            "Overriding: %s with event completion of %s of %s", dataset, events, datasetExpectedEvents
                        )

            return dict(percentCompletion)

        except Exception as error:
            self.logger.error("Failed to get completion fraction")
            self.logger.error(str(error))

    def getNCopies(self, CPUh: float, m: int = 2, M: int = 3, w: int = 50000, C0: int = 100000) -> int:
        """
        The function to get the number of needed copies based on the computing time
        :param CPUh: computing hours
        :return: number of required copies
        """
        try:
            sigmoid = lambda x: 1 / (1 + math.exp(-x))
            f = sigmoid(-C0 / w)
            D = (M - m) / (1 - f)
            O = (f * M - m) / (f - 1)

            return int(O + D * sigmoid((CPUh - C0) / w))

        except Exception as error:
            self.logger.error("Failed to compute needed copies")
            self.logger.error(str(error))

    def getNextVersion(self) -> int:
        """
        The function to get the next processing version
        :return: next version
        """
        try:
            version = max(0, int(self.request.get("ProcessingVersion", 0)) - 1)
            version = self._getVersionByWildcardPattern(version)
            version = self._getVersionByConflictingWorkflows(version)

            return version + 1

        except Exception as error:
            self.logger.error("Failed to get next version")
            self.logger.error(str(error))

    def getGlideWMSMonSummary(self):
        """
        The function to get the glide mon summary
        :return: glide mon summary
        """
        return self.gwmsReader.getRequestSummary(self.wf)

    def getSummary(self) -> dict:
        """
        The function to get the workflow request summary
        :return: summary
        """
        if not self.summary:
            self.summary = self.reqmgrReader.getWorkloadSummary(self.wf)
        return self.summary

    def checkSplittingsSize(self) -> Tuple[bool, list]:
        """
        The function to check the splittings
        :return: if to hold and a list of modified splittings
        """
        return self.request.checkSplittingsSize(self.getSplittingsSchema(strip=True))

    def go(self, silent: bool = False) -> bool:
        """
        The function to check if a workflow is allowed to go
        :param silent: if True no logs are sent
        :return: True if allowed to go, False o/w
        """
        try:
            campaignsAndLabels = self.request.getCampaignsAndLabels()
            for campaign, label in campaignsAndLabels:
                if "pilot" in label.lower():
                    if not silent:
                        self.logger.info(
                            "pilot keyword in processing string %s in campaign %s, assigning the workflow",
                            label,
                            campaign,
                        )
                    return True

            if "pilot" in self.request.get("SubRequestType", ""):
                if not silent:
                    self.logger.info("pilot keyword in SubRequestType, assigning the workflow")
                return True

            for campaign, label in campaignsAndLabels:
                if not self.campaignController.go(campaign, label):
                    if not silent:
                        self.logger.info("No go due to %s, %s", campaign, label)
                    return False

            return True

        except Exception as error:
            self.logger.error("Failed to check if allowed to go or not")
            self.logger.error(str(error))
