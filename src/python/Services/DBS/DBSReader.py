"""
File       : DBSReader.py
Author     : Hasan Ozturk <haozturk AT cern dot com>
Description: General API for reading data from DBS
"""

import os
from logging import Logger
from collections import defaultdict
from dbs.apis.dbsClient import DbsApi

from Utilities.ConfigurationHandler import ConfigurationHandler
from Utilities.IteratorTools import mapValues, mapKeys, filterKeys
from Utilities.Logging import getLogger
from Utilities.Decorators import runWithMultiThreading, runWithRetries
from Cache.CacheManager import CacheManager

from typing import Optional, List, Tuple, Union


class DBSReader(object):
    """
    _DBSReader_
    General API for reading data from DBS
    """

    def __init__(self, url: Optional[str] = None, logger: Optional[Logger] = None, **contact):
        try:
            super().__init__()
            self.logger = logger or getLogger(self.__class__.__name__)

            if url:
                self.dbsUrl = url.replace("cmsweb.cern.ch", "cmsweb-prod.cern.ch")
            else:
                configurationHandler = ConfigurationHandler()
                self.dbsUrl = os.getenv("DBS_READER_URL", configurationHandler.get("dbs_url"))
            self.dbs = DbsApi(self.dbsUrl, **contact)
            self.cache = CacheManager()

        except Exception as error:
            raise Exception(f"Error initializing DBSReader\n{str(error)}")

    def check(self) -> bool:
        """
        The function to check if dbs is responding
        :return: True if it is responding, False o/w
        """
        try:
            if "testbed" in self.dbsUrl:
                checkDataset = "/QDTojWinc_NC_M-1200_TuneZ2star_8TeV-madgraph/Summer12pLHE-DMWM_Validation_DONOTDELETE_Alan_TEST-v1/GEN"
            else:
                checkDataset = (
                    "/TTJets_mtop1695_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/RunIIWinter15GS-MCRUN2_71_V1-v1/GEN-SIM"
                )
            response = self.dbs.listBlockSummaries(dataset=checkDataset, detail=True)
            if not response:
                raise Exception("DBS corrupted")
            return True

        except Exception as error:
            self.logger.error("Failed to get any response from DBS")
            self.logger.error(str(error))
            return False

    @runWithMultiThreading(mtParam="filenames")
    @runWithRetries(default=[])
    def _getFileLumiArray(self, filenames: Union[List[str], List[List[str]]], run: int) -> List[dict]:
        """
        The function to get the lumi section arrays for a given set of file names in given run
        :param filename: logical file names
        :param run: run number
        :return: a list of lumi section arrays

        This function runs by default with multithreading on the param "filenames". The filenames can
        be a list of str, or a list of lists of str for querying DBS for multiple filenames
        at once.
        """
        return (
            self.dbs.listFileLumiArray(logical_file_name=filenames, run_num=run)
            if run != 1
            else self.dbs.listFileLumiArray(logical_file_name=filenames)
        )

    @runWithMultiThreading(mtParam="blocks")
    @runWithRetries(default=[])
    def _getBlockFileLumis(self, blocks: List[str], validFileOnly: bool = True) -> List[dict]:
        """
        The function to get lumi section files from a given block
        :param blocks: blocks names
        :param validFileOnly: if True, keeps only valid files, keep all o/w
        :return: lumi sections files

        This function runs by default with multithreading on the param blocks, which is a
        list of block names.
        """
        return self.dbs.listFileLumis(block_name=blocks, validFileOnly=int(validFileOnly))

    def getDBSStatus(self, dataset: str) -> str:
        """
        The function to get the DBS status of a given dataset
        :param dataset: dataset name
        :return: DBS status
        """
        try:
            response = self.dbs.listDatasets(dataset=dataset, dataset_access_type="*", detail=True)
            dbsStatus = response[0]["dataset_access_type"]
            self.logger.debug(f"{dataset} is {dbsStatus}")
            return dbsStatus

        except Exception as error:
            self.logger.error("Exception while getting the status of following dataset on DBS: %s", dataset)
            self.logger.error(str(error))

    def getFilesWithLumiInRun(self, dataset: str, run: int) -> List[dict]:
        """
        The function to get the files with lumi sections for a given dataset in a given run
        :param dataset: dataset name
        :param run: run number
        :return: a list of files with lumi sections
        """
        try:
            result = (
                self.dbs.listFiles(dataset=dataset, detail=True, run_num=run, validFileOnly=1)
                if run != 1
                else self.dbs.listFiles(dataset=dataset, detail=True, validFileOnly=1)
            )
            filenames = [file["logical_file_name"] for file in result]

            querySize = 100
            queryFilesList = [filenames[i : i + querySize] for i in range(0, len(filenames), querySize)]
            return self._getFileLumiArray(filenames=queryFilesList, run=run)

        except Exception as error:
            self.logger.error("Failed to get files for dataset %s and run %s", dataset, run)
            self.logger.error(str(error))

    def getBlockName(self, filename: str) -> str:
        """
        The function to get the block name for a given file
        :param filename: logical file name
        :return: block name
        """
        try:
            result = self.dbs.listFileArray(logical_file_name=filename, detail=True)
            return result[0]["block_name"]

        except Exception as error:
            self.logger.error("Failed to get block name from DBS for file %s", filename)
            self.logger.error(str(error))

    def getDatasetFiles(self, dataset: str, validFileOnly: bool = False, details: bool = False) -> List[dict]:
        """
        The function to get the files for a given dataset
        :param dataset: dataset name
        :param validFileOnly: if True, keep only valid files, keep all o/w
        :param details: if True, returns details for each file, o/w only keep file names and validity
        :return: a list of files
        """
        try:
            cacheKey = f"dbs_listFile_{dataset}"
            cached = self.cache.get(cacheKey)
            if cached:
                self.logger.info("listFile of %s taken from cache", dataset)
                files = cached
            else:
                files = self.dbs.listFiles(dataset=dataset, detail=True)
                self.logger.info("Caching listFile of %s", dataset)
                self.cache.set(cacheKey, files)

            if validFileOnly:
                files = [file for file in files if file["is_file_valid"]]

            if files and not details:
                keysToKeep = ["logical_file_name", "is_file_valid"]
                files = list(filterKeys(keysToKeep, *files))

            return files

        except Exception as error:
            self.logger.error("Failed to get file array from DBS for dataset %s", dataset)
            self.logger.error(str(error))

    def getDatasetBlockNames(self, dataset: str) -> List[str]:
        """
        The function to get the block names of a given dataset
        :param dataset: dataset name
        :return: a list of block names
        """
        try:
            result = self.dbs.listBlocks(dataset=dataset)
            blocks = set()
            blocks.update(block["block_name"] for block in result)
            return list(blocks)

        except Exception as error:
            self.logger.error("Failed to get block names from DBS for dataset %s", dataset)
            self.logger.error(str(error))

    def getDatasetBlockNamesByRuns(self, dataset: str, runs: list) -> List[str]:
        """
        The function to get the block names of a given dataset in the given runs
        :param dataset: dataset name
        :param runs: run numbers
        :return: a list of block names
        """
        try:
            blocks = set()
            for run in map(int, runs):
                result = (
                    self.dbs.listBlocks(dataset=dataset, run_num=run)
                    if run != 1
                    else self.dbs.listBlocks(dataset=dataset)
                )
                blocks.update(block["block_name"] for block in result)
            return list(blocks)

        except Exception as error:
            self.logger.error("Failed to get block names from DBS for dataset %s", dataset)
            self.logger.error(str(error))

    def getDatasetBlockNamesByLumis(self, dataset: str, lumisByRun: dict) -> List[str]:
        """
        The function to get the block names of a given dataset in the given lumi sections
        :param dataset: dataset name
        :param lumisByRun: a dict of format {run: [lumis]}
        :return: a list of block names
        """
        try:
            blocks = set()
            for run, lumiList in lumisByRun.items():
                if int(run) != 1:
                    result = self.dbs.listFileArray(
                        dataset=dataset,
                        lumi_list=lumiList,
                        run_num=int(run),
                        detail=True,
                    )
                else:
                    # NOTE: dbs api does not support run_num=1 w/o defining a logical_file_name
                    # To avoid the exception, in this case make the call with filenames instead of lumis
                    files = self.getDatasetFiles(dataset)
                    filenames = [file["logical_file_name"] for file in files]
                    result = self.dbs.listFileArray(
                        dataset=dataset,
                        logical_file_names=filenames,
                        run_num=int(run),
                        detail=True,
                    )
                blocks.update(block["block_name"] for block in result)
            return list(blocks)

        except Exception as error:
            self.logger.error("Failed to get block names from DBS for dataset %s", dataset)
            self.logger.error(str(error))

    def getDatasetSize(self, dataset: str) -> float:
        """
        The function to get the size (in terms of GB) of a given dataset
        :param dataset: dataset name
        :return: dataset size
        """
        try:
            blocks = self.dbs.listBlockSummaries(dataset=dataset, detail=True)
            return sum([block["file_size"] for block in blocks]) / (1024.0 ** 3)

        except Exception as error:
            self.logger.error("Failed to get size of dataset %s from DBS", dataset)
            self.logger.error(str(error))

    def getDatasetEventsPerLumi(self, dataset: str) -> float:
        """
        The function to get the number of events per lumi
        :param dataset: dataset name
        :return: events per lumi
        """
        try:
            events, lumis = self.getDatasetEventsAndLumis(dataset)
            return events / float(lumis) if lumis else 0.0

        except Exception as error:
            self.logger.error("Failed to get events per lumis from DBS")
            self.logger.error(str(error))

    def getDatasetEventsAndLumis(self, dataset: str) -> Tuple[int, int]:
        """
        The function to get the total number of events and lumi sections for a given dataset
        :param dataset: dataset name
        :return: total number of events and of lumi sections
        """
        try:
            files = self.dbs.listFileSummaries(dataset=dataset, validFileOnly=1)
            events = sum([file["num_event"] for file in files if file is not None])
            lumis = sum([file["num_lumi"] for file in files if file is not None])
            return events, lumis

        except Exception as error:
            self.logger.error("Failed to get events and lumis from DBS")
            self.logger.error(str(error))

    def getBlocksEventsAndLumis(self, blocks: List[str]) -> Tuple[int, int]:
        """
        The function to get the total number of events and lumi sections for given blocks
        :param blocks: blocks names
        :return: total number of events and of lumi sections
        """
        try:
            files = []
            for block in blocks:
                files.extend(self.dbs.listFileSummaries(block_name=block, validFileOnly=1))
            events = sum([file["num_event"] for file in files if file is not None])
            lumis = sum([file["num_lumi"] for file in files if file is not None])
            return events, lumis

        except Exception as error:
            self.logger.error("Failed to get events and lumis from DBS")
            self.logger.error(str(error))

    def getDatasetRuns(self, dataset: str) -> List[int]:
        """
        The function to get the runs for a given dataset
        :param dataset: dataset name
        :return: a list of run numbers
        """
        try:
            result = self.dbs.listRuns(dataset=dataset)
            runs = []
            for run in result:
                if isinstance(run["run_num"], list):
                    runs.extend(run["run_num"])
                else:
                    runs.append(run["run_num"])
            return runs

        except Exception as error:
            self.logger.error("Failed to get runs from DBS for dataset %s", dataset)
            self.logger.error(str(error))

    def getDatasetParent(self, dataset: str) -> List[str]:
        """
        The function to get the parent dataset of a given dataset
        :param dataset: dataset name
        :return: a list of parent names
        """
        try:
            result = self.dbs.listDatasetParents(dataset=dataset)
            return [item.get("parent_dataset") for item in result]

        except Exception as error:
            self.logger.error("Failed to get parents from DBS for dataset %s", dataset)
            self.logger.error(str(error))

    def getDatasetNames(self, dataset: str, details: bool = True) -> List[dict]:
        """
        The function to get the datasets matching a given dataset name
        :param dataset: dataset name
        :param details: return dataset info if True, o/w return only names
        :return: list of datasets
        """
        try:
            _, datasetName, processedName, tierName = dataset.split("/")
            result = self.dbs.listDatasets(
                primary_ds_name=datasetName,
                processed_ds_name=processedName,
                data_tier_name=tierName,
                dataset_access_type="*",
            )

            if details:
                return result
            return [item["dataset"] for item in result]

        except Exception as error:
            self.logger.error("Failed to get info from DBS for dataset %s", dataset)
            self.logger.error(str(error))

    def getLFNBase(self, dataset: str) -> str:
        """
        The function to get the base of logical file names for a given dataset
        :param dataset: dataset name
        :return: base of logical file names
        """
        try:
            result = self.dbs.listFiles(dataset=dataset)
            filename = result[0]["logical_file_name"]
            return "/".join(filename.split("/")[:3])

        except Exception as error:
            self.logger.error("Failed to get LFN base from DBS for dataset %s", dataset)
            self.logger.error(str(error))

    def getRecoveryBlocksAndLocations(self, filesAndLocations: dict) -> Tuple[list, dict]:
        """
        The function to get the blocks needed for the recovery of a workflow
        :param filesAndLocations: dict of file names and locations
        :return: all blocks in DBS and their location
        """
        try:
            blocks = set()
            blocksAndLocations = defaultdict(set)
            cachedBlockFiles = defaultdict(str)
            for filename, location in filesAndLocations.items():
                if filename in cachedBlockFiles:
                    blockName = cachedBlockFiles[filename]
                else:
                    blockName = self.getBlockName(filename)
                    if blockName is None:
                        continue
                    files = self.getDatasetFiles(blockName.split("#")[0], details=True)
                    for file in files:
                        cachedBlockFiles[file["logical_file_name"]] = file["block_name"]
                        blocks.add(file["block_name"])
                blocksAndLocations[blockName].update(location)

            blocksAndLocations = mapValues(list, blocksAndLocations)
            return list(blocks), blocksAndLocations

        except Exception as error:
            self.logger.error("Failed to recovery blocks from DBS")
            self.logger.error(str(error))

    def getDatasetLumisAndFiles(
        self, dataset: str, validFileOnly: bool = True, withCache: bool = True
    ) -> Tuple[dict, dict]:
        """
        The function to get the lumi sections and files of a given dataset
        :param dataset: dataset name
        :param validFileOnly: if True, keep only valid files, o/w keep all
        :param withCache: if True, get cached data, o/w build from blocks
        :return: a dict in the format {run: [lumis]} and a dict in the format {(run:lumis): [files]}
        """
        try:
            cacheKey = f"json_lumis_{dataset}"
            cached = self.cache.get(cacheKey)
            if withCache and cached:
                self.logger.info("json_lumis of %s taken from cache", dataset)
                lumisByRun, filesByLumis = cached["lumis"], cached["files"]
            else:
                blocks = self.dbs.listBlocks(dataset=dataset)
                lumisByRun, filesByLumis = self.getBlocksLumisAndFilesForCaching(blocks, validFileOnly)
                self.logger.info("Caching json_lumis of %s", dataset)
                self.cache.set(
                    cacheKey,
                    {"files": filesByLumis, "lumis": lumisByRun},
                    lifeTimeMinutes=600,
                )

            lumisByRun = mapKeys(int, lumisByRun)
            filesByLumis = mapKeys(lambda k: tuple(map(int, k.split(":"))), filesByLumis)
            return lumisByRun, filesByLumis

        except Exception as error:
            self.logger.error("Failed to get lumi sections and files from DBS for dataset %s", dataset)
            self.logger.error(str(error))

    def getBlocksLumisAndFilesForCaching(self, blocks: List[dict], validFileOnly: bool = True) -> Tuple[dict, dict]:
        """
        The function to get the lumi sections and files of given blocks
        :param blocks: blocks
        :param validFileOnly: if True, keep only valid files, keep all o/w
        :return: a dict in the format {'run': [lumis]} and a dict in the format {'run:lumis': [files]}, where the keys are strings
        """
        try:
            filesByLumis, lumisByRun = defaultdict(set), defaultdict(set)

            queryBlocksList = [block.get("block_name") for block in blocks]
            files = self._getBlockFileLumis(blocks=queryBlocksList, validFileOnly=validFileOnly)
            for file in files:
                runKey = str(file["run_num"])
                lumisByRun[runKey].update(file["lumi_section_num"])
                for lumiKey in file["lumi_section_num"]:
                    filesByLumis[f"{runKey}:{lumiKey}"].add(file["logical_file_name"])

            lumisByRun = mapValues(list, lumisByRun)
            filesByLumis = mapValues(list, filesByLumis)
            return lumisByRun, filesByLumis

        except Exception as error:
            self.logger.error("Failed to get lumi sections and files from DBS for given blocks")
            self.logger.error(str(error))
