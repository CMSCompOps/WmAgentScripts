"""
File       : DBSReader.py
Author     : Hasan Ozturk <haozturk AT cern dot com>
Description: General API for reading data from DBS
"""

import logging
import os
from collections import defaultdict
from dbs.apis.dbsClient import DbsApi

from Utils.ConfigurationHandler import ConfigurationHandler
from Utils.Decorators import runWithMultiThreading
from Services.Mongo.MongoInfo import CacheInfo

from typing import Optional, List, Tuple, Union


class DBSReader(object):
    """
    _DBSReader_
    General API for reading data from DBS
    """

    def __init__(
        self,
        url: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
        **contact,
    ):
        try:
            if url:
                self.dbsUrl = url.replace("cmsweb.cern.ch", "cmsweb-prod.cern.ch")
            else:
                configurationHandler = ConfigurationHandler()
                self.dbsUrl = os.getenv(
                    "DBS_READER_URL", configurationHandler.get("dbs_url")
                )
            self.dbs = DbsApi(self.dbsUrl, **contact)
            self.cache = CacheInfo()
            logging.basicConfig(level=logging.INFO)
            self.logger = logger or logging.getLogger(self.__class__.__name__)

        except Exception as e:
            msg = "Error in DBSReader with DbsApi\n"
            msg += f"{e}\n"
            raise Exception(msg)

    @runWithMultiThreading
    def _getFileLumiArray(self, filenames: List[str], run: int) -> List[dict]:
        # TODO: test if it is working properly with mt
        """
        The function to get the lumi arrays for a given set of file names and run
        :param filename: lumi logical file names
        :param run: run name
        :return: a list of lumi arrays

        This function runs by default with multithreading and a list of
        dicts, e. g. [{'filename': filename, 'run': run}] must be given as input.
        """
        try:
            return (
                self.dbs.listFileLumiArray(logical_file_name=filenames, run_num=run)
                if run != 1
                else self.dbs.listFileLumiArray(logical_file_name=filenames)
            )

        except Exception as error:
            self.logger.error("Failed to get lumi array files")
            self.logger.error(str(error))

    @runWithMultiThreading
    def _getBlockFileLumis(self, block: str, validFileOnly: bool = True) -> dict:
        # TODO: test if it is working properly with mt
        """
        The function to get lumi files from a given block
        :param block: block name
        :param validFileOnly: if True, keeps only valid files, keep all o/w
        :return: lumi files

        This function runs by default with multithreading and a list of
        dicts, e. g. [{'block': block}] must be given as input.
        """
        try:
            return self.dbs.listFileLumis(
                block_name=block, validFileOnly=int(validFileOnly)
            )

        except Exception as error:
            self.logger.error("Failed to get files from DBS for block %s", block)
            self.logger.error(str(error))

    def getDBSStatus(self, dataset: str) -> str:
        """
        The function to get the DBS status of a given dataset
        :param dataset: dataset name
        :return: DBS status
        """
        try:
            response = self.dbs.listDatasets(
                dataset=dataset, dataset_access_type="*", detail=True
            )
            dbsStatus = response[0]["dataset_access_type"]
            self.logger.info(f"{dataset} is {dbsStatus}")
            return dbsStatus

        except Exception as error:
            self.logger.error(
                "Exception while getting the status of following dataset on DBS: %s",
                dataset,
            )
            self.logger.error(str(error))

    def getFilesWithLumiInRun(self, dataset: str, run: int) -> List[dict]:
        """
        The function to get the files with lumi for a given dataset and run
        :param dataset: dataset name
        :param run: run name
        :return: a list of files with lumi
        """
        try:
            result = (
                self.dbs.listFiles(
                    dataset=dataset, detail=True, run_num=run, validFileOnly=1
                )
                if run != 1
                else self.dbs.listFiles(dataset=dataset, detail=True, validFileOnly=1)
            )
            filenames = [file["logical_file_name"] for file in result]
            querySize = 100
            return self._getFileLumiArray(
                [
                    {"filenames": filenames[i : i + querySize], "run": run}
                    for i in range(0, len(filenames), querySize)
                ]
            )

        except Exception as error:
            self.logger.error(
                "Failed to get files for dataset %s and run %s",
                dataset,
                run,
            )
            self.logger.error(str(error))

    def getBlockName(self, filename: str) -> str:
        """
        The function to get the block name for a given file
        :param filename: file name
        :return: block name
        """
        try:
            result = self.dbs.listFileArray(logical_file_name=filename, detail=True)
            return result[0]["block_name"]

        except Exception as error:
            self.logger.error("Failed to get block name from DBS for file %s", filename)
            self.logger.error(str(error))

    def getDatasetFiles(
        self,
        dataset: str,
        validFileOnly: bool = False,
        details: bool = False,
    ) -> List[dict]:
        """
        The function to get the files for a given dataset
        :param dataset: dataset name
        :param validFileOnly: if True, keep only valid files, keep all o/w
        :param details: if True, returns details for each file, just file names and validity o/w
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
                self.cache.store(cacheKey, files)

            if validFileOnly:
                files = [file for file in files if file["is_file_valid"]]

            if not details:
                keysToKeep = ["logical_file_name", "is_file_valid"]
                files = list(filterDictsByKeyList(keysToKeep, *files))

            return files

        except Exception as error:
            self.logger.error(
                "Failed to get file array from DBS for dataset %s", dataset
            )
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
            self.logger.error(
                "Failed to get block names from DBS for dataset %s",
                dataset,
            )
            self.logger.error(str(error))

    def getDatasetBlockNamesByRuns(self, dataset: str, runs: list) -> List[str]:
        """
        The function to get the block names of a given dataset and runs
        :param dataset: dataset name
        :param runs: run names
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
            self.logger.error(
                "Failed to get block names from DBS for dataset %s",
                dataset,
            )
            self.logger.error(str(error))

    def getDatasetBlockNamesByLumis(self, dataset: str, lumis: dict) -> List[str]:
        # TODO: check type of lumis
        # TODO: needed a fix in here :/
        """
        The function to get the block names of a given dataset and runs
        :param dataset: dataset name
        :param lumis: dict relating run name and lumi list
        :return: a list of block names
        """
        try:
            blocks = set()
            for run, lumiList in lumis.items():
                if int(run) != 1:
                    result = self.dbs.listFileArray(
                        dataset=dataset,
                        lumi_list=lumiList,
                        run_num=int(run),
                        detail=True,
                    )
                else:
                    # NOTE: dbs api does not support run_num=1 w/o logical_file_name
                    # To avoid the exception, in this case make the call with filename insteads of lumis
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
            self.logger.error(
                "Failed to get block names from DBS for dataset %s",
                dataset,
            )
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

    def getDatasetEventsAndLumis(self, dataset: str) -> Tuple[int, int]:
        """
        The function to get number of events and lumis for a given dataset
        :param dataset: dataset name
        :return: number of events and lumis
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
        The function to get number of events and lumis for given blocks
        :param blocks: blocks names
        :return: number of events and lumis
        """
        try:
            files = []
            for block in blocks:
                files.extend(
                    self.dbs.listFileSummaries(block_name=block, validFileOnly=1)
                )
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
        :return: a list of runs
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
        :return: a list of dataset parents names
        """
        try:
            result = self.dbs.listDatasetParents(dataset=dataset)
            return [item.get("parent_dataset") for item in result]

        except Exception as error:
            self.logger.error("Failed to get parents from DBS for dataset %s", dataset)
            self.logger.error(str(error))

    def getDatasetNames(self, dataset: str) -> List[dict]:
        """
        The function to get the datasets matching a given dataset name
        :param dataset: dataset name
        :return: a list of dicts with dataset names
        """
        try:
            _, datasetName, processedName, tierName = dataset.split("/")
            result = self.dbs.listDatasets(
                primary_ds_name=datasetName,
                processed_ds_name=processedName,
                data_tier_name=tierName,
                dataset_access_type="*",
            )
            return result

        except Exception as error:
            self.logger.error("Failed to get info from DBS for dataset %s", dataset)
            self.logger.error(str(error))

    def getLFNBase(self, dataset: str) -> str:
        """
        The function to get the logical file name base for a given dataset
        :param dataset: dataset name
        :return: logical file name base
        """
        try:
            result = self.dbs.listFiles(dataset=dataset)
            filename = result[0]["logical_file_name"]
            return "/".join(filename.split("/")[:3])

        except Exception as error:
            self.logger.error("Failed to get LFN base from DBS for dataset %s", dataset)
            self.logger.error(str(error))

    def getRecoveryBlocks(self, filesAndLocations: dict) -> Tuple[list, dict]:
        """
        The function to get the blocks needed for the recovery of a workflow
        :param filesAndLocations: dict of files names and locations in DBS
        :return: all blocks and locations,
        """
        try:
            blocks = set()
            blocksAndLocations = defaultdict(set)
            cacheBlockFiles = defaultdict(str)
            for filename, location in filesAndLocations.items():
                if filename in cacheBlockFiles:
                    blockName = cacheBlockFiles[filename]
                else:
                    blockName = self.getBlockName(filename)
                    if blockName:
                        filesArrays = self.getDatasetFileArray(
                            blockName.split("#")[0], details=True
                        )
                        for fileArray in filesArrays:
                            cacheBlockFiles[fileArray["logical_file_name"]] = fileArray[
                                "block_name"
                            ]
                            blocks.add(fileArray["block_name"])
                blocksAndLocations[blockName].update(location)
            return blocks, blocksAndLocations

        except Exception as error:
            self.logger.error("Failed to recovery blocks from DBS")
            self.logger.error(str(error))

    def getDatasetLumisAndFiles(
        self, dataset: str, validFileOnly: bool = True
    ) -> Tuple[dict, dict]:
        """
        The function to get the lumis and files of a given dataset
        :param dataset: dataset name
        :param validFileOnly: if True, keep only valid files, keep all o/w
        :return: a dict in the format {run: [lumis]} and a dict in the format {(run:lumis): [files]}
        """
        try:
            cacheKey = f"json_lumis_{dataset}"
            cached = self.cache.get(cacheKey)
            if cached:
                self.logger.info("lumis of %s taken from cache", dataset)
                lumisByRun, filesByLumis = cached["lumis"], cached["files"]
            else:
                blocks = self.dbs.listBlocks(dataset=dataset)
                lumisByRun, filesByLumis = self.getBlocksLumisAndFilesForCaching(
                    blocks, validFileOnly
                )
                self.cache.store(
                    cacheKey,
                    {"files": filesByLumis, "lumis": lumisByRun},
                    lifeTimeMinutes=600,
                )

            lumisByRun = dict((int(k), v) for k, v in lumisByRun.items())
            filesByLumis = dict(
                (tuple(map(int, k.split(":"))), v) for k, v in filesByLumis.items()
            )
            return lumisByRun, filesByLumis

        except Exception as error:
            self.logger.error(
                "Failed to get lumis and files from DBS for dataset %s", dataset
            )
            self.logger.error(str(error))

    def getBlocksLumisAndFilesForCaching(
        self, blocks: List[dict], validFileOnly: bool = True
    ) -> Tuple[dict, dict]:
        """
        The function to get the lumis and files of given blocks
        :param blocks: blocks
        :param validFileOnly: if True, keep only valid files, keep all o/w
        :return: a dict in the format {run: [lumis]} and a dict in the format {(run:lumis): [files]}
        """
        filesByLumis, lumisByRun = defaultdict(set), defaultdict(set)
        files = self._getBlockFileLumis(
            [
                {"block": block.get("block_name"), "validFileOnly": validFileOnly}
                for block in blocks
            ]
        )
        for file in files:
            runKey = str(file["run_num"])
            lumisByRun[runKey].update(file["lumi_section_num"])
            for lumiKey in file["lumi_section_num"]:
                filesByLumis[f"{runKey}:{lumiKey}"].add(file["logical_file_name"])

        lumisByRun = dict((k, list(v)) for k, v in lumisByRun.items())
        filesByLumis = dict((k, list(v)) for k, v in filesByLumis.items())
        return lumisByRun, filesByLumis


# TODO: MOVE TO ANOTHER MODULE (MAYBE ONE FOR DATA CLEANING?)
def filterDictsByKeyList(lst: list, data: dict, *otherData: dict) -> dict:
    """
    The function to filter dict data by a given list of keys to keep
    :param lst: key values to keep
    :param data/otherData: dicts
    :return: filtered data (keeping the input order)
    """
    filteredData = []
    for d in [data] + list(otherData):
        filteredData.append(
            dict(
                (k, v)
                for k, v in d.items()
                if k in lst or (isinstance(k, tuple) and k[0] in lst)
            )
        )
    return tuple(filteredData) if len(filteredData) > 1 else filteredData[0]


def filterLumisAndFilesByRuns(
    filesByLumis: dict, lumisByRun: dict, runs: list
) -> Tuple[dict, dict]:
    """
    The function to get the lumis and files filteres by runs
    :param filesByLumis: a dict in the format {run: [lumis]}
    :param lumisByRun: a dict in the format {(run:lumis): [files]}
    :param run: run names
    :return: a dict in the format {run: [lumis]} and a dict in the format {(run:lumis): [files]}
    """
    return filterDictsByKeyList(runs, lumisByRun, filesByLumis)


def filterLumisAndFilesByLumis(
    filesByLumis: dict, lumisByRun: dict, lumis: dict
) -> Tuple[dict, dict]:
    """
    The function to get the lumis and files filteres by lumis
    :param filesByLumis: a dict in the format {run: [lumis]}
    :param lumisByRun: a dict in the format {(run:lumis): [files]}
    :param lumis: a dict in the format {run: lumis}
    :return: a dict in the format {run: [lumis]} and a dict in the format {(run:lumis): [files]}
    """
    runs = map(int, lumis.keys())
    lumis = set((k, v) for k, v in lumis.items())
    lumisByRun = filterDictsByKeyList(runs, lumisByRun)
    filesByLumis = filterDictsByKeyList(lumis, filesByLumis)
    return lumisByRun, filesByLumis


def getRecoveryFilesAndLocations(
    recoveryDocs: List[dict], suffixTaskFilter: Optional[str] = None
) -> dict:
    """
    The function to get the files and locations of given recovery docs
    :param recoveryDocs: recovery docs
    :param suffixTaskFilter: filter tasks ending with given suffix
    :return: a dict of files and locations
    """
    filesAndLocations = defaultdict(set)
    for doc in recoveryDocs:
        task = doc.get("fileset_name", "")
        if suffixTaskFilter and not task.endswith(suffixTaskFilter):
            continue

        for filename in doc["files"]:
            filesAndLocations[filename].update(doc["files"][filename]["locations"])
        else:
            filesAndLocations[filename].update([])

    print(f"{len(filesAndLocations)} files in recovery")

    return dict((k, list(v)) for k, v in filesAndLocations.items())


def splitFilesInAndNotInBDS(filesAndLocations: dict) -> Tuple[dict, dict]:
    """
    The function to split the files in a subset of files in DBS and not in DBS
    :param filesAndLocations: dict of files and locations
    :return: two dicts of files and locations
    """
    filesInDBS, filesNotInDBS = set(), set()
    for filename in filesAndLocations:
        if any(
            filename.startswith(strg) for strg in ["/store/unmerged/", "MCFakeFile-"]
        ):
            filesNotInDBS.add(filename)
        else:
            filesInDBS.add(filename)

    inDBS = filterDictsByKeyList(filesInDBS, filesAndLocations)
    notInDBS = filterDictsByKeyList(filesNotInDBS, filesAndLocations)
    return inDBS, notInDBS
