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
from Utils.Decorators import runWithThreads
from Services.MongoInfo.MongoInfo import CacheInfo

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
    ) -> None:
        try:
            if url:
                # TODO: keep this ?
                self.dbsUrl = url.replace("cmsweb.cern.ch", "cmsweb-prod.cern.ch")
            else:
                configurationHandler = ConfigurationHandler()
                self.dbsUrl = os.getenv(
                    "DBS_READER_URL", configurationHandler.get("dbs_url")
                )
            self.dbs = DbsApi(self.dbsUrl, **contact)
            self.cache = CacheInfo()
            self.logger = logger or logging.getLogger(self.__class__.__name__)

        except Exception as e:
            msg = "Error in DBSReader with DbsApi\n"
            msg += f"{e}\n"
            raise Exception(msg)

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
        The function to get the lumi arrays for a given dataset and run
        :param dataset: dataset name
        :param run: run name
        :return: a list of lumi arrays
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
            return self._getLumiArrays(
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

    @runWithThreads
    def _getLumiArrays(self, filenames: List[str], run: int) -> List[dict]:
        # TODO: test if it is working properly with the wrapper
        # TODO: rename ?
        """
        The function to get the lumi arrays for a given set of file names and run
        :param filename: lumi logical file names
        :param run: run name
        :return: a list of lumi arrays

        This function runs with threads
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

    def getDatasetFileArray(
        self,
        dataset: str,
        validFileOnly: bool = False,
        details: bool = False,
    ) -> List[dict]:
        """
        The function to get the file array for a given dataset
        :param dataset: dataset name
        :param validFileOnly: if True, keep only valid files, keep all o/w
        :param details: if True, returns details for each file, just file names and validity o/w
        :return: a list of file dicts
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
                files = [self._filterKeysByList(file, keysToKeep) for file in files]

            return files

        except Exception as error:
            self.logger.error(
                "Failed to get file array from DBS for dataset %s", dataset
            )
            self.logger.error(str(error))

    def getDatasetBlocks(self, dataset: str, runs=None, lumis=None):
        # TODO: what does this function return ?
        # TODO: keep returning an empty list in case of error ?
        # TODO: try to simplify this
        """
        The function to get the blocks of a given dataset
        :param dataset: dataset name
        :param runs: run names (type?)
        :param lumis: lumi section run names (type?)
        :return: a list of block names (???)
        """
        blocks = set()
        if lumis:
            self.logger.info("Entering a heavy check on block per lumi")
            for run in lumis:
                # TODO: fix when run is 1 ? Exception in listFileArray 'Invalid input: files API does not supprt run_num=1 without logical_file_name.'
                try:
                    files = (
                        self.dbs.listFileArray(
                            dataset=dataset,
                            lumi_list=lumis[run],
                            run_num=int(run),
                            detail=True,
                        )
                        if run != 1
                        else self.dbs.listFileArray(
                            dataset=dataset,
                            lumi_list=lumis[run],
                            detail=True,
                        )
                    )
                    self.logger.info(
                        "Retrieved %s files for dataset %s, lumi %s, run %s",
                        len(files),
                        dataset,
                        lumis[run],
                        int(run),
                    )
                    blocks.update([file["block_name"] for file in files])

                except Exception as error:
                    self.logger.error(
                        "Failed to get file array from DBS for dataset %s, lumi %s, run %s",
                        dataset,
                        lumis[run],
                        int(run),
                    )
                    self.logger.error(str(error))

        elif runs:
            for run in runs:
                # TODO: check behavior of old version, 'cause it raises an exception
                try:
                    blocks.update(
                        [
                            block["block_name"]
                            for block in self.dbs.listBlocks(
                                dataset=dataset, run_num=int(run)
                            )
                        ]
                    )

                except Exception as error:
                    self.logger.error(
                        "Failed to get blocks from DBS for dataset %s, run %s",
                        dataset,
                        int(run),
                    )
                    self.logger.error(str(error))

        else:
            try:
                blocks.update(
                    [
                        block["block_name"]
                        for block in self.dbs.listBlocks(dataset=dataset)
                    ]
                )

            except Exception as error:
                self.logger.error(
                    "Failed to get blocks from DBS for dataset %s", dataset
                )
                self.logger.error(str(error))

        return list(blocks)

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

    def getDatasetEventsAndLumis(self, dataset: str, blocks=None) -> Tuple[int, int]:
        # TODO: try to optimize this
        # TODO: try to simplify this
        """
        The function to get (???) a given dataset
        :param dataset: dataset name
        :param blocks: blocks names (type?)
        :return: number of events and lumis
        """
        try:
            if blocks:
                files = []
                for block in blocks:
                    files.extend(
                        self.dbs.listFileSummaries(block_name=block, validFileOnly=1)
                    )
            else:
                files = self.dbs.listFileSummaries(dataset=dataset, validFileOnly=1)

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

    def getDatasetLumisAndFiles(
        self,
        dataset: str,
        runs: Optional[list] = None,
        lumis: Optional[dict] = None,
        validFileOnly: bool = True,
    ) -> Tuple[dict, dict]:
        # TODO: try to simplify this
        """
        The function to get the lumis and files of a given dataset
        :param dataset: dataset name
        :param runs: runs names
        :param lumis: lumis names and runs
        :param validFileOnly: if True, keeps only valid files, keep all o/w
        :return: a dict of lumis and a dict of files
        """
        if runs and lumis:
            self.logger.error("Only runs or lumis should be defined, not both")
            return {}, {}

        try:
            cacheKey = f"json_lumis_{dataset}"
            cached = self.cache.get(cacheKey)
            if cached:
                self.logger.info("lumis of %s taken from cache", dataset)
                filesPerLumi, lumisPerRun = cached["files"], cached["lumis"]
            else:
                filesPerLumi, lumisPerRun = defaultdict(set), defaultdict(set)
                blocks = self.dbs.listBlocks(dataset=dataset)
                files = self._getBlockFiles(
                    [
                        {
                            "block": block.get("block_name"),
                            "validFileOnly": validFileOnly,
                        }
                        for block in blocks
                    ]
                )
                for file in files:
                    runKey = str(file["run_num"])
                    lumisPerRun[runKey].update(file["lumi_section_num"])
                    for lumiKey in file["lumi_section_num"]:
                        filesPerLumi[f"{runKey}:{lumiKey}"].add(
                            file["logical_file_name"]
                        )

                lumisPerRun = dict((k, list(v)) for k, v in lumisPerRun.items())
                filesPerLumi = dict((k, list(v)) for k, v in filesPerLumi.items())

                self.cache.store(
                    cacheKey,
                    {"files": filesPerLumi, "lumis": lumisPerRun},
                    lifeTimeMinutes=600,
                )

            lumisPerRun = dict((int(k), v) for k, v in lumisPerRun.items())
            filesPerLumi = dict(
                (tuple(map(int, k.split(":"))), v) for k, v in filesPerLumi.items()
            )

            if runs:
                lumisPerRun = self._filterKeysByList(lumisPerRun, runs)
                filesPerLumi = self._filterKeysByList(filesPerLumi, runs)
            elif lumis:
                runs = map(int(lumis.keys()))
                lumis = set((k, v) for k, v in lumis.items())
                lumisPerRun = self._filterKeysByList(lumisPerRun, runs)
                filesPerLumi = self._filterKeysByList(filesPerLumi, lumis)
            return lumisPerRun, filesPerLumi

        except Exception as error:
            self.logger.error(
                "Failed to get lumis and files from DBS for dataset %s", dataset
            )
            self.logger.error(str(error))

    @runWithThreads
    def _getBlockFiles(self, block: str, validFileOnly: bool = True) -> dict:
        # TODO: rename ?
        """
        The function to get lumi files from a given block
        :param block: block name
        :param validFileOnly: if True, keeps only valid files, keep all o/w
        :return: lumi files
        """
        try:
            return self.dbs.listFileLumis(block, validFileOnly=int(validFileOnly))
        except Exception as error:
            self.logger.error("Failed to get files from DBS for block %s", block)
            self.logger.error(str(error))

    def _filterKeysByList(data: dict, lst: list) -> dict:
        """
        The function to filter dict keys by a given list
        :param data: a dict
        :param lst: key values to keep
        :return: a dict
        """
        return dict(
            (k, v)
            for k, v in data.items()
            if k in lst or (isinstance(k, tuple) and k[0] in lst)
        )

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

    def getLFNbase(self, dataset: str) -> str:
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
            for file, location in filesAndLocations.items():
                if file in cacheBlockFiles:
                    blockName = cacheBlockFiles[file]
                else:
                    blockName = self.getBlockName(file)
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

    # MOVE TO OTHER FILE
    def getRecoveryDocs(self, collectionName: str) -> List[dict]:
        # TODO: confirm call against couchdb
        # TODO: this function doesn't make sense here
        # TODO: missing url
        """
        The function to get the recovery docs of a given collection
        :param collectionName: collection name
        :return: list of recovery docs
        """
        from Utils.WebTools import getResponse

        try:
            result = getResponse(
                url=None,
                endpoint=f"/couchdb/acdcserver/_design/ACDC/_view/byCollectionName?key={collectionName}&include_docs=true&reduce=false",
            )
            data = result["rows"]
            return [item["doc"] for item in data]

        except Exception as error:
            self.logger.error(
                "Failed to get recovery docs from ACDC server for collection %s",
                collectionName,
            )
            self.logger.error(str(error))

    def getRecoveryFilesAndLocations(
        self, recoveryDocs: List[dict], suffixTaskFilter: Optional[str] = None
    ) -> dict:
        # TODO: this function doesn't make sense here
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

        self.logger.info(f"{len(filesAndLocations)} files in recovery")

        return dict((k, list(v)) for k, v in filesAndLocations.items())

    def splitFilesInAndNotInBDS(self, filesAndLocations: dict) -> Tuple[dict, dict]:
        # TODO: this function doesn't make sense here
        """
        The function to split the files in a subset of files in DBS and not in DBS
        :param filesAndLocations: dict of files and locations
        :return: two dicts of files and locations
        """
        filesInDBS, filesNotInDBS = set(), set()
        for filename in filesAndLocations:
            if any(
                filename.startswith(strg)
                for strg in ["/store/unmerged/", "MCFakeFile-"]
            ):
                filesNotInDBS.add(filename)
            else:
                filesInDBS.add(filename)

        inDBS = self._filterKeysByList(filesAndLocations, filesInDBS)
        notInDBS = self._filterKeysByList(filesAndLocations, filesNotInDBS)
        return inDBS, notInDBS
