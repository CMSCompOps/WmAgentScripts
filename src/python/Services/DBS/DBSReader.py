"""
File       : DBSReader.py
Author     : Hasan Ozturk <haozturk AT cern dot com>
Description: General API for reading data from DBS
"""

# from dbs.apis.dbsClient import DbsApi
import logging
import os
import time
from collections import defaultdict

from Utils.ConfigurationHandler import ConfigurationHandler
from Utils.Decorators import runWithThreads
from Services.MongoInfo.MongoInfo import CacheInfo

from typing import Optional, List, Tuple


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
                self.dbsURL = url.replace("cmsweb.cern.ch", "cmsweb-prod.cern.ch")
            else:
                configurationHandler = ConfigurationHandler()
                self.dbsURL = os.getenv(
                    "DBS_READER_URL", configurationHandler.get("dbs_url")
                )
            # self.dbs = DbsApi(self.dbsURL, **contact)
            self.dbs = None
            self.logger = logger or logging.getLogger(self.__class__.__name__)

        except Exception as e:
            msg = "Error in DBSReader with DbsApi\n"
            msg += f"{e}\n"
            raise Exception(msg)

    def getDBSStatus(self, dataset: str):
        # TODO: what type does this function returns ?
        """
        The function to get the DBS status of outputs
        :param dataset: dataset name
        :return: DBS status of the given dataset
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

    def getFilesWithLumiInRun(self, dataset: str, run: int):
        # TODO: what does this function returns ?
        """
        The function to get the files of a given dataset and run
        :param dataset: dataset name
        :param run: run name
        :return: a list of (???)
        """
        try:
            result = (
                self.dbs.listFiles(
                    dataset=dataset, detail=True, run_num=run, validFileOnly=1
                )
                if run != 1
                else self.dbs.listFiles(dataset=dataset, detail=True, validFileOnly=1)
            )
            filenames = [
                file["logical_file_name"]
                for file in result
                if file["is_file_valid"] == 1
            ]
            files = []
            bucketSize = 100
            for bucketStart in range(0, len(filenames), bucketSize):
                bucketFilenames = filenames[bucketStart : bucketStart + bucketSize]
                files.extend(
                    self.dbs.listFileLumiArray(
                        logical_file_name=bucketFilenames, run_num=run
                    )
                    if run != 1
                    else self.dbs.listFileLumiArray(logical_file_name=bucketFilenames)
                )
            return files

        except Exception as error:
            self.logger.error(
                "Failed to get files for dataset %s and run %s",
                dataset,
                run,
            )
            self.logger.error(str(error))

    def getFileBlock(self, filename: str) -> str:
        # TODO: rename, getFileBlockName?
        """
        The function to get the block name of a given file
        :param filename: file name
        :return: block name
        """
        try:
            result = self.dbs.listFileArray(logical_file_name=filename, detail=True)
            # TODO: check a better way of doing this, possibly result[0]["block_name"] ?
            return [df["block_name"] for df in result][0]

        except Exception as error:
            self.logger.error("Failed to get block name from DBS for file %s", filename)
            self.logger.error(str(error))

    def getDatasetFileArray(
        self,
        dataset: str,
        validFileOnly: bool = False,
        details: bool = False,
        useArray: bool = False,
    ) -> List[dict]:
        # TODO: try to simplify this
        # TODO: try to optimize this
        """
        The function to get the file array for a given dataset
        :param dataset: dataset name
        :param validFileOnly: file status
        :param details: if True, returns (???)
        :param useArray: if True, returns (???)
        :return: file array
        """
        try:
            cacheCall = "listFileArray" if useArray else "listFile"
            cacheKey = f"dbs_{cacheCall}_{dataset}"
            cached = CacheInfo().get(cacheKey)

            if cached:
                self.logger.info("%s of %s taken from cache", cacheCall, dataset)
                files = cached
            elif useArray:
                files = self.dbs.listFileArray(dataset=dataset, detail=True)
            else:
                files = self.dbs.listFiles(dataset=dataset, detail=True)

            if validFileOnly:
                files = [file for file in files if file["is_file_valid"]]

            if not details:
                keysToKeep = ["logical_file_name", "is_file_valid"]
                files = [
                    dict([(k, v) for k, v in file.items() if k in keysToKeep])
                    for file in files
                ]

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
        # TODO: try to optimize this
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

            if files == [None]:
                files = []

            events = sum([file["num_event"] for file in files])
            lumis = sum([file["num_lumi"] for file in files])
            return events, lumis

        except Exception as error:
            self.logger.error("Failed to get events and lumis from DBS")
            self.logger.error(str(error))

    def getDatasetRuns(self, dataset: str) -> List[int]:
        """
        The function to get the runs for a given dataset
        :param dataset: dataset name
        :return: list of runs
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
        runs=None,
        lumis=None,
        withCache: bool = False,
        force: bool = False,
        checkInvalidFiles: bool = False,
    ):
        # TODO: try to optimize this
        # TODO: try to simplify this
        """
        The function to get the lumis and files of a given dataset
        :param dataset: dataset name
        :param runs: runs names
        :param lumis: lumis names
        :param withCache: (???)
        :param force: (???)
        :param checkInvalidFiles: (???)
        :return: a dict of lumis and a dict of files
        """
        if runs and lumis:
            self.logger.error("Only runs or lumis should be defined, not both")
            return {}, {}

        try:
            if lumis:
                lumis = set((k, v) for k, v in lumis.items())

            if force:
                withCache = False

            cache = CacheInfo()
            cacheKey = f"json_lumis_{dataset}"
            if withCache:
                cacheKey = f"json_lumis_{dataset}"
                cached = cache.get(cacheKey)

            if not withCache or not cached:
                self.logger.info("Querying getDatasetLumisAndFiles for %s", dataset)
                fullLumiJson = defaultdict(set)
                filesPerLumi = defaultdict(set)

                # TODO: try to simplify this
                blocks = self.dbs.listBlocks(dataset=dataset)
                threads = [
                    {
                        "block": block.get("block_name"),
                        "checkInvalidFiles": checkInvalidFiles,
                    }
                    for block in blocks
                ]
                threadsResult = self._getBlockFiles(threads)
                for thread in threadsResult:
                    if not thread.files:
                        continue
                    for file in thread.files:
                        fullLumiJson[str(file["run_num"])].update(
                            file["lumi_section_num"]
                        )
                        for lumi in file["lumi_section_num"]:
                            filesPerLumi[f"{file['run_num']}:{lumi}"].add(
                                file["logical_file_name"]
                            )

                for k, v in fullLumiJson.items():
                    fullLumiJson[k] = list(v)
                for k, v in filesPerLumi.items():
                    filesPerLumi[k] = list(v)

                cache.store(
                    cacheKey,
                    {"files": filesPerLumi, "lumis": fullLumiJson},
                    lifeTimeMinutes=600,
                )
            else:
                filesPerLumi = cached["files"]
                fullLumiJson = cached["lumis"]

            lumiJson = dict([(int(k), v) for (k, v) in fullLumiJson.items()])
            filesJson = dict(
                [(tuple(map(int, k.split(":"))), v) for (k, v) in filesPerLumi.items()]
            )
            if runs:
                lumiJson = dict(
                    [(int(k), v) for (k, v) in fullLumiJson.items() if int(k) in runs]
                )
                filesJson = dict(
                    [
                        (tuple(map(int, k.split(":"))), v)
                        for (k, v) in filesPerLumi.items()
                        if int(k.split(":")[0]) in runs
                    ]
                )
            elif lumis:
                runs = map(int(lumis.keys()))
                lumiJson = dict(
                    [(int(k), v) for (k, v) in fullLumiJson.items() if int(k) in runs]
                )
                filesJson = dict(
                    [
                        (tuple(map(int, k.split(":"))), v)
                        for (k, v) in filesPerLumi.items()
                        if map(int, k.split(":")) in lumis
                    ]
                )
            return lumiJson, filesJson

        except Exception as error:
            self.logger.error(
                "Failed to get lumis and files from DBS for dataset %s", dataset
            )
            self.logger.error(str(error))

    @runWithThreads
    def _getBlockFiles(
        self,
        block: str,
        checkInvalidFiles: bool = False,
    ):
        try:
            return self.dbs.listFileLumis(
                block, validFileOnly=int(not checkInvalidFiles)
            )
        except Exception as error:
            self.logger.error("Failed to get files from DBS for block %s", block)
            self.logger.error(str(error))

    def findParent(self, dataset: str):
        # TODO: what type does this function return ?
        """
        The function to get the parent dataset of a given dataset
        :param dataset: dataset name
        :return: a list of parent datasets
        """
        try:
            result = self.dbs.listDatasetParents(dataset=dataset)
            return [parent.get("parent_dataset") for parent in result]

        except Exception as error:
            self.logger.error("Failed to get parents from DBS for dataset %s", dataset)
            self.logger.error(str(error))

    def getDatasets(self, dataset: str):
        # TODO: what does this function return ?
        # TODO: rename, getDatasetInfo ?
        """
        The function to get (???)
        :param dataset: dataset name
        :return: (????)
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

    def getLFNbase(self, dataset: str):
        # TODO: what does this function return ?
        """
        The function to get (???)
        :param dataset: dataset name
        :return: (????)
        """
        try:
            result = self.dbs.listFiles(dataset=dataset)
            filename = result[0]["logical_file_name"]
            return "/".join(filename.split("/")[:3])

        except Exception as error:
            self.logger.error("Failed to get name from DBS for dataset %s", dataset)
            self.logger.error(str(error))

    def getRecoveryBlocks(
        self, collectionName: Optional[str] = None, forTask: Optional[str] = None
    ):
        # TODO: what does this function return ?
        # TODO: try to simplify this
        """
        The function to get the blocks needed for the recovery of a workflow
        :param collectionName: collection name
        :param forTask: (????)
        :return: (????)
        """
        try:
            # TODO: implement getRecoveryDoc ?
            # TODO: use docs or files as input param instead of the collectionName ?
            docs = self.getRecoveryDoc(collectionName)
            files = set()
            filesAndLoc = defaultdict(set)
            for doc in docs:
                task = doc.get("fileset_name", "")
                if forTask and not task.endswith(forTask):
                    continue
                files.update(doc["files"].keys())
                for filename in doc["files"]:
                    filesAndLoc[filename].update(doc["files"][filename]["locations"])

            self.logger.info(f"{len(files)} files in recovery")
            blocks = set()
            blocksLoc = defaultdict(set)
            filesNoBlock = set()
            filesInBlock = set()
            datasets = set()
            cache = CacheInfo()
            fileBlockCache = defaultdict(str)
            for file in files:
                if not file.startswith("/store/unmerged/") and not file.startswith(
                    "MCFakeFile-"
                ):
                    if file in fileBlockCache:
                        fileBlock = fileBlockCache[file]
                    else:
                        fileBlock = self.getFileBlock(file)
                        if fileBlock:
                            for fileArray in self.getDatasetFileArray(
                                fileBlock.split("#")[0],
                                detail=True,
                                cacheTimeour=12 * 60 * 60,
                            ):
                                fileBlockCache[
                                    fileArray["logical_file_name"]
                                ] = fileBlock["block_name"]
                    filesInBlock.add(file)
                    blocks.add(fileBlock)
                    blocksLoc[fileBlock].update(filesAndLoc.get(file, []))
                else:
                    filesNoBlock.add(file)

            fileBlockDoc = defaultdict(lambda: defaultdict(set))
            datasetBlocks = set()
            for file, block in fileBlockCache.items():
                fileBlockDoc[block.split("#")[0]][block].add(file)
                datasetBlocks.add(block)

            filesAndLocNoBlock = dict(
                [(k, list(v)) for (k, v) in filesAndLoc.items() if k in filesNoBlock]
            )
            filesAndLoc = dict(
                [(k, list(v)) for (k, v) in filesAndLoc.items() if k in filesInBlock]
            )
            return (
                datasetBlocks,
                blocksLoc,
                filesInBlock,
                filesAndLoc,
                filesAndLocNoBlock,
            )

        except Exception as error:
            self.logger.error(
                "Failed to recovery blocks from DBS for %s", collectionName
            )
            self.logger.error(str(error))

    def getRecoveryDoc(self, collectionName: str):
        # TODO: what does this function return ?
        # TODO: confirm call against couchdb
        # TODO: I don't think this should be here
        """
        The function to get the recovery docs of a given collection
        :param collectionName: collection name
        :return: (????)
        """
        from Utils.WebTools import getResponse

        try:
            result = getResponse(
                url=self.dbsURL,
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
