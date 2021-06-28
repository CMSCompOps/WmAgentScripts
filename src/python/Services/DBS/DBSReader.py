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
import threading

from Utils.ConfigurationHandler import ConfigurationHandler

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
        # instantiate dbs api object
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
        # TODO: whats does this function returns ?
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
            msg = f"Exception while getting the status of following dataset on DBS: {dataset}"
            msg += f"Error: {str(error)}"
            self.logger.error(msg)

    def getFilesWithLumiInRun(self, dataset: str, run: int):
        # TODO: whats does this function returns?
        # TODO: keep returning an empty list in case of error?
        """
        The function to get the files of a given dataset and run
        :param dataset: dataset name
        :param run: run name
        :return: a list of (???)
        """
        result = []
        try:
            result = (
                self.dbs.listFiles(
                    dataset=dataset, detail=True, run_num=run, validFileOnly=1
                )
                if run != 1
                else self.dbs.listFiles(dataset=dataset, detail=True, validFileOnly=1)
            )

        except Exception as error:
            self.logger.error(
                f"Fatal exception in running dbsapi.listFiles for {dataset} and {run}"
            )
            self.logger.error(str(error))

        filenames = [
            file["logical_file_name"] for file in result if file["is_file_valid"] == 1
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

    def getFileBlock(self, filename: str) -> str:
        # TODO: rename
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
            self.logger.error(f"Failed to get block name from DBS for file {filename}")
            self.logger.error(str(error))

    def getDatasetFileArray(
        self,
        dataset: str,
        validFileOnly: bool = False,
        details: bool = False,
        cacheTimeout: int = 30,
        useArray: bool = False,
    ) -> List[dict]:
        """
        The function to get the file array for a given dataset
        :param dataset: dataset name
        :param validFileOnly: file status
        :param details: if True, returns (???)
        :param cacheTimeout: time out
        :param useArray: if True, returns (???)
        :return: file array
        """
        try:
            # TODO: implement cacheInfo ?
            # call = "listFileArray" if useArray else "listFile"
            # cacheKey = f"dbs_{call}_{dataset}"
            # cached = cacheInfo().get(cacheKey)
            call, cached = None, False

            if cached:
                self.logger.info(f"{call} of {dataset} taken from cache")
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
                f"Failed to get file array from DBS for dataset {dataset}"
            )
            self.logger.error(str(error))

    def getDatasetBlocks(self, dataset: str, runs=None, lumis=None):
        # TODO: whats does this function return?
        # TODO: keep returning an empty list in case of error?
        """
        The function to get the blocks of a given dataset
        :param dataset: dataset name
        :param run: run names (type?)
        :param lumi: lumi section run names (type?)
        :return: a list of block names (???)
        """
        blocks = set()
        if lumis:
            self.logger.info("Entering a heavy check on block per lumi")
            for run in lumis:
                # TODO: fix when run is 1 ? Exception in listFileArray 'Invalid input: files API does not supprt run_num=1 without logical_file_name.'
                try:
                    files = self.dbs.listFileArray(
                        dataset=dataset,
                        lumi_list=lumis[run],
                        run_num=int(run),
                        detail=True,
                    )
                    self.logger.info(
                        f"Retrieved {len(files)} files for dataset {dataset}, lumi {lumis[run]}, run {int(run)}"
                    )
                    blocks.update([file["block_name"] for file in files])

                except Exception as error:
                    self.logger.error(
                        f"Failed to get file array from DBS for dataset {dataset}, lumi {lumis[run]}, run {int(run)}"
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
                        f"Failed to get blocks from DBS for dataset {dataset}, run {int(run)}"
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
                    f"Failed to get blocks from DBS for dataset {dataset}"
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
            self.logger.error(f"Failed to get size of dataset {dataset} from DBS")
            self.logger.error(str(error))

    def getDatasetEventsAndLumis(self, dataset: str, blocks=None) -> Tuple[int, int]:
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
            self.logger.error(f"Failed to get runs from DBS for dataset {dataset}")
            self.logger.error(str(error))

    def getDatasetLumisAndFiles(
        self,
        dataset: str,
        runs=None,
        lumis=None,
        withCache: bool = False,
        force: bool = False,
        checkWithInvalidFilesToo: bool = False,
    ):
        """
        The function to get the lumis and files of a given dataset
        :param dataset: dataset name
        :param runs: runs names
        :param lumis: lumis names
        :param withCache: (???)
        :param force: (???)
        :param checkWithInvalidFilesToo: (???)
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

            cached, cache = None, None
            cacheKey = f"json_lumis_{dataset}"
            if withCache:
                # TODO: implement cacheInfo ?
                # cached = cacheInfo().get(cacheKey)
                pass

            if not cached:
                self.logger.info(f"Querying getDatasetLumisAndFiles for {dataset}")
                fullLumiJson = defaultdict(set)
                filesPerLumi = defaultdict(set)

                # TODO: better way of doing this?
                class getFilesFromBlock(threading.Thread):
                    def __init__(self, dbs, block, checkWithInvalidFilesToo):
                        threading.Thread.__init__(self)
                        self.dbs = dbs
                        self.block = block
                        self.checkInvalidFiles = checkWithInvalidFilesToo
                        self.files = None

                    def run(self):
                        self.files = self.dbs.listFileLumis(
                            block_name=self.block,
                            validFileOnly=int(not self.checkInvalidFiles),
                        )

                blocks = self.dbs.listBlocks(dataset=dataset)
                threads = [
                    getFilesFromBlock(
                        self.dbs, block.get("block_name"), checkWithInvalidFilesToo
                    )
                    for block in blocks
                ]
                runThreads = ThreadHandler(
                    threads=threads, n_threads=10, label="getDatasetLumisAndFiles"
                )
                runThreads.start()
                while runThreads.is_alive():
                    time.sleep(1)

                for thread in runThreads.threads:
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
                    lifetime_min=600,
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
                f"Failed to get lumis and files from DBS for dataset {dataset}"
            )
            self.logger.error(str(error))

    def findParent(self, dataset: str):
        """
        The function to get the parent dataset of a given dataset
        :param dataset: dataset name
        :return: a list of parent datasets
        """
        try:
            result = self.dbs.listDatasetParents(dataset=dataset)
            return [parent.get("parent_dataset") for parent in result]

        except Exception as error:
            self.logger.error(f"Failed to get parents from DBS for dataset {dataset}")
            self.logger.error(str(error))

    def getDatasets(self, dataset: str):
        try:
            _, datasetName, processedName, tierName = dataset.split("/")
            result = self.dbs.listDatasets(
                primary_ds_name=datasetName,
                processed_ds_name=processedName,
                data_tier_name=tierName,
                dataset_access_type="*",
            )

        except Exception as error:
            self.logger.error(f"Failed to get datasets from DBS for dataset {dataset}")
            self.logger.error(str(error))

    def getLFNbase(self, dataset: str):
        try:
            result = self.dbs.listFiles(dataset=dataset)
            filename = result[0]["logical_file_name"]
            return "/".join(filename.split("/")[:3])

        except Exception as error:
            self.logger.error(f"Failed to get name from DBS for dataset {dataset}")
            self.logger.error(str(error))

    def getRecoveryBlocks(self, collectionName=None, forTask=None):
        try:
            # TODO: implement getRecoveryDoc ?
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
            cache = cacheInfo()
            fileBlockCache = defaultdict(str)
            for file in files:
                if not file.startswith("/store/unmerged/") and not file.startswith(
                    "MCFakeFile-"
                ):
                    if file in fileBlockCache:
                        fileBlock = fileBlockCache[file]
                    else:
                        fileBlock = getFileBlock(file)
                        if fileBlock:
                            for fileArray in self.getDatasetFileArray(
                                fileBlock.split("#")[0],
                                detail=True,
                                cacheTimeour=12 * 60 * 60,
                            ):
                                fileBlockCache[fileArray["logical_file_name"]] = fileBlock[
                                    "block_name"
                                ]
                    filesInBlock.add(file)
                    blocks.add(fileBlock)
                    blocksLoc[fileBlock].update(filesAndLoc.get(file, []))
                else:
                    filesNoBlock.add(file)
            
            fileBlockDoc = defaultdict( lambda : defaultdict( set ))
            datasetBlocks = set()
            for file, block in fileBlockCache.items():
                fileBlockDoc[block.split('#')[0]]][block].add(file)
                datasetBlocks.add(block)
            
            filesAndLocNoBlock = dict([(k,list(v)) for (k,v) in filesAndLoc.items() if k in filesNoBlock])
            filesAndLoc = dict([(k,list(v)) for (k,v) in filesAndLoc.items() if k in filesInBlock])
            return datasetBlocks, blocksLoc, filesInBlock,filesAndLoc,filesAndLocNoBlock

        except Exception as error:
            self.logger.error(f"Failed to recovery blocks from DBS for {collectionName}")
            self.logger.error(str(error))

