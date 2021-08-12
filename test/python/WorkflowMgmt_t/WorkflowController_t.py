"""
_WorkflowController_t_
Unit test for WorkflowController helper class.
"""

import os
import json
import math
import unittest
from unittest.mock import patch, mock_open, MagicMock
from collections import Counter

from WorkflowMgmt.WorkflowController import WorkflowController

from WorkflowMgmt.WorkflowSchemaHandlers.BaseWfSchemaHandler import BaseWfSchemaHandler
from WorkflowMgmt.WorkflowSchemaHandlers.StepChainWfSchemaHandler import StepChainWfSchemaHandler
from WorkflowMgmt.WorkflowSchemaHandlers.TaskChainWfSchemaHandler import TaskChainWfSchemaHandler


class WorkflowControllerTest(unittest.TestCase):
    rucioConfig = {"account": os.getenv("RUCIO_ACCOUNT")}

    # This workflow is a monte carlo request. Use it for testing functions depending on the request type as well as splittings functions.
    mcParams = {
        "workflow": "pdmvserv_SMP-RunIISummer15wmLHEGS-00016_00051_v0__160525_042701_9941",
        "campaign": "RunIISummer15wmLHEGS",
        "prepId": "SMP-RunIISummer15wmLHEGS-00016",
        "requestType": "MonteCarlo",
        "acquisitionEra": "RunIISummer15wmLHEGS",
        "processingString": "MCRUN2_71_V1",
        "scramArch": "slc6_amd64_gcc481",
        "memory": 869,
        "multicore": 1,
        "requestNumEvents": 4600000,
        "filterEfficiency": 1,
        "cpuSec": 204123160.0,
        "neededCopies": 2,
        "nextVersion": 3,
        "wildcardVersionGuess": 2,
        "nTasks": 8,
        "nWorkTasks": 1,
        "firstTask": "Production",
        "productionTaskOutput": [
            "/WplusToMuNu_NNPDF30_TuneEE5C_13TeV-powheg-herwigpp/RunIISummer15wmLHEGS-MCRUN2_71_V1-v1/GEN-SIM",
            "/WplusToMuNu_NNPDF30_TuneEE5C_13TeV-powheg-herwigpp/RunIISummer15wmLHEGS-MCRUN2_71_V1-v1/LHE",
        ],
        "productionConfigId": "8c88280d5d1fab06affb5bd1939a8ac3",
        "family": "vlimant_SMP-RunIISummer15wmLHEGS-00016_00051_v0__160531_105424_6724",
        "splittingParams": {
            "splittingAlgo": "EventBased",
            "events_per_job": 649,
            "events_per_lumi": 200,
            "splittingTask": "/pdmvserv_SMP-RunIISummer15wmLHEGS-00016_00051_v0__160525_042701_9941/Production",
            "dropParams": [
                "algorithm",
                "trustPUSitelists",
                "trustSitelists",
                "deterministicPileup",
                "type",
                "include_parents",
                "lheInputFiles",
                "runWhitelist",
                "runBlacklist",
                "collectionName",
                "group",
                "couchDB",
                "couchURL",
                "owner",
                "initial_lfn_counter",
                "filesetName",
                "runs",
                "lumis",
            ],
        },
        "cachedFile": {"filename": "test", "fileData": {"test": "data"}},
    }

    # This workflow is a reconstruction request. Use it for testing functions with a non-empty run white list or a non-empty input dataset
    rerecoParams = {
        "workflow": "sagarwal_Run2017E-31Mar2018-v1-HighMultiplicityEOF3-Nano1June2019_10215_190607_141831_7758",
        "runWhiteList": [303818, 303819],
        "block": "/HighMultiplicityEOF3/Run2017E-31Mar2018-v1/MINIAOD#5d2100bc-370d-11e8-aea5-02163e01877e",
        "cpuSec": 4185924.2,
    }

    # This workflow is a step chain request. Use it for testing function depending on the request type.
    stepChainParams = {
        "workflow": "cmsunified_task_TSG-Run3Winter21DRMiniAOD-00081__v1_T_210507_182332_1792",
        "campaign": "Run3Winter21DRMiniAOD",
        "prepId": "TSG-Run3Winter21DRMiniAOD-00081",
        "requestType": "StepChain",
        "acquisitionEra": "Run3Winter21DRMiniAOD",
        "processingString": "FlatPU30to80_Scouting_Patatrack_112X_mcRun3_2021_realistic_v16",
        "scramArch": "slc7_amd64_gcc900",
        "memory": 15900,
        "multicore": 8,
        "filterEfficiency": 1,
        "cpuSec": 92160,
        "nextVersion": 3,
        "steps": ["TSG-Run3Winter21DRMiniAOD-00081_0", "TSG-Run3Winter21DRMiniAOD-00081_1"],
        "primary": [
            "/VectorZPrimeToQQ_M-100_Pt-300_TuneCP5_14TeV-madgraph-pythia8/Run3Winter21wmLHEGS-112X_mcRun3_2021_realistic_v15-v2/GEN-SIM"
        ],
        "secondary": ["/MinBias_TuneCP5_14TeV-pythia8/Run3Winter21GS-112X_mcRun3_2021_realistic_v15-v1/GEN-SIM"],
        "step2Output": "/VectorZPrimeToQQ_M-100_Pt-300_TuneCP5_14TeV-madgraph-pythia8/Run3Winter21DRMiniAOD-FlatPU30to80_Scouting_Patatrack_112X_mcRun3_2021_realistic_v16-v2/MINIAODSIM",
    }

    # This workflow is a task chain / redigi request. Use it for testing function depending on the request type.
    redigiTaskChainParams = {
        "workflow": "pdmvserv_task_BPH-RunIIFall18GS-00350__v1_T_201021_154340_8354",
        "requestType": "TaskChain",
        "scramArch": "slc6_amd64_gcc700",
        "memory": 14700,
        "multicore": [8, 8, 8, 1],
        "requestNumEvents": 5000000,
        "cpuSec": 175077.33927660278,
        "nextVersion": 2,
        "totalTimePerEvent": 389.0607539480062,
        "blowUp": 49.028994082840235,
        "completionFraction": 0.9842801533207057,
        "secondary": [
            "/Neutrino_E-10_gun/RunIISummer17PrePremix-PUAutumn18_102X_upgrade2018_realistic_v15-v1/GEN-SIM-DIGI-RAW"
        ],
        "tasks": {
            "BPH-RunIIAutumn18DRPremix-00212_0": {
                "acquisitionEra": "RunIIAutumn18DRPremix",
                "processingString": "102X_upgrade2018_realistic_v15",
                "campaign": "RunIIAutumn18DRPremix",
                "memory": 14700,
                "multicore": 8,
                "prepId": "BPH-RunIIAutumn18DRPremix-00212",
            },
            "BPH-RunIIAutumn18DRPremix-00212_1": {
                "acquisitionEra": "RunIIAutumn18DRPremix",
                "processingString": "102X_upgrade2018_realistic_v15",
                "prepId": "BPH-RunIIAutumn18DRPremix-00212",
            },
            "BPH-RunIIAutumn18MiniAOD-00364_0": {
                "acquisitionEra": "RunIIAutumn18MiniAOD",
                "processingString": "102X_upgrade2018_realistic_v15",
                "prepId": "BPH-RunIIAutumn18MiniAOD-00364",
            },
            "BPH-RunIIFall18GS-00350_0": {
                "acquisitionEra": "RunIIFall18GS",
                "processingString": "102X_upgrade2018_realistic_v11",
                "filterEfficiency": 9e-05,
                "prepId": "BPH-RunIIFall18GS-00350",
            },
        },
        "task3Output": "/DPS_ToJPsiJPsi_TuneCP5_DP_13TeV-pythia8/RunIIAutumn18DRPremix-102X_upgrade2018_realistic_v15-v1/AODSIM",
    }

    # This workflow is a task chain / relval request. Use it for testing functions with a non-empty lumi white list and non-empty wmstats.
    relvalTaskChainParams = {
        "workflow": "kaura_EXPRESS_newco_RelVal_344068_210803_171336_8221",
        "block": "/ExpressCosmics/Commissioning2021-Express-v1/FEVT#f84fe79b-e395-430d-a92a-5c697c9f268a",
        "campaign": "CMSSW_11_3_2__ALCA_137-2021_08_03_17_02",
        "lumiList": {"344068": [[1, 1512]]},
    }

    def setUp(self) -> None:
        self.mcWfController = WorkflowController(self.mcParams.get("workflow"), rucioConfig=self.rucioConfig)

        self.rerecoWfController = WorkflowController(self.rerecoParams.get("workflow"), rucioConfig=self.rucioConfig)

        self.stepChainWfController = WorkflowController(
            self.stepChainParams.get("workflow"), rucioConfig=self.rucioConfig
        )

        self.redigiTaskChainWfController = WorkflowController(
            self.redigiTaskChainParams.get("workflow"), rucioConfig=self.rucioConfig
        )

        self.relvalTaskChainWfController = WorkflowController(
            self.relvalTaskChainParams.get("workflow"), rucioConfig=self.rucioConfig
        )

        super().setUp()
        return

    def tearDown(self) -> None:
        super().tearDown()
        return

    def testGetWorkloadHandler(self) -> None:
        """_getWorkloadHandler gets the workload handler"""
        ### Test when base handler
        isBase = isinstance(self.mcWfController.request, BaseWfSchemaHandler)
        self.assertTrue(isBase)

        ### Test when step chain handler
        isStepChain = isinstance(self.stepChainWfController.request, StepChainWfSchemaHandler)
        self.assertTrue(isStepChain)

        ### Test when task chain handler
        isTaskChain = isinstance(self.redigiTaskChainWfController.request, TaskChainWfSchemaHandler)
        self.assertTrue(isTaskChain)

    @patch("os.path.isfile")
    @patch("builtins.open", create=True)
    def testGetFromFile(self, mockOpen: MagicMock, mockIsFile: MagicMock) -> None:
        """_getFromFile gets the data from file"""
        # Test behavior when the file does not exist
        mockIsFile.return_value = False
        response = self.mcWfController._getFromFile(self.mcParams.get("cachedFile").get("filename"))
        isNone = response is None
        self.assertTrue(isNone)

        # Test behavior when the file exists
        mockIsFile.return_value = True
        mockOpen.return_value = mock_open(
            read_data=json.dumps(self.mcParams.get("cachedFile").get("fileData"))
        ).return_value
        response = self.mcWfController._getFromFile(self.mcParams.get("cachedFile").get("filename"))
        isDict = isinstance(response, dict)
        self.assertTrue(isDict)

        isEqual = response == self.mcParams.get("cachedFile").get("fileData")
        self.assertTrue(isEqual)

    def testGetVersionByWildcardPattern(self) -> None:
        """_getVersionByWildcardPattern searches the version by the dataset name pattern"""
        response = self.mcWfController._getVersionByWildcardPattern()
        isInt = isinstance(response, int)
        self.assertTrue(isInt)

        isEqual = response == self.mcParams.get("wildcardVersionGuess")
        self.assertTrue(isEqual)

    def testGetVersionByConflictingWorkflows(self) -> None:
        """_getVersionByConflictingWorkflows searches the version by conflicting workflows"""
        response = self.mcWfController._getVersionByConflictingWorkflows()
        isInt = isinstance(response, int)
        self.assertTrue(isInt)

        isZero = response == 0
        self.assertTrue(isZero)

    def testIsHeavyToRead(self) -> None:
        """isHeavyToRead checks if it is heavy to read"""
        # Test when response is True
        response = self.stepChainWfController.isHeavyToRead(self.stepChainParams.get("secondary"))
        isBool = isinstance(response, bool)
        self.assertTrue(isBool)

        isTrue = response
        self.assertTrue(isTrue)

        # Test when response is False
        response = self.redigiTaskChainWfController.isHeavyToRead(self.redigiTaskChainParams.get("secondary"))
        isBool = isinstance(response, bool)
        self.assertTrue(isBool)

        isFalse = not response
        self.assertTrue(isFalse)

    @patch("WorkflowMgmt.WorkflowController.WorkflowController._saveInFile")
    def testGetWMStats(self, mockSave: MagicMock) -> None:
        """getWMStats gets the wmstats"""
        # Test when wmstats exists and no cache is allowed. This request is quite heavy, so only test this.
        mockSave.return_value = True
        response = self.relvalTaskChainWfController.getWMStats()
        isDict = isinstance(response, dict)
        self.assertTrue(isDict)

        isFound = response.get("Campaign") == self.relvalTaskChainParams.get("campaign")
        self.assertTrue(isFound)

    def testGetFamily(self) -> None:
        """getFamily gets the workflow family"""
        # Test when onlyResubmissions is False and includeItself is False
        response = self.mcWfController.getFamily()
        isList = isinstance(response, list)
        self.assertTrue(isList)

        isListOfDict = isinstance(response[0], dict)
        self.assertTrue(isListOfDict)

        isFound = response[0].get("RequestName") == self.mcParams.get("family")
        self.assertTrue(isFound)

        # Test when onlyResubmissions is False and includeItself is True
        response = self.mcWfController.getFamily(includeItself=True)
        isList = isinstance(response, list)
        self.assertTrue(isList)

        isListOfDict = isinstance(response[0], dict)
        self.assertTrue(isListOfDict)

        isFound = False
        for member in response:
            if member.get("RequestName") == self.mcParams.get("workflow"):
                isFound = True
                break
        self.assertTrue(isFound)

        isFound = False
        for member in response:
            if member.get("RequestName") == self.mcParams.get("family"):
                isFound = True
                break
        self.assertTrue(isFound)

        # Test when onlyResubmissions is True and includeItself is False
        response = self.mcWfController.getFamily(onlyResubmissions=True)
        isList = isinstance(response, list)
        self.assertTrue(isList)

        isEmpty = len(response) == 0
        self.assertTrue(isEmpty)

        # Test when onlyResubmissions is True and includeItself is True
        response = self.mcWfController.getFamily(onlyResubmissions=True, includeItself=True)
        isList = isinstance(response, list)
        self.assertTrue(isList)

        isListOfDict = isinstance(response[0], dict)
        self.assertTrue(isListOfDict)

        isFound = False
        for member in response:
            if member.get("RequestName") == self.mcParams.get("workflow"):
                isFound = True
                break
        self.assertTrue(isFound)

    def testGetAllTasks(self) -> None:
        """getAllTasks gets all tasks"""
        response = self.mcWfController.getAllTasks()
        isList = isinstance(response, list)
        self.assertTrue(isList)

        sameLen = len(response) == self.mcParams.get("nTasks")
        self.assertTrue(sameLen)

    def testGetWorkTasks(self) -> None:
        """getWorkTasks gets work tasks"""
        response = self.mcWfController.getWorkTasks()
        isList = isinstance(response, list)
        self.assertTrue(isList)

        sameLen = len(response) == self.mcParams.get("nWorkTasks")
        self.assertTrue(sameLen)

    def testGetFirstTask(self) -> None:
        """getFirstTask gets first task"""
        response = self.mcWfController.getFirstTask()
        isStr = isinstance(response, str)
        self.assertTrue(isStr)

        isFound = response == self.mcParams.get("firstTask")
        self.assertTrue(isFound)

    def testGetOutputDatasetsPerTask(self) -> None:
        """getOutputDatasetsPerTask gets output datasets per task"""
        ### Test when base request
        response = self.mcWfController.getOutputDatasetsPerTask()
        isDict = isinstance(response, dict)
        self.assertTrue(isDict)

        isValueList = all(isinstance(v, list) for v in response.values())
        self.assertTrue(isValueList)

        isFound = False
        for k, v in response.items():
            if k == "Production":
                isFound = set(v) == set(self.mcParams.get("productionTaskOutput"))
                break
        self.assertTrue(isFound)

        ### Test when step chain request
        response = self.stepChainWfController.getOutputDatasetsPerTask()
        isDict = isinstance(response, dict)
        self.assertTrue(isDict)

        isValueList = all(isinstance(v, list) for v in response.values())
        self.assertTrue(isValueList)

        isFound = False
        for k, v in response.items():
            if k == "Step2":
                isFound = v[0] == self.stepChainParams.get("step2Output")
                break
        self.assertTrue(isFound)

        ### Test when task chain request
        response = self.redigiTaskChainWfController.getOutputDatasetsPerTask()
        isDict = isinstance(response, dict)
        self.assertTrue(isDict)

        isValueList = all(isinstance(v, list) for v in response.values())
        self.assertTrue(isValueList)

        isFound = False
        for k, v in response.items():
            if k == "Task3":
                isFound = v[0] == self.redigiTaskChainParams.get("task3Output")
                break
        self.assertTrue(isFound)

    def testGetCampaignByTask(self) -> None:
        """getCampaignByTask gets the campaigns for a given task"""
        ### Test when base request
        response = self.mcWfController.getCampaignByTask("")
        isStr = isinstance(response, str)
        self.assertTrue(isStr)

        isFound = response == self.mcParams.get("campaign")
        self.assertTrue(isFound)

        ### Test when step chain request
        task = self.stepChainParams.get("steps")[0]
        response = self.stepChainWfController.getCampaignByTask(task)
        isStr = isinstance(response, str)
        self.assertTrue(isStr)

        isFound = response == self.stepChainParams.get("campaign")
        self.assertTrue(isFound)

        ### Test when task chain request
        task = list(self.redigiTaskChainParams.get("tasks").keys())[0]
        response = self.redigiTaskChainWfController.getCampaignByTask(task)
        isStr = isinstance(response, str)
        self.assertTrue(isStr)

        isFound = response == self.redigiTaskChainParams.get("tasks").get(task).get("campaign")
        self.assertTrue(isFound)

    def testGetMemoryByTask(self) -> None:
        """getMemoryByTask gets the memory for a given task"""
        ### Test when base request
        response = self.mcWfController.getMemoryByTask("")
        isInt = isinstance(response, int)
        self.assertTrue(isInt)

        isEqual = response == self.mcParams.get("memory")
        self.assertTrue(isEqual)

        ### Test when step chain request
        task = self.stepChainParams.get("steps")[0]
        response = self.stepChainWfController.getMemoryByTask(task)
        isInt = isinstance(response, int)
        self.assertTrue(isInt)

        isEqual = response == self.stepChainParams.get("memory")
        self.assertTrue(isEqual)

        ### Test when task chain request
        task = list(self.redigiTaskChainParams.get("tasks").keys())[0]
        response = self.redigiTaskChainWfController.getMemoryByTask(task)
        isInt = isinstance(response, int)
        self.assertTrue(isInt)

        isEqual = response == self.redigiTaskChainParams.get("tasks").get(task).get("memory")
        self.assertTrue(isEqual)

    def testGetCoreByTask(self) -> None:
        """getCoreByTask gets the memory for a given task"""
        ### Test when base request
        response = self.mcWfController.getCoreByTask("")
        isInt = isinstance(response, int)
        self.assertTrue(isInt)

        isEqual = response == self.mcParams.get("multicore")
        self.assertTrue(isEqual)

        ### Test when step chain request
        task = self.stepChainParams.get("steps")[0]
        response = self.stepChainWfController.getCoreByTask(task)
        isInt = isinstance(response, int)
        self.assertTrue(isInt)

        isEqual = response == self.stepChainParams.get("multicore")
        self.assertTrue(isEqual)

        ### Test when task chain request
        task = list(self.redigiTaskChainParams.get("tasks").keys())[0]
        response = self.redigiTaskChainWfController.getCoreByTask(task)
        isInt = isinstance(response, int)
        self.assertTrue(isInt)

        isEqual = response == self.redigiTaskChainParams.get("tasks").get(task).get("multicore")
        self.assertTrue(isEqual)

    def testGetFilterEfficiencyByTask(self) -> None:
        """getFilterEfficiencyByTask gets the filter efficiency for a given task"""
        ### Test when base request
        response = self.mcWfController.getFilterEfficiencyByTask("")
        isFloat = isinstance(response, float)
        self.assertTrue(isFloat)

        isEqual = response == self.mcParams.get("filterEfficiency")
        self.assertTrue(isEqual)

        ### Test when step chain request
        task = self.stepChainParams.get("steps")[0]
        response = self.stepChainWfController.getFilterEfficiencyByTask(task)
        isFloat = isinstance(response, float)
        self.assertTrue(isFloat)

        isEqual = response == self.stepChainParams.get("filterEfficiency")
        self.assertTrue(isEqual)

        ### Test when task chain request
        task = list(self.redigiTaskChainParams.get("tasks").keys())[-1]
        response = self.redigiTaskChainWfController.getFilterEfficiencyByTask(task)
        isFloat = isinstance(response, float)
        self.assertTrue(isFloat)

        isEqual = response == self.redigiTaskChainParams.get("tasks").get(task).get("filterEfficiency")
        self.assertTrue(isEqual)

    def testGetLumiWhiteList(self) -> None:
        """getLumiWhiteList gets the lumi white list"""
        ### Test when base request
        lumiList = self.mcWfController.request.getLumiWhiteList()
        isDict = isinstance(lumiList, dict)
        self.assertTrue(isDict)

        isEmpty = len(lumiList) == 0
        self.assertTrue(isEmpty)

        ### Test when step chain request
        lumiList = self.stepChainWfController.request.getLumiWhiteList()
        isDict = isinstance(lumiList, dict)
        self.assertTrue(isDict)

        isEmpty = len(lumiList) == 0
        self.assertTrue(isEmpty)

        ### Test when task chain request. This workflow has non-empty lumiList
        lumiList = self.relvalTaskChainWfController.request.getLumiWhiteList()
        isDict = isinstance(lumiList, dict)
        self.assertTrue(isDict)

        isFound = lumiList == self.relvalTaskChainParams.get("lumiList")
        self.assertTrue(isFound)

    def testGetBlockWhiteList(self) -> None:
        """getBlockWhiteList gets the block white list"""
        ### Test when base request
        blockList = self.mcWfController.getBlockWhiteList()
        isList = isinstance(blockList, list)
        self.assertTrue(isList)

        isEmpty = len(blockList) == 0
        self.assertTrue(isEmpty)

        ### Test when step chain request
        blockList = self.stepChainWfController.getBlockWhiteList()
        isList = isinstance(blockList, list)
        self.assertTrue(isList)

        isEmpty = len(blockList) == 0
        self.assertTrue(isEmpty)

        ### Test when task chain request
        blockList = self.redigiTaskChainWfController.getBlockWhiteList()
        isList = isinstance(blockList, list)
        self.assertTrue(isList)

        isEmpty = len(blockList) == 0
        self.assertTrue(isEmpty)

    def testGetRunWhiteList(self) -> None:
        """getRunWhiteList gets the run white list"""
        ### Test when base request and non-empty run white list
        runList = self.rerecoWfController.getRunWhiteList()
        isList = isinstance(runList, list)
        self.assertTrue(isList)

        isListOfInt = isinstance(runList[0], int)
        self.assertTrue(isListOfInt)

        isEqual = set(runList) == set(self.rerecoParams.get("runWhiteList"))
        self.assertTrue(isEqual)

        ### Test when step chain request
        runList = self.stepChainWfController.getRunWhiteList()
        isList = isinstance(runList, list)
        self.assertTrue(isList)

        isEmpty = len(runList) == 0
        self.assertTrue(isEmpty)

        ### Test when task chain request
        runList = self.redigiTaskChainWfController.getRunWhiteList()
        isList = isinstance(runList, list)
        self.assertTrue(isList)

        isEmpty = len(runList) == 0
        self.assertTrue(isEmpty)

    def testGetPrepIds(self) -> None:
        """getPrepIds gets the prep ids"""
        ### Test when base request
        response = self.mcWfController.getPrepIds()
        isList = isinstance(response, list)
        self.assertTrue(isList)

        isListOfStr = isinstance(response[0], str)
        self.assertTrue(isListOfStr)

        isFound = response[0] == self.mcParams.get("prepId")
        self.assertTrue(isFound)

        ### Test when step chain request
        response = self.stepChainWfController.getPrepIds()
        isList = isinstance(response, list)
        self.assertTrue(isList)

        isListOfStr = isinstance(response[0], str)
        self.assertTrue(isListOfStr)

        isFound = response[0] == self.stepChainParams.get("prepId")
        self.assertTrue(isFound)

        ### Test when task chain request
        response = self.redigiTaskChainWfController.getPrepIds()
        isList = isinstance(response, list)
        self.assertTrue(isList)

        isListOfStr = isinstance(response[0], str)
        self.assertTrue(isListOfStr)

        isFound = False
        for task in self.redigiTaskChainParams.get("tasks").values():
            if task.get("prepId") not in response:
                break
        else:
            isFound = True
        self.assertTrue(isFound)

    def testGetScramArches(self) -> None:
        """getScramArches gets the arches"""
        ### Test when base request
        response = self.mcWfController.getScramArches()
        isList = isinstance(response, list)
        self.assertTrue(isList)

        isListOfStr = isinstance(response[0], str)
        self.assertTrue(isListOfStr)

        isFound = response[0] == self.mcParams.get("scramArch")
        self.assertTrue(isFound)

        ### Test when step chain request
        response = self.stepChainWfController.getScramArches()
        isList = isinstance(response, list)
        self.assertTrue(isList)

        isListOfStr = isinstance(response[0], str)
        self.assertTrue(isListOfStr)

        isFound = response[0] == self.stepChainParams.get("scramArch")
        self.assertTrue(isFound)

        ### Test when task chain request
        response = self.redigiTaskChainWfController.getScramArches()
        isList = isinstance(response, list)
        self.assertTrue(isList)

        isListOfStr = isinstance(response[0], str)
        self.assertTrue(isListOfStr)

        isFound = response[0] == self.redigiTaskChainParams.get("scramArch")
        self.assertTrue(isFound)

    def testGetComputingTime(self) -> None:
        """getComputingTime gets the computing time"""
        ### Test when base request
        response = self.mcWfController.getComputingTime(unit="s")
        isFloat = isinstance(response, float)
        self.assertTrue(isFloat)

        isEqual = response == self.mcParams.get("cpuSec")
        self.assertTrue(isEqual)

        ### Test when base request and non-empty input dataset
        response = self.rerecoWfController.getComputingTime(unit="s")
        isFloat = isinstance(response, float)
        self.assertTrue(isFloat)

        isEqual = response == self.rerecoParams.get("cpuSec")
        self.assertTrue(isEqual)

        ### Test when step chain request
        response = self.stepChainWfController.getComputingTime(unit="s")
        isFloat = isinstance(response, float)
        self.assertTrue(isFloat)

        isEqual = response == self.stepChainParams.get("cpuSec")
        self.assertTrue(isEqual)

        ### Test when task chain request
        response = self.redigiTaskChainWfController.getComputingTime(unit="s")
        isFloat = isinstance(response, float)
        self.assertTrue(isFloat)

        isEqual = math.isclose(response, self.redigiTaskChainParams.get("cpuSec"))
        self.assertTrue(isEqual)

    def testGetBlocks(self) -> None:
        """getBlocks gets the blocks"""
        ### Test when base request and non-empty run white list
        blocks = self.rerecoWfController.getBlocks()
        isList = isinstance(blocks, list)
        self.assertTrue(isList)

        isListOfStr = isinstance(blocks[0], str)
        self.assertTrue(isListOfStr)

        isFound = self.rerecoParams.get("block") in blocks
        self.assertTrue(isFound)

        ### Test when step chain request
        blocks = self.stepChainWfController.getBlocks()
        isList = isinstance(blocks, list)
        self.assertTrue(isList)

        isEmpty = len(blocks) == 0
        self.assertTrue(isEmpty)

        ### Test when task chain request and nom-empty lumi list
        blocks = self.relvalTaskChainWfController.getBlocks()
        isList = isinstance(blocks, list)
        self.assertTrue(isList)

        isFound = self.relvalTaskChainParams.get("block") in blocks
        self.assertTrue(isFound)

    def testGetSplittings(self) -> None:
        """getSplittings gets the splittings"""
        response = self.mcWfController.getSplittings()
        isList = isinstance(response, list)
        self.assertTrue(isList)

        isListOfDict = isinstance(response[0], dict)
        self.assertTrue(isListOfDict)

        isFound = False
        for k, v in response[0].items():
            if k not in self.mcParams.get("splittingParams") or v != self.mcParams.get("splittingParams").get(k):
                break
        else:
            isFound = True
        self.assertTrue(isFound)

    def testGetSplittingsSchema(self) -> None:
        """getSplittingsSchema gets the splittings"""
        # Test when strip is False and allTasks is False
        splittings = self.mcWfController.getSplittingsSchema()
        isList = isinstance(splittings, list)
        self.assertTrue(isList)

        isListOfDicts = isinstance(splittings[0], dict)
        self.assertTrue(isListOfDicts)

        isFound = all(self.mcParams.get("workflow") in splt["taskName"] for splt in splittings)
        self.assertTrue(isFound)

        keptOnlySomeTasksTypes = all(splt["taskType"] in ["Production", "Processing", "Skim"] for splt in splittings)
        self.assertTrue(keptOnlySomeTasksTypes)

        # Test when strip is True and allTasks is True
        splittings = self.mcWfController.getSplittingsSchema(
            strip=True,
            allTasks=True,
        )
        isList = isinstance(splittings, list)
        self.assertTrue(isList)

        isListOfDicts = isinstance(splittings[0], dict)
        self.assertTrue(isListOfDicts)

        isFound = all(self.mcParams.get("workflow") in splt["taskName"] for splt in splittings)
        self.assertTrue(isFound)

        keptOnlySomeParams = False
        for splt in splittings:
            if splt["splitParams"] in self.mcParams.get("splittingParams").get("dropParams"):
                break
        else:
            keptOnlySomeParams = True
        self.assertTrue(keptOnlySomeParams)

    def testGetConfigCacheID(self) -> None:
        """getConfigCacheID gets the cache configuration id"""
        response = self.mcWfController.getConfigCacheID()
        isDict = isinstance(response, dict)
        self.assertTrue(isDict)

        isKeyStr = all(isinstance(k, str) for k in response)
        self.assertTrue(isKeyStr)

        isValueStr = all(isinstance(v, str) for v in response.values())
        self.assertTrue(isValueStr)

        isFound = response.get("Production") == self.mcParams.get("productionConfigId")
        self.assertTrue(isFound)

    def testGetBlowupFactor(self) -> None:
        """getBlowupFactor gets the blocks"""
        ### Test when base request
        response = self.mcWfController.getBlowupFactor()
        isFloat = isinstance(response, float)
        self.assertTrue(isFloat)

        isEqual = response == 1
        self.assertTrue(isEqual)

        ### Test when step chain request
        response = self.stepChainWfController.getBlowupFactor()
        isFloat = isinstance(response, float)
        self.assertTrue(isFloat)

        isEqual = response == 1
        self.assertTrue(isEqual)

        ### Test when task chain request
        response = self.redigiTaskChainWfController.getBlowupFactor()
        isFloat = isinstance(response, float)
        self.assertTrue(isFloat)

        isEqual = math.isclose(response, self.redigiTaskChainParams.get("blowUp"))
        self.assertTrue(isEqual)

    def testGetCompletionFraction(self) -> None:
        """getCompletionFraction gets the completion fraction"""
        ### Test when base request
        response = self.mcWfController.getCompletionFraction()
        isDict = isinstance(response, dict)
        self.assertTrue(isDict)

        isKeyStr = all(isinstance(k, str) for k in response)
        self.assertTrue(isKeyStr)

        isValueFloat = all(isinstance(v, float) for v in response.values())
        self.assertTrue(isValueFloat)

        isZero = all(v == 0 for v in response.values())
        self.assertTrue(isZero)

        ### Test when step chain request
        response = self.stepChainWfController.getCompletionFraction()
        isDict = isinstance(response, dict)
        self.assertTrue(isDict)

        isKeyStr = all(isinstance(k, str) for k in response)
        self.assertTrue(isKeyStr)

        isValueFloat = all(isinstance(v, float) for v in response.values())
        self.assertTrue(isValueFloat)

        isComplete = all(v == 1 for v in response.values())
        self.assertTrue(isComplete)

        ### Test when task chain request
        response = self.redigiTaskChainWfController.getCompletionFraction()
        isDict = isinstance(response, dict)
        self.assertTrue(isDict)

        isKeyStr = all(isinstance(k, str) for k in response)
        self.assertTrue(isKeyStr)

        isValueFloat = all(isinstance(v, float) for v in response.values())
        self.assertTrue(isValueFloat)

        isEqual = all(math.isclose(v, self.redigiTaskChainParams.get("completionFraction")) for v in response.values())
        self.assertTrue(isEqual)

    def testGetNCopies(self) -> None:
        """getNCopies gets the number of needed copies"""
        response = self.mcWfController.getNCopies(self.mcParams.get("cpuSec") / 3600.0)
        isInt = isinstance(response, int)
        self.assertTrue(isInt)

        isEqual = response == self.mcParams.get("neededCopies")
        self.assertTrue(isEqual)

    def testGetNextVersion(self) -> None:
        """getNextVersion gets the next processing version"""
        ### Test when base request
        response = self.mcWfController.getNextVersion()
        isInt = isinstance(response, int)
        self.assertTrue(isInt)

        isEqual = response == self.mcParams.get("nextVersion")
        self.assertTrue(isEqual)

        ### Test when step chain request
        response = self.stepChainWfController.getNextVersion()
        isInt = isinstance(response, int)
        self.assertTrue(isInt)

        isEqual = response == self.stepChainParams.get("nextVersion")
        self.assertTrue(isEqual)

        ### Test when task chain request
        response = self.redigiTaskChainWfController.getNextVersion()
        isInt = isinstance(response, int)
        self.assertTrue(isInt)

        isEqual = response == self.redigiTaskChainParams.get("nextVersion")
        self.assertTrue(isEqual)

    def testGetSummary(self) -> None:
        """getSummary gets the workload summary"""
        response = self.mcWfController.getSummary()
        isDict = isinstance(response, dict)
        self.assertTrue(isDict)

        isFound = response.get("campaign") == self.mcParams.get("campaign")
        self.assertTrue(isFound)

    def testCheckSplittingsSize(self) -> None:
        """checkSplitting checks the splittings"""
        ### Test when base request
        response = self.mcWfController.checkSplitting()
        isTuple = isinstance(response, tuple)
        self.assertTrue(isTuple)

        isBool = isinstance(response[0], bool)
        self.assertTrue(isBool)
        isFalse = not response[0]
        self.assertTrue(isFalse)

        isList = isinstance(response[1], list)
        self.assertTrue(isList)
        isEmpty = len(response[1]) == 0
        self.assertTrue(isEmpty)

        ### Test when step chain request
        response = self.stepChainWfController.checkSplitting()
        isTuple = isinstance(response, tuple)
        self.assertTrue(isTuple)

        isBool = isinstance(response[0], bool)
        self.assertTrue(isBool)
        isFalse = not response[0]
        self.assertTrue(isFalse)

        isList = isinstance(response[1], list)
        self.assertTrue(isList)
        isEmpty = len(response[1]) == 0
        self.assertTrue(isEmpty)

        ### Test when task chain request
        response = self.redigiTaskChainWfController.checkSplitting()
        isTuple = isinstance(response, tuple)
        self.assertTrue(isTuple)

        isBool = isinstance(response[0], bool)
        self.assertTrue(isBool)
        isFalse = not response[0]
        self.assertTrue(isFalse)

        isList = isinstance(response[1], list)
        self.assertTrue(isList)
        isEmpty = len(response[1]) == 0
        self.assertTrue(isEmpty)

    def testIsRelVal(self) -> None:
        """isRelVal checks if a request is relval"""
        ### Test when base request
        response = self.mcWfController.request.isRelVal()
        isBool = isinstance(response, bool)
        self.assertTrue(isBool)

        isFalse = not response
        self.assertTrue(isFalse)

        ### Test when step chain request
        response = self.stepChainWfController.request.isRelVal()
        isBool = isinstance(response, bool)
        self.assertTrue(isBool)

        isFalse = not response
        self.assertTrue(isFalse)

        ### Test when task chain request and rel val
        response = self.relvalTaskChainWfController.request.isRelVal()
        isBool = isinstance(response, bool)
        self.assertTrue(isBool)

        isTrue = response
        self.assertTrue(isTrue)

    def testIsProducingPremix(self) -> None:
        """isProducingPremix checks if a request is producing premix"""
        ### Test when base request
        response = self.mcWfController.request.isProducingPremix()
        isBool = isinstance(response, bool)
        self.assertTrue(isBool)

        isFalse = not response
        self.assertTrue(isFalse)

        ### Test when step chain request
        response = self.stepChainWfController.request.isProducingPremix()
        isBool = isinstance(response, bool)
        self.assertTrue(isBool)

        isFalse = not response
        self.assertTrue(isFalse)

        ### Test when task chain request
        response = self.redigiTaskChainWfController.request.isProducingPremix()
        isBool = isinstance(response, bool)
        self.assertTrue(isBool)

        isFalse = not response
        self.assertTrue(isFalse)

    def testIsGoodToConvertToStepChain(self) -> None:
        """isGoodToConvertToStepChain checks if a request is good to be converted to step chain"""
        ### Test when base request
        response = self.mcWfController.request.isGoodToConvertToStepChain()
        isBool = isinstance(response, bool)
        self.assertTrue(isBool)

        isFalse = not response
        self.assertTrue(isFalse)

        ### Test when step chain request
        response = self.stepChainWfController.request.isGoodToConvertToStepChain()
        isBool = isinstance(response, bool)
        self.assertTrue(isBool)

        isFalse = not response
        self.assertTrue(isFalse)

        ### Test when task chain request
        response = self.redigiTaskChainWfController.request.isGoodToConvertToStepChain()
        isBool = isinstance(response, bool)
        self.assertTrue(isBool)

        isFalse = not response
        self.assertTrue(isFalse)

    def testGetAcquisitionEra(self) -> None:
        """getAcquisitionEra gets the acquisition era"""
        ### Test when base request
        response = self.mcWfController.request.getAcquisitionEra()
        isStr = isinstance(response, str)
        self.assertTrue(isStr)

        isFound = response == self.mcParams.get("acquisitionEra")
        self.assertTrue(isFound)

        ### Test when step chain request
        response = self.stepChainWfController.request.getAcquisitionEra()
        isDict = isinstance(response, dict)
        self.assertTrue(isDict)

        isKeyStr = all(isinstance(k, str) for k in response)
        self.assertTrue(isKeyStr)

        isValueStr = all(isinstance(v, str) for v in response.values())
        self.assertTrue(isValueStr)

        isFound = False
        for k, v in response.items():
            if k not in self.stepChainParams.get("steps") or v != self.stepChainParams.get("acquisitionEra"):
                break
        else:
            isFound = True
        self.assertTrue(isFound)

        ### Test when task chain request
        response = self.redigiTaskChainWfController.request.getAcquisitionEra()
        isDict = isinstance(response, dict)
        self.assertTrue(isDict)

        isKeyStr = all(isinstance(k, str) for k in response)
        self.assertTrue(isKeyStr)

        isValueStr = all(isinstance(v, str) for v in response.values())
        self.assertTrue(isValueStr)

        isFound = False
        for k, v in response.items():
            if k not in self.redigiTaskChainParams.get("tasks") or v != self.redigiTaskChainParams.get("tasks").get(
                k
            ).get("acquisitionEra"):
                break
        else:
            isFound = True
        self.assertTrue(isFound)

    def testGetProcessingString(self) -> None:
        """getProcessingString gets the processing string"""
        ### Test when base request
        response = self.mcWfController.request.getProcessingString()
        isStr = isinstance(response, str)
        self.assertTrue(isStr)

        isFound = response == self.mcParams.get("processingString")
        self.assertTrue(isFound)

        ### Test when step chain request
        response = self.stepChainWfController.request.getProcessingString()
        isDict = isinstance(response, dict)
        self.assertTrue(isDict)

        isKeyStr = all(isinstance(k, str) for k in response)
        self.assertTrue(isKeyStr)

        isValueStr = all(isinstance(v, str) for v in response.values())
        self.assertTrue(isValueStr)

        isFound = False
        for k, v in response.items():
            if k not in self.stepChainParams.get("steps") or v != self.stepChainParams.get("processingString"):
                break
        else:
            isFound = True
        self.assertTrue(isFound)

        ### Test when task chain request
        response = self.redigiTaskChainWfController.request.getProcessingString()
        isDict = isinstance(response, dict)
        self.assertTrue(isDict)

        isKeyStr = all(isinstance(k, str) for k in response)
        self.assertTrue(isKeyStr)

        isValueStr = all(isinstance(v, str) for v in response.values())
        self.assertTrue(isValueStr)

        isFound = False
        for k, v in response.items():
            if k not in self.redigiTaskChainParams.get("tasks") or v != self.redigiTaskChainParams.get("tasks").get(
                k
            ).get("processingString"):
                break
        else:
            isFound = True
        self.assertTrue(isFound)

    def testGetMemory(self) -> None:
        """getMemory gets the memory"""
        ### Test when base request
        response = self.mcWfController.request.getMemory()
        isFloat = isinstance(response, float)
        self.assertTrue(isFloat)

        isEqual = response == self.mcParams.get("memory")
        self.assertTrue(isEqual)

        ### Test when step chain request
        response = self.stepChainWfController.request.getMemory()
        isFloat = isinstance(response, float)
        self.assertTrue(isFloat)

        isEqual = response == self.stepChainParams.get("memory")
        self.assertTrue(isEqual)

        ### Test when task chain request
        response = self.redigiTaskChainWfController.request.getMemory()
        isFloat = isinstance(response, float)
        self.assertTrue(isFloat)

        isEqual = response == self.redigiTaskChainParams.get("memory")
        self.assertTrue(isEqual)

    def testGetIO(self) -> None:
        """getIO gets the inputs/outputs"""
        ### Test when base request
        response = self.mcWfController.request.getIO()
        isTuple = isinstance(response, tuple)
        self.assertTrue(isTuple)

        isBool = isinstance(response[0], bool)
        self.assertTrue(isBool)
        isFalse = not response[0]
        self.assertTrue(isFalse)

        for io in response[1:]:
            isSet = isinstance(io, set)
            self.assertTrue(isSet)
            isEmpty = len(io) == 0
            self.assertTrue(isEmpty)

        ### Test when step chain request
        response = self.stepChainWfController.request.getIO()
        isTuple = isinstance(response, tuple)
        self.assertTrue(isTuple)

        isBool = isinstance(response[0], bool)
        self.assertTrue(isBool)
        isFalse = not response[0]
        self.assertTrue(isFalse)

        isSet = isinstance(response[1], set)
        self.assertTrue(isSet)
        isFound = list(response[1]) == self.stepChainParams.get("primary")
        self.assertTrue(isFound)

        isSet = isinstance(response[2], set)
        self.assertTrue(isSet)
        isEmpty = len(response[2]) == 0
        self.assertTrue(isEmpty)

        isSet = isinstance(response[3], set)
        self.assertTrue(isSet)
        isFound = list(response[3]) == self.stepChainParams.get("secondary")
        self.assertTrue(isFound)

        ### Test when task chain request
        response = self.redigiTaskChainWfController.request.getIO()
        isTuple = isinstance(response, tuple)
        self.assertTrue(isTuple)

        isBool = isinstance(response[0], bool)
        self.assertTrue(isBool)
        isFalse = not response[0]
        self.assertTrue(isFalse)

        for io in response[1:3]:
            isSet = isinstance(io, set)
            self.assertTrue(isSet)
            isEmpty = len(io) == 0
            self.assertTrue(isEmpty)

        isSet = isinstance(response[3], set)
        self.assertTrue(isSet)
        isFound = list(response[3]) == self.redigiTaskChainParams.get("secondary")
        self.assertTrue(isFound)

    def testGetMulticore(self) -> None:
        """getMulticore gets the multicore"""
        ### Test when base request
        # Test when maxOnly is True
        response = self.mcWfController.request.getMulticore()
        isInt = isinstance(response, int)
        self.assertTrue(isInt)

        isEqual = response == self.mcParams.get("multicore")
        self.assertTrue(isEqual)

        # Test when maxOnly is False
        response = self.mcWfController.request.getMulticore(maxOnly=False)
        isList = isinstance(response, list)
        self.assertTrue(isList)

        isListOfInt = isinstance(response[0], int)
        self.assertTrue(isListOfInt)

        isEqual = response[0] == self.mcParams.get("multicore")
        self.assertTrue(isEqual)

        ### Test when step chain request
        # Test when maxOnly is True
        response = self.stepChainWfController.request.getMulticore()
        isInt = isinstance(response, int)
        self.assertTrue(isInt)

        isEqual = response == self.stepChainParams.get("multicore")
        self.assertTrue(isEqual)

        # Test when maxOnly is False
        response = self.stepChainWfController.request.getMulticore(maxOnly=False)
        isList = isinstance(response, list)
        self.assertTrue(isList)

        isListOfInt = isinstance(response[0], int)
        self.assertTrue(isListOfInt)

        isEqual = response[0] == self.stepChainParams.get("multicore")
        self.assertTrue(isEqual)

        ### Test when task chain request
        # Test when maxOnly is True
        response = self.redigiTaskChainWfController.request.getMulticore()
        isInt = isinstance(response, int)
        self.assertTrue(isInt)

        isEqual = response == max(self.redigiTaskChainParams.get("multicore"))
        self.assertTrue(isEqual)

        # Test when maxOnly is False
        response = self.redigiTaskChainWfController.request.getMulticore(maxOnly=False)
        isList = isinstance(response, list)
        self.assertTrue(isList)

        isListOfInt = isinstance(response[0], int)
        self.assertTrue(isListOfInt)

        isEqual = Counter(response) == Counter(self.redigiTaskChainParams.get("multicore"))
        self.assertTrue(isEqual)

    def testGetRequestNumEvents(self) -> None:
        """getRequestNumEvents gets the number of events requested"""
        ### Test when base request
        response = self.mcWfController.request.getRequestNumEvents()
        isInt = isinstance(response, int)
        self.assertTrue(isInt)

        isEqual = response == self.mcParams.get("requestNumEvents")
        self.assertTrue(isEqual)

        ### Test when step chain request
        response = self.stepChainWfController.request.getRequestNumEvents()
        isInt = isinstance(response, int)
        self.assertTrue(isInt)

        isZero = response == 0
        self.assertTrue(isZero)

        ### Test when task chain request
        response = self.redigiTaskChainWfController.request.getRequestNumEvents()
        isInt = isinstance(response, int)
        self.assertTrue(isInt)

        isEqual = response == self.redigiTaskChainParams.get("requestNumEvents")
        self.assertTrue(isEqual)

    def testGetCampaigns(self) -> None:
        """getCampaigns gets the campaigns"""
        ### Test when base request
        # Test when details is True
        response = self.mcWfController.request.getCampaigns()
        isStr = isinstance(response, str)
        self.assertTrue(isStr)

        isFound = response == self.mcParams.get("campaign")
        self.assertTrue(isFound)

        # Test when details is False
        response = self.mcWfController.request.getCampaigns(details=False)
        isList = isinstance(response, list)
        self.assertTrue(isList)

        isListOfStr = isinstance(response[0], str)
        self.assertTrue(isListOfStr)

        isFound = response[0] == self.mcParams.get("campaign")
        self.assertTrue(isFound)

        ### Test when step chain request
        # Test when details is True
        response = self.stepChainWfController.request.getCampaigns()
        isDict = isinstance(response, dict)
        self.assertTrue(isDict)

        isKeyStr = all(isinstance(k, str) for k in response)
        self.assertTrue(isKeyStr)

        isValueStr = all(isinstance(v, str) for v in response.values())
        self.assertTrue(isValueStr)

        isFound = False
        for k, v in response.items():
            if k not in self.stepChainParams.get("steps") or v != self.stepChainParams.get("campaign"):
                break
        else:
            isFound = True
        self.assertTrue(isFound)

        # Test when details is False
        response = self.stepChainWfController.request.getCampaigns(details=False)
        isList = isinstance(response, list)
        self.assertTrue(isList)

        isListOfStr = isinstance(response[0], str)
        self.assertTrue(isListOfStr)

        isFound = response[0] == self.stepChainParams.get("campaign")
        self.assertTrue(isFound)

        ### Test when task chain request
        # Test when details is True
        response = self.redigiTaskChainWfController.request.getCampaigns()
        isDict = isinstance(response, dict)
        self.assertTrue(isDict)

        isKeyStr = all(isinstance(k, str) for k in response)
        self.assertTrue(isKeyStr)

        isValueStr = all(isinstance(v, str) for v in response.values())
        self.assertTrue(isValueStr)

        isFound = False
        for k, v in response.items():
            if k not in self.redigiTaskChainParams.get("tasks") or v != self.redigiTaskChainParams.get("tasks").get(
                k
            ).get("acquisitionEra"):
                break
        else:
            isFound = True
        self.assertTrue(isFound)

        # Test when details is False
        response = self.redigiTaskChainWfController.request.getCampaigns(details=False)
        isList = isinstance(response, list)
        self.assertTrue(isList)

        isListOfStr = isinstance(response[0], str)
        self.assertTrue(isListOfStr)

        isFound = False
        for _, v in self.redigiTaskChainParams.get("tasks").items():
            if v.get("acquisitionEra") not in response:
                break
        else:
            isFound = True
        self.assertTrue(isFound)

    def testGetCampaignsAndLabels(self) -> None:
        """getCampaignsAndLabels gets a list of campaigns and labels"""
        ### Test when base request
        response = self.mcWfController.request.getCampaignsAndLabels()
        isList = isinstance(response, list)
        self.assertTrue(isList)

        isListOfTuple = isinstance(response[0], tuple)
        self.assertTrue(isListOfTuple)

        isFound = response[0][0] == self.mcParams.get("campaign")
        self.assertTrue(isFound)

        isFound = response[0][1] == self.mcParams.get("processingString")
        self.assertTrue(isFound)

        ### Test when step chain request
        response = self.stepChainWfController.request.getCampaignsAndLabels()
        isList = isinstance(response, list)
        self.assertTrue(isList)

        isListOfTuple = isinstance(response[0], tuple)
        self.assertTrue(isListOfTuple)

        isFound = response[0][0] == self.stepChainParams.get("campaign")
        self.assertTrue(isFound)

        isFound = response[0][1] == self.stepChainParams.get("processingString")
        self.assertTrue(isFound)

        ### Test when task chain request
        response = self.redigiTaskChainWfController.request.getCampaignsAndLabels()
        isList = isinstance(response, list)
        self.assertTrue(isList)

        isListOfTuple = isinstance(response[0], tuple)
        self.assertTrue(isListOfTuple)

        isFound = False
        for task in self.redigiTaskChainParams.get("tasks").values():
            if (task.get("acquisitionEra"), task.get("processingString")) not in response:
                break
        else:
            isFound = True
        self.assertTrue(isFound)

    def testHasAcceptableEfficiency(self) -> None:
        """hasAcceptableEfficiency checks if TaskChain has acceptable efficiency"""
        response = self.redigiTaskChainWfController.request._hasAcceptableEfficiency()
        isBool = isinstance(response, bool)
        self.assertTrue(isBool)

        isFalse = not response
        self.assertTrue(isFalse)

    def testGetTimeInfo(self) -> None:
        """_getTimeInfo gets the time info for a task chain request"""
        response = self.redigiTaskChainWfController.request._getTimeInfo()
        isDict = isinstance(response, dict)
        self.assertTrue(isDict)

        isValueDict = all(isinstance(v, dict) for v in response.values())
        self.assertTrue(isValueDict)

        total = sum([v["timePerEvent"] for v in response.values()])
        isEqual = math.isclose(total, self.redigiTaskChainParams.get("totalTimePerEvent"))
        self.assertTrue(isEqual)

    def testGetChainValues(self) -> None:
        """getChainValues gets the values in the chain"""
        response = self.relvalTaskChainWfController.request.getChainValues("Campaign")
        isDict = isinstance(response, dict)
        self.assertTrue(isDict)

        isKeyStr = all(isinstance(k, str) for k in response)
        self.assertTrue(isKeyStr)

        isValueStr = all(isinstance(v, str) for v in response.values())
        self.assertTrue(isValueStr)

        isFound = self.relvalTaskChainParams.get("campaign") in response.values()
        self.assertTrue(isFound)


if __name__ == "__main__":
    unittest.main()
