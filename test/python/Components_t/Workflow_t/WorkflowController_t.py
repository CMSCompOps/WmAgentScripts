"""
_WorkflowController_t_
Unit test for WorkflowController helper class.
"""

import unittest
import math
from collections import Counter

from Components.Workflow.WorkflowController import WorkflowController

from Components.Workload.NonChainWorkloadHandler import NonChainWorkloadHandler
from Components.Workload.StepChainWorkloadHandler import StepChainWorkloadHandler
from Components.Workload.TaskChainWorkloadHandler import TaskChainWorkloadHandler


class WorkflowControllerTest(unittest.TestCase):
    nonChainParams = {
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
    }

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
        "steps": ["TSG-Run3Winter21DRMiniAOD-00081_0", "TSG-Run3Winter21DRMiniAOD-00081_1"],
        "primary": [
            "/VectorZPrimeToQQ_M-100_Pt-300_TuneCP5_14TeV-madgraph-pythia8/Run3Winter21wmLHEGS-112X_mcRun3_2021_realistic_v15-v2/GEN-SIM"
        ],
        "secondary": ["/MinBias_TuneCP5_14TeV-pythia8/Run3Winter21GS-112X_mcRun3_2021_realistic_v15-v1/GEN-SIM"],
    }

    taskChainParams = {
        "workflow": "pdmvserv_task_BPH-RunIIFall18GS-00350__v1_T_201021_154340_8354",
        "requestType": "TaskChain",
        "scramArch": "slc6_amd64_gcc700",
        "memory": 14700,
        "multicore": [8, 8, 8, 1],
        "requestNumEvents": 5000000,
        "cpuSec": 175077.33927660278,
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
    }

    def setUp(self) -> None:
        self.nonChainWfController = WorkflowController(self.nonChainParams.get("workflow"))
        self.stepChainWfController = WorkflowController(self.stepChainParams.get("workflow"))
        self.taskChainWfController = WorkflowController(self.taskChainParams.get("workflow"))
        super().setUp()
        return

    def tearDown(self) -> None:
        super().tearDown()
        return

    def testWorkloadInterface(self) -> None:
        """WorkloadInterface gets the request data handler"""
        ### Test when non-chain request
        isNonChain = isinstance(self.nonChainWfController.request, NonChainWorkloadHandler)
        self.assertTrue(isNonChain)

        ### Test when step chain request
        isStepChain = isinstance(self.stepChainWfController.request, StepChainWorkloadHandler)
        self.assertTrue(isStepChain)

        ### Test when task chain request
        isTaskChain = isinstance(self.taskChainWfController.request, TaskChainWorkloadHandler)
        self.assertTrue(isTaskChain)

    def testIsRelVal(self) -> None:
        """isRelVal checks if a request is relval"""
        ### Test when non-chain request
        response = self.nonChainWfController.request.isRelVal()
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

        ### Test when task chain request
        response = self.taskChainWfController.request.isRelVal()
        isBool = isinstance(response, bool)
        self.assertTrue(isBool)

        isFalse = not response
        self.assertTrue(isFalse)

    def testIsProducingPremix(self) -> None:
        """isProducingPremix checks if a request is producing premix"""
        ### Test when non-chain request
        response = self.nonChainWfController.request.isProducingPremix()
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
        response = self.taskChainWfController.request.isProducingPremix()
        isBool = isinstance(response, bool)
        self.assertTrue(isBool)

        isFalse = not response
        self.assertTrue(isFalse)

    def testIsGoodToConvertToStepChain(self) -> None:
        """isGoodToConvertToStepChain checks if a request is good to be converted to step chain"""
        ### Test when non-chain request
        response = self.nonChainWfController.request.isGoodToConvertToStepChain()
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
        response = self.taskChainWfController.request.isGoodToConvertToStepChain()
        isBool = isinstance(response, bool)
        self.assertTrue(isBool)

        isFalse = not response
        self.assertTrue(isFalse)

    def testHasAcceptableEfficiency(self) -> None:
        """hasAcceptableEfficiency checks if TaskChain has acceptable efficiency"""
        response = self.taskChainWfController.request._hasAcceptableEfficiency()
        isBool = isinstance(response, bool)
        self.assertTrue(isBool)

        isFalse = not response
        self.assertTrue(isFalse)

    def testGetAcquisitionEra(self) -> None:
        """getAcquisitionEra gets the acquisition era"""
        ### Test when non-chain request
        response = self.nonChainWfController.request.getAcquisitionEra()
        isStr = isinstance(response, str)
        self.assertTrue(isStr)

        isFound = response == self.nonChainParams.get("acquisitionEra")
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
        response = self.taskChainWfController.request.getAcquisitionEra()
        isDict = isinstance(response, dict)
        self.assertTrue(isDict)

        isKeyStr = all(isinstance(k, str) for k in response)
        self.assertTrue(isKeyStr)

        isValueStr = all(isinstance(v, str) for v in response.values())
        self.assertTrue(isValueStr)

        isFound = False
        for k, v in response.items():
            if k not in self.taskChainParams.get("tasks") or v != self.taskChainParams.get("tasks").get(k).get(
                "acquisitionEra"
            ):
                break
        else:
            isFound = True
        self.assertTrue(isFound)

    def testGetProcessingString(self) -> None:
        """getProcessingString gets the processing string"""
        ### Test when non-chain request
        response = self.nonChainWfController.request.getProcessingString()
        isStr = isinstance(response, str)
        self.assertTrue(isStr)

        isFound = response == self.nonChainParams.get("processingString")
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
        response = self.taskChainWfController.request.getProcessingString()
        isDict = isinstance(response, dict)
        self.assertTrue(isDict)

        isKeyStr = all(isinstance(k, str) for k in response)
        self.assertTrue(isKeyStr)

        isValueStr = all(isinstance(v, str) for v in response.values())
        self.assertTrue(isValueStr)

        isFound = False
        for k, v in response.items():
            if k not in self.taskChainParams.get("tasks") or v != self.taskChainParams.get("tasks").get(k).get(
                "processingString"
            ):
                break
        else:
            isFound = True
        self.assertTrue(isFound)

    def testGetMemory(self) -> None:
        """getMemory gets the memory"""
        ### Test when non-chain request
        response = self.nonChainWfController.request.getMemory()
        isFloat = isinstance(response, float)
        self.assertTrue(isFloat)

        isEqual = response == self.nonChainParams.get("memory")
        self.assertTrue(isEqual)

        ### Test when step chain request
        response = self.stepChainParams.request.getMemory()
        isFloat = isinstance(response, float)
        self.assertTrue(isFloat)

        isEqual = response == self.stepChainParams.get("memory")
        self.assertTrue(isEqual)

        ### Test when task chain request
        response = self.taskChainWfController.request.getMemory()
        isFloat = isinstance(response, float)
        self.assertTrue(isFloat)

        isEqual = response == self.taskChainParams.get("memory")
        self.assertTrue(isEqual)

    def testGetIO(self) -> None:
        """getIO gets the inputs/outputs"""
        ### Test when non-chain request
        response = self.nonChainWfController.request.getIO()
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
        isEmpty = len(response[2][0]) == 0
        self.assertTrue(isEmpty)

        isSet = isinstance(response[3], set)
        self.assertTrue(isSet)
        isFound = list(response[3]) == self.stepChainParams.get("secondary")
        self.assertTrue(isFound)

        ### Test when task chain request
        response = self.taskChainWfController.request.getIO()
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
        isFound = list(response[3]) == self.taskChainParams.get("secondary")
        self.assertTrue(isFound)

    def testGetMulticore(self) -> None:
        """getMulticore gets the multicore"""
        ### Test when non-chain request
        # Test when maxOnly is True
        response = self.nonChainWfController.request.getMulticore()
        isInt = isinstance(response, int)
        self.assertTrue(isInt)

        isEqual = response == self.nonChainParams.get("multicore")
        self.assertTrue(isEqual)

        # Test when maxOnly is False
        response = self.nonChainWfController.request.getMulticore(maxOnly=False)
        isList = isinstance(response, list)
        self.assertTrue(isList)

        isListOfInt = isinstance(response[0], int)
        self.assertTrue(isListOfInt)

        isEqual = response[0] == self.nonChainParams.get("multicore")
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
        response = self.taskChainWfController.request.getMulticore()
        isInt = isinstance(response, int)
        self.assertTrue(isInt)

        isEqual = response == max(self.taskChainParams.get("multicore"))
        self.assertTrue(isEqual)

        # Test when maxOnly is False
        response = self.taskChainWfController.request.getMulticore(maxOnly=False)
        isList = isinstance(response, list)
        self.assertTrue(isList)

        isListOfInt = isinstance(response[0], int)
        self.assertTrue(isListOfInt)

        isEqual = Counter(response) == Counter(self.taskChainParams.get("multicore"))
        self.assertTrue(isEqual)

    def testGetRequestNumEvents(self) -> None:
        """getRequestNumEvents gets the number of events requested"""
        ### Test when non-chain request
        response = self.nonChainWfController.request.getRequestNumEvents()
        isInt = isinstance(response, int)
        self.assertTrue(isInt)

        isEqual = response == self.nonChainParams.get("requestNumEvents")
        self.assertTrue(isEqual)

        ### Test when step chain request
        response = self.stepChainParams.request.getRequestNumEvents()
        isInt = isinstance(response, int)
        self.assertTrue(isInt)

        isZero = response == 0
        self.assertTrue(isZero)

        ### Test when task chain request
        response = self.taskChainWfController.request.getRequestNumEvents()
        isInt = isinstance(response, int)
        self.assertTrue(isInt)

        isEqual = response == self.taskChainParams.get("requestNumEvents")
        self.assertTrue(isEqual)

    def testGetCampaigns(self) -> None:
        """getCampaigns gets the campaigns"""
        ### Test when non-chain request
        # Test when details is True
        response = self.nonChainWfController.request.getCampaigns()
        isStr = isinstance(response, int)
        self.assertTrue(isStr)

        isFound = response == self.nonChainParams.get("campaign")
        self.assertTrue(isFound)

        # Test when details is False
        response = self.nonChainWfController.request.getCampaigns(details=False)
        isList = isinstance(response, list)
        self.assertTrue(isList)

        isListOfStr = isinstance(response[0], str)
        self.assertTrue(isListOfStr)

        isFound = response[0] == self.nonChainParams.get("campaign")
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
        response = self.taskChainWfController.request.getCampaigns()
        isDict = isinstance(response, dict)
        self.assertTrue(isDict)

        isKeyStr = all(isinstance(k, str) for k in response)
        self.assertTrue(isKeyStr)

        isValueStr = all(isinstance(v, str) for v in response.values())
        self.assertTrue(isValueStr)

        isFound = False
        for k, v in response.items():
            if k not in self.taskChainParams.get("tasks") or v != self.taskChainParams.get("tasks").get(k).get(
                "acquisitionEra"
            ):
                break
        else:
            isFound = True
        self.assertTrue(isFound)

        # Test when details is False
        response = self.taskChainWfController.request.getCampaigns(details=False)
        isList = isinstance(response, list)
        self.assertTrue(isList)

        isListOfStr = isinstance(response[0], str)
        self.assertTrue(isListOfStr)

        isFound = False
        for _, v in self.taskChainParams.get("tasks").items():
            if v.get("acquisitionEra") not in response:
                break
        else:
            isFound = True
        self.assertTrue(isFound)

    def testGetCampaignsAndLabels(self) -> None:
        """getCampaignsAndLabels gets a list of campaigns and labels"""
        ### Test when non-chain request
        response = self.nonChainWfController.request.getCampaignsAndLabels()
        isList = isinstance(response, list)
        self.assertTrue(isList)

        isListOfTuple = isinstance(response[0], tuple)
        self.assertTrue(isListOfTuple)

        isFound = response[0][0] == self.nonChainParams.get("campaign")
        self.assertTrue(isFound)

        isFound = response[0][1] == self.nonChainParams.get("processingString")
        self.assertTrue(isFound)

        ### Test when step chain request
        response = self.stepChainWfController.request.getCampaignsAndLabels()
        isList = isinstance(response, list)
        self.assertTrue(isList)

        isListOfTuple = isinstance(response[0], tuple)
        self.assertTrue(isListOfTuple)

        isFound = response[0][0] == self.nonChainParams.get("campaign")
        self.assertTrue(isFound)

        isFound = response[0][1] == self.nonChainParams.get("processingString")
        self.assertTrue(isFound)

        ### Test when task chain request
        response = self.taskChainWfController.request.getCampaignsAndLabels()
        isList = isinstance(response, list)
        self.assertTrue(isList)

        isListOfTuple = isinstance(response[0], tuple)
        self.assertTrue(isListOfTuple)

        isFound = False
        for task in self.taskChainParams.get("tasks").values():
            if (task.get("acquisitionEra"), task.get("processingString")) not in response:
                break
        else:
            isFound = True
        self.assertTrue(isFound)

    def testIsHeavyToRead(self) -> None:
        """isHeavyToRead checks if it is heavy to read"""
        # Test when response is True
        response = self.stepChainWfController.isHeavyToRead(self.taskChainParams.get("secondary"))
        isBool = isinstance(response, bool)
        self.assertTrue(isBool)

        isTrue = response
        self.assertTrue(isTrue)

        # Test when response is False
        response = self.taskChainWfController.isHeavyToRead(self.taskChainParams.get("secondary"))
        isBool = isinstance(response, bool)
        self.assertTrue(isBool)

        isFalse = not response
        self.assertTrue(isFalse)

    def testGetCampaignByTask(self) -> None:
        """getCampaignByTask gets the campaigns for a given task"""
        ### Test when non-chain request
        response = self.nonChainWfController.getCampaignByTask("")
        isStr = isinstance(response, str)
        self.assertTrue(isStr)

        isFound = response == self.nonChainParams.get("campaign")
        self.assertTrue(isFound)

        ### Test when step chain request
        task = self.stepChainParams.get("steps")[0]
        response = self.stepChainWfController.getCampaignByTask(task)
        isStr = isinstance(response, str)
        self.assertTrue(isStr)

        isFound = response == self.stepChainParams.get("campaign")
        self.assertTrue(isFound)

        ### Test when task chain request
        task = list(self.taskChainParams.get("tasks").keys())[0]
        response = self.taskChainWfController.getCampaignByTask(task)
        isStr = isinstance(response, str)
        self.assertTrue(isStr)

        isFound = response == self.taskChainParams.get("tasks").get(task).get("campaign")
        self.assertTrue(isFound)

    def testGetMemoryByTask(self) -> None:
        """getMemoryByTask gets the memory for a given task"""
        ### Test when non-chain request
        response = self.nonChainWfController.getMemoryByTask("")
        isInt = isinstance(response, int)
        self.assertTrue(isInt)

        isEqual = response == self.nonChainParams.get("memory")
        self.assertTrue(isEqual)

        ### Test when step chain request
        task = self.stepChainParams.get("steps")[0]
        response = self.stepChainWfController.getMemoryByTask(task)
        isInt = isinstance(response, int)
        self.assertTrue(isInt)

        isEqual = response == self.stepChainParams.get("memory")
        self.assertTrue(isEqual)

        ### Test when task chain request
        task = list(self.taskChainParams.get("tasks").keys())[0]
        response = self.taskChainWfController.getMemoryByTask(task)
        isInt = isinstance(response, int)
        self.assertTrue(isInt)

        isEqual = response == self.taskChainParams.get("tasks").get(task).get("memory")
        self.assertTrue(isEqual)

    def testGetCoreByTask(self) -> None:
        """getCoreByTask gets the memory for a given task"""
        ### Test when non-chain request
        response = self.nonChainWfController.getCoreByTask("")
        isInt = isinstance(response, int)
        self.assertTrue(isInt)

        isEqual = response == self.nonChainParams.get("multicore")
        self.assertTrue(isEqual)

        ### Test when step chain request
        task = self.stepChainParams.get("steps")[0]
        response = self.stepChainWfController.getCoreByTask(task)
        isInt = isinstance(response, int)
        self.assertTrue(isInt)

        isEqual = response == self.stepChainParams.get("multicore")
        self.assertTrue(isEqual)

        ### Test when task chain request
        task = list(self.taskChainParams.get("tasks").keys())[0]
        response = self.taskChainWfController.getCoreByTask(task)
        isInt = isinstance(response, int)
        self.assertTrue(isInt)

        isEqual = response == self.taskChainParams.get("tasks").get(task).get("multicore")
        self.assertTrue(isEqual)

    def testGetFilterEfficiencyByTask(self) -> None:
        """getFilterEfficiencyByTask gets the filter efficiency for a given task"""
        ### Test when non-chain request
        response = self.nonChainWfController.getFilterEfficiencyByTask("")
        isFloat = isinstance(response, float)
        self.assertTrue(isFloat)

        isEqual = response == self.nonChainParams.get("filterEfficiency")
        self.assertTrue(isEqual)

        ### Test when step chain request
        task = self.stepChainParams.get("steps")[0]
        response = self.stepChainWfController.getFilterEfficiencyByTask(task)
        isFloat = isinstance(response, float)
        self.assertTrue(isFloat)

        isEqual = response == self.stepChainParams.get("filterEfficiency")
        self.assertTrue(isEqual)

        ### Test when task chain request
        task = list(self.taskChainParams.get("tasks").keys())[-1]
        response = self.taskChainWfController.getFilterEfficiencyByTask(task)
        isFloat = isinstance(response, float)
        self.assertTrue(isFloat)

        isEqual = response == self.taskChainParams.get("tasks").get(task).get("filterEfficiency")
        self.assertTrue(isEqual)

    def testGetLumiWhiteList(self) -> None:
        """getLumiWhiteList gets the lumi white list"""
        ### Test when non-chain request
        lumiList = self.nonChainWfController.getLumiWhiteList()
        isList = isinstance(lumiList, list)
        self.assertTrue(isList)

        isEmpty = len(lumiList) == 0
        self.assertTrue(isEmpty)

        ### Test when step chain request
        lumiList = self.stepChainWfController.getLumiWhiteList()
        isList = isinstance(lumiList, list)
        self.assertTrue(isList)

        isEmpty = len(lumiList) == 0
        self.assertTrue(isEmpty)

        ### Test when task chain request
        lumiList = self.taskChainWfController.getLumiWhiteList()
        isList = isinstance(lumiList, list)
        self.assertTrue(isList)

        isEmpty = len(lumiList) == 0
        self.assertTrue(isEmpty)

    def testGetBlockWhiteList(self) -> None:
        """getBlockWhiteList gets the block white list"""
        ### Test when non-chain request
        blockList = self.nonChainWfController.getBlockWhiteList()
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
        blockList = self.taskChainWfController.getBlockWhiteList()
        isList = isinstance(blockList, list)
        self.assertTrue(isList)

        isEmpty = len(blockList) == 0
        self.assertTrue(isEmpty)

    def testGetRunWhiteList(self) -> None:
        """getRunWhiteList gets the run white list"""
        ### Test when non-chain request
        runList = self.nonChainWfController.getRunWhiteList()
        isList = isinstance(runList, list)
        self.assertTrue(isList)

        isEmpty = len(runList) == 0
        self.assertTrue(isEmpty)

        ### Test when step chain request
        runList = self.stepChainWfController.getRunWhiteList()
        isList = isinstance(runList, list)
        self.assertTrue(isList)

        isEmpty = len(runList) == 0
        self.assertTrue(isEmpty)

        ### Test when task chain request
        runList = self.taskChainWfController.getRunWhiteList()
        isList = isinstance(runList, list)
        self.assertTrue(isList)

        isEmpty = len(runList) == 0
        self.assertTrue(isEmpty)

    def testGetPrepIds(self) -> None:
        """getPrepIds gets the prep ids"""
        ### Test when non-chain request
        response = self.nonChainWfController.getPrepIds()
        isList = isinstance(response, list)
        self.assertTrue(isList)

        isListOfStr = isinstance(response[0], str)
        self.assertTrue(isListOfStr)

        isFound = response[0] == self.nonChainWfController.get("prepId")
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
        response = self.taskChainWfController.getPrepIds()
        isList = isinstance(response, list)
        self.assertTrue(isList)

        isListOfStr = isinstance(response[0], str)
        self.assertTrue(isListOfStr)

        isFound = False
        for task in self.taskChainParams.get("tasks").values():
            if task.get("prepId") not in response:
                break
        else:
            isFound = True
        self.assertTrue(isFound)

    def testGetScramArches(self) -> None:
        """getScramArches gets the arches"""
        ### Test when non-chain request
        response = self.nonChainWfController.getScramArches()
        isList = isinstance(response, list)
        self.assertTrue(isList)

        isListOfStr = isinstance(response[0], str)
        self.assertTrue(isListOfStr)

        isFound = response[0] == self.nonChainWfController.get("scramArch")
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
        response = self.taskChainWfController.getScramArches()
        isList = isinstance(response, list)
        self.assertTrue(isList)

        isListOfStr = isinstance(response[0], str)
        self.assertTrue(isListOfStr)

        isFound = response[0] == self.taskChainParams.get("scramArch")
        self.assertTrue(isFound)

    def testGetComputingTime(self) -> None:
        """getComputingTime gets the computing time"""
        ### Test when non-chain request
        response = self.nonChainWfController.getComputingTime(unit="s")
        isFloat = isinstance(response, float)
        self.assertTrue(isFloat)

        isEqual = response == self.nonChainWfController.get("cpuSec")
        self.assertTrue(isEqual)

        ### Test when step chain request
        ### TODO

        ### Test when task chain request
        response = self.taskChainWfController.getComputingTime(unit="s")
        isFloat = isinstance(response, float)
        self.assertTrue(isFloat)

        isEqual = math.isclose(response, self.taskChainParams.get("cpuSec"))
        self.assertTrue(isEqual)

    def testGetBlocks(self) -> None:
        """getBlocks gets the blocks"""
        ### Test when non-chain request
        blocks = self.nonChainWfController.getBlocks()
        isList = isinstance(blocks, list)
        self.assertTrue(isList)

        isEmpty = len(blocks) == 0
        self.assertTrue(isEmpty)

        ### Test when step chain request
        blocks = self.stepChainWfController.getBlocks()
        isList = isinstance(blocks, list)
        self.assertTrue(isList)

        isEmpty = len(blocks) == 0
        self.assertTrue(isEmpty)

        ### Test when task chain request
        blocks = self.taskChainWfController.getBlocks()
        isList = isinstance(blocks, list)
        self.assertTrue(isList)

        isEmpty = len(blocks) == 0
        self.assertTrue(isEmpty)

    def testGetBlowupFactor(self) -> None:
        """getBlowupFactor gets the blocks"""
        ### Test when non-chain request
        response = self.nonChainWfController.getBlowupFactor()
        isFloat = isinstance(response, list)
        self.assertTrue(isFloat)

        isEqual = response == 1
        self.assertTrue(isEqual)

        ### Test when step chain request
        response = self.stepChainWfController.getBlowupFactor()
        isFloat = isinstance(response, list)
        self.assertTrue(isFloat)

        isEqual = response == 1
        self.assertTrue(isEqual)

        ### Test when task chain request
        ### TODO

    def testGetNCopies(self) -> None:
        """getNCopies gets the number of needed copies"""
        response = self.nonChainWfController.getNCopies(self.nonChainParams.get("cpuSec") / 3600.0)
        isInt = isinstance(response, int)
        self.assertTrue(isInt)

        isEqual = response == self.nonChainParams.get("neededCopies")
        self.assertTrue(isEqual)


if __name__ == "__main__":
    unittest.main()
