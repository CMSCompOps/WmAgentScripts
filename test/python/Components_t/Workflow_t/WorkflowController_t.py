"""
_WorkflowController_t_
Unit test for WorkflowController helper class.
"""

import unittest

from Components.Workflow.WorkflowController import WorkflowController

from Components.RequestData.NonChainRequestDataHandler import NonChainRequestDataHandler
from Components.RequestData.StepChainRequestDataHandler import StepChainRequestDataHandler
from Components.RequestData.TaskChainRequestDataHandler import TaskChainRequestDataHandler


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
        "requestType": "StepChain",
    }

    taskChainParams = {
        "workflow": "pdmvserv_task_BPH-RunIIFall18GS-00350__v1_T_201021_154340_8354",
        "requestType": "TaskChain",
        "secondary": [
            "/Neutrino_E-10_gun/RunIISummer17PrePremix-PUAutumn18_102X_upgrade2018_realistic_v15-v1/GEN-SIM-DIGI-RAW"
        ],
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

    def testRequestDataInterface(self) -> None:
        """RequestDataInterface gets the request data handler"""
        ### Test when non-chain request
        isNonChain = isinstance(self.nonChainWfController.request, NonChainRequestDataHandler)
        self.assertTrue(isNonChain)

        ### Test when step chain request
        isStepChain = isinstance(self.stepChainWfController.request, StepChainRequestDataHandler)
        self.assertTrue(isStepChain)

        ### Test when task chain request
        isTaskChain = isinstance(self.taskChainWfController.request, TaskChainRequestDataHandler)
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
        ### TODO

    def testGetAcquisitionEra(self) -> None:
        """getAcquisitionEra gets the acquisition era"""
        ### Test when non-chain request
        response = self.nonChainWfController.request.getAcquisitionEra()
        isStr = isinstance(response, str)
        self.assertTrue(isStr)

        isFound = response == self.nonChainParams.get("acquisitionEra")
        self.assertTrue(isFound)

        ### Test when step chain request
        ### TODO

        ### Test when task chain request
        ### TODO

    def testGetProcessingString(self) -> None:
        """getProcessingString gets the processing string"""
        ### Test when non-chain request
        response = self.nonChainWfController.request.getProcessingString()
        isStr = isinstance(response, str)
        self.assertTrue(isStr)

        isFound = response == self.nonChainParams.get("processingString")
        self.assertTrue(isFound)

        ### Test when step chain request
        ### TODO

        ### Test when task chain request
        ### TODO

    def testGetMemory(self) -> None:
        """getMemory gets the memory"""
        ### Test when non-chain request
        response = self.nonChainWfController.request.getMemory()
        isInt = isinstance(response, int)
        self.assertTrue(isInt)

        isEqual = response == self.nonChainParams.get("memory")
        self.assertTrue(isEqual)

        ### Test when step chain request
        ### TODO

        ### Test when task chain request
        ### TODO

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
        ### TODO

        ### Test when task chain request
        ### TODO

    def testGetMulticore(self) -> None:
        """getMulticore gets the multicore"""
        ### Test when non-chain request
        # Test when details is False
        response = self.nonChainWfController.request.getMulticore()
        isInt = isinstance(response, int)
        self.assertTrue(isInt)

        isEqual = response == self.nonChainParams.get("multicore")
        self.assertTrue(isEqual)

        # Test when details is True
        response = self.nonChainWfController.request.getMulticore(details=True)
        isList = isinstance(response, list)
        self.assertTrue(isList)

        isListOfInt = isinstance(response[0], int)
        self.assertTrue(isListOfInt)

        isEqual = response[0] == self.nonChainParams.get("multicore")
        self.assertTrue(isEqual)

        ### Test when step chain request
        ### TODO

        ### Test when task chain request
        ### TODO

    def testGetEvents(self) -> None:
        """getEvents gets the number of events requested"""
        ### Test when non-chain request
        response = self.nonChainWfController.request.getEvents()
        isInt = isinstance(response, int)
        self.assertTrue(isInt)

        isEqual = response == self.nonChainParams.get("requestNumEvents")
        self.assertTrue(isEqual)

        ### Test when step chain request
        ### TODO

        ### Test when task chain request
        ### TODO

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
        ### TODO

        ### Test when task chain request
        ### TODO

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
        ### TODO

        ### Test when task chain request
        ### TODO

    def testIsHeavyToRead(self) -> None:
        """isHeavyToRead checks if it is heavy to read"""
        response = self.taskChainWfController.isHeavyToRead(self.taskChainParams.get("secondary"))
        isBool = isinstance(response, bool)
        self.assertTrue(isBool)

        isFalse = not response
        self.assertTrue(isFalse)

    def testGetCampaignByTask(self) -> None:
        """getCampaignByTask gets the campaigns for a given task"""
        ### Test when non-chain request
        response = self.nonChainWfController.getCampaignByTask("")
        isStr = isinstance(response, int)
        self.assertTrue(isStr)

        isFound = response == self.nonChainParams.get("campaign")
        self.assertTrue(isFound)

        ### Test when step chain request
        ### TODO

        ### Test when task chain request
        ### TODO

    def testGetMemoryByTask(self) -> None:
        """getMemoryByTask gets the memory for a given task"""
        ### Test when non-chain request
        response = self.nonChainWfController.getMemoryByTask("")
        isInt = isinstance(response, int)
        self.assertTrue(isInt)

        isEqual = response == self.nonChainParams.get("memory")
        self.assertTrue(isEqual)

        ### Test when step chain request
        ### TODO

        ### Test when task chain request
        ### TODO

    def testGetCoreByTask(self) -> None:
        """getCoreByTask gets the memory for a given task"""
        ### Test when non-chain request
        response = self.nonChainWfController.getCoreByTask("")
        isInt = isinstance(response, int)
        self.assertTrue(isInt)

        isEqual = response == self.nonChainParams.get("multicore")
        self.assertTrue(isEqual)

        ### Test when step chain request
        ### TODO

        ### Test when task chain request
        ### TODO

    def testGetFilterEfficiencyByTask(self) -> None:
        """getFilterEfficiencyByTask gets the filter efficiency for a given task"""
        ### Test when non-chain request
        response = self.nonChainWfController.getFilterEfficiencyByTask("")
        isFloat = isinstance(response, float)
        self.assertTrue(isFloat)

        isEqual = response == self.nonChainParams.get("filterEfficiency")
        self.assertTrue(isEqual)

        ### Test when step chain request
        ### TODO

        ### Test when task chain request
        ### TODO

    def testGetLumiWhiteList(self) -> None:
        """getLumiWhiteList gets the lumi white list"""
        ### Test when non-chain request
        lumiList = self.nonChainWfController.getLumiWhiteList()
        isList = isinstance(lumiList, list)
        self.assertTrue(isList)

        isEmpty = len(lumiList) == 0
        self.assertTrue(isEmpty)

        ### Test when step chain request
        ### TODO

        ### Test when task chain request
        ### TODO

    def testGetBlockWhiteList(self) -> None:
        """getBlockWhiteList gets the block white list"""
        ### Test when non-chain request
        blockList = self.nonChainWfController.getBlockWhiteList()
        isList = isinstance(blockList, list)
        self.assertTrue(isList)

        isEmpty = len(blockList) == 0
        self.assertTrue(isEmpty)

        ### Test when step chain request
        ### TODO

        ### Test when task chain request
        ### TODO

    def testGetRunWhiteList(self) -> None:
        """getRunWhiteList gets the run white list"""
        ### Test when non-chain request
        runList = self.nonChainWfController.getRunWhiteList()
        isList = isinstance(runList, list)
        self.assertTrue(isList)

        isEmpty = len(runList) == 0
        self.assertTrue(isEmpty)

        ### Test when step chain request
        ### TODO

        ### Test when task chain request
        ### TODO

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
        ### TODO

        ### Test when task chain request
        ### TODO

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
        ### TODO

        ### Test when task chain request
        ### TODO

    def testGetScramArches(self) -> None:
        """getScramArches gets the arches"""
        ### Test when non-chain request
        response = self.nonChainWfController.getComputingTime(unit="s")
        isFloat = isinstance(response, float)
        self.assertTrue(isFloat)

        isEqual = response == self.nonChainWfController.get("cpuSec")
        self.assertTrue(isEqual)

        ### Test when step chain request
        ### TODO

        ### Test when task chain request
        ### TODO

    def testGetBlocks(self) -> None:
        """getBlocks gets the blocks"""
        ### Test when non-chain request
        blocks = self.nonChainWfController.getBlocks()
        isList = isinstance(blocks, list)
        self.assertTrue(isList)

        isEmpty = len(blocks) == 0
        self.assertTrue(isEmpty)

        ### Test when step chain request
        ### TODO

        ### Test when task chain request
        ### TODO

    def testGetBlowupFactor(self) -> None:
        """getBlowupFactor gets the blocks"""
        ### Test when non-chain request
        response = self.nonChainWfController.getBlowupFactor()
        isFloat = isinstance(response, list)
        self.assertTrue(isFloat)

        isEqual = response == 1
        self.assertTrue(isEqual)

        ### Test when step chain request
        ### TODO

        ### Test when task chain request
        ### TODO

    def testGetNCopies(self) -> None:
        """getNCopies gets the number of needed copies"""
        ### Test when non-chain request
        response = self.nonChainWfController.getNCopies(self.nonChainParams.get("cpuSec") / 3600.0)
        isInt = isinstance(response, int)
        self.assertTrue(isInt)

        isEqual = response == self.nonChainParams.get("neededCopies")
        self.assertTrue(isEqual)

        ### Test when step chain request
        ### TODO

        ### Test when task chain request
        ### TODO


if __name__ == "__main__":
    unittest.main()
