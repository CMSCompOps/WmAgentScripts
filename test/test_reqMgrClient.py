#!/usr/bin/python
"""
    Unit tests for WmAgentScripts/reqMgrClient.py
"""
import unittest
import sys
sys.path.append("../")
import reqMgrClient as reqMgr

VALID_STATUS =  ["new","assignment-approved","assigned", "acquired", "running-open", "running-closed","rejected","aborted","completed",
                 "rejected-archived"]
VALID_TYPES =  ["MonteCarlo", "ReDigi", "TaskChain", "MonteCarloFromGEN","StepChain"]

url = 'cmsweb-testbed.cern.ch'
#testwf = "jbadillo_RunIIWinter15GS-001-T2_CH_CERN_T0Backfill_150527_153801_6417"
testwf = "jbadillo_Summer11-T2_IN_TIFRBackfill_151022_113935_4776"
#testwf = "amaltaro_MonteCarloFromGEN_PNN_Oracle_19Oct2015_v1_151019_112049_1503"
    
# TODO load a wf with input

class TestReqMgrClient(unittest.TestCase):

    def setUp(self):
        pass

    def testWorkflowObj(self):
        # Creation of Workflow objects
        wfobj = reqMgr.Workflow(testwf, url=url)
        self.assertEqual(testwf, wfobj.name)
    
    def testWorkflowInfo(self):
        # getting WorkflowInfo
        info = reqMgr.getWorkflowInfo(url, testwf)
        self.assert_(info)
    
    def testGetWorkflowStatus(self):
        # Workflow status    
        st = reqMgr.getWorkflowStatus(url, testwf)
        if st is None:
            self.fail("None status")
        if st not in VALID_STATUS:
            self.fail("%s not a valid state"%st)

    def testGetFilterEfficiency(self):
        # getFilterEfficiency
        fe = reqMgr.getFilterEfficiency(url, testwf)
        if fe is None:
            self.fail("None filter efficiency")
        fe = float(fe)

    def testGetWorkflowType(self):
        # getWorkflowType
        ty = reqMgr.getWorkflowType(url, testwf)
        if ty is None:
            self.fail("None type")
        if ty not in VALID_TYPES:
            self.fail("%s not a valid type"%ty)

    def testGetWorkflowPriority(self):
        # getWorkflowPriority
        pr = reqMgr.getWorkflowPriority(url, testwf)
        if pr is None:
            self.fail("None priority")
        pr = float(pr)

    def testGetRequestTeam(self):
        # getRequestTeam
        te = reqMgr.getRequestTeam(url, testwf)
        if te is None:
            self.fail("None team")

    def testGetInputEvents(self):
        # getInputEvents
        ev = reqMgr.getInputEvents(url, testwf)
        if ev is None:
            self.fail("None input events")
        ev = float(ev)

    def testGetInputLumis(self):
        # getInputLumis
        il = reqMgr.getInputLumis(url, testwf)
        if il is None:
            self.fail("None lumis")
        il = float(il)



# Creation of request
# requestManagerPut

    def testForceCompleteWorkflow(self):
        # forceCompleteWorkflow
        te = reqMgr.forceCompleteWorkflow(url, testwf)
        if te is None:
            self.fail("None output")
            
    def testCcloseOutWorkflow(self):
        # closeOutWorkflow
        te = reqMgr.closeOutWorkflow(url, testwf)
        if te is None:
            self.fail("None output")
    def testAnnounceWorkflow(self):
        # announceWorkflow
        te = reqMgr.announceWorkflow(url, testwf)
        if te is None:
            self.fail("None output")
    def testSetWorkflowApproved(self):
        # setWorkflowApproved
        te = reqMgr.setWorkflowApproved(url, testwf)
        if te is None:
            self.fail("None output")
    """
    def testInvalidateWorkflow(self):
        # invalidateWorkflow
        te = reqMgr.invalidateWorkflow(url, testwf)
        if te is None:
            self.fail("None output")
    """
    def testRejectWorkflow(self):
        # rejectWorkflow
        te = reqMgr.rejectWorkflow(url, testwf)
        if te is None:
            self.fail("None output")
    def testAbortWorkflow(self):
        # abortWorkflow
        te = reqMgr.abortWorkflow(url, testwf)
        if te is None:
            self.fail("None output")
    def testCloneWorkflow(self):
        # cloneWorkflow
        te = reqMgr.cloneWorkflow(url, testwf)
        if te is None:
            self.fail("None output")

if __name__ == '__main__':
    #wfobj = reqMgr.getWorkflowInfo("cmsweb.cern.ch", "pdmvserv_EXO-RunIISpring15MiniAODv2-02433_00142_v0__151013_190450_100")
    #wfobj = reqMgr.getWorkflowInfo(url, testwf)
    unittest.main()



