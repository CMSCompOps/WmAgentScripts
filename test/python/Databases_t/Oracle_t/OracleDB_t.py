#!/usr/bin/env python
"""
_OracleDB_t_
Unit test for OracleDB and OracleClient helper class.
"""

import unittest
from sqlalchemy.orm import Session
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import DeclarativeMeta

from Databases.Oracle import OracleDB
from Databases.Oracle.OracleClient import OracleClient


class OracleClientTest(unittest.TestCase):
    oracleDBParams = {
        "schema": "cms_unified_admin",
        "tables": ["WORKFLOW", "OUTPUT", "TRANSFER", "TRANSFERIMP", "LOCKOFLOCK", "lock", "logrecord"],
        "workflow": {
            "columns": ["id", "name", "status", "wm_status"],
            "wm_status": "completed",
            "name": "pdmvserv_SMP-RunIISummer15wmLHEGS-00016_00051_v0__160525_042701_9941",
        },
        "output": {
            "columns": [
                "id",
                "datasetname",
                "nlumis",
                "expectedlumis",
                "nevents",
                "nblocks",
                "dsb_status",
                "status",
                "workfow_id",
                "date",
            ],
            "name": "/TTJets_TuneCUETP8M1_13TeV-amcatnloFXFX-scaledown-pythia8/RunIIWinter15GS-MCRUN2_71_V1-v1/GEN-SIM",
        },
        "transfer": {"columns": ["id", "phedexid", "workflows_id"]},
        "transferImp": {"columns": ["id", "phedexid", "workflow_id", "workflow", "active"]},
        "lockOfLock": {"columns": ["id", "lock", "time", "endtime", "owner"]},
        "lock": {"columns": ["id", "item", "lock", "is_block", "time", "reason"]},
        "logRecord": {
            "columns": ["id", "workflow", "logfile", "path", "task", "year", "month"],
            "name": "pdmvserv_EXO-RunIISummer15wmLHEGS-01962_00249_v0__161123_003115_4307",
        },
    }

    def setUp(self) -> None:
        self.oracleClient = OracleClient()
        super().setUp()
        return

    def tearDown(self) -> None:
        self.oracleClient.session.close()
        super().tearDown()
        return

    def testBase(self) -> None:
        """base defines mappings to OracleDB tables"""
        isDeclarativeMeta = isinstance(self.oracleClient.base, DeclarativeMeta)
        self.assertTrue(isDeclarativeMeta)

        schema = self.oracleClient.base.schema
        isEqual = schema == self.oracleDBParams.get("schema")
        self.assertTrue(isEqual)

        hasAllTables = False
        for table in self.oracleClient.base.metadata.tables.keys():
            if table.split(self.oracleClient.base.schema)[1].strip(".") not in self.oracleDBParams.get("tables"):
                break
        else:
            hasAllTables = True
        self.assertTrue(hasAllTables)

    def testEngine(self) -> None:
        """engine communicates to OracleDB"""
        isEngine = isinstance(self.oracleClient.engine, Engine)
        self.assertTrue(isEngine)

        isBonded = self.oracleClient.base.metadata.bind == self.oracleClient.engine
        self.assertTrue(isBonded)

    def testStartSession(self) -> None:
        """startSessions makes a session to OracleDB"""
        self.oracleClient.session = self.oracleClient.startSession()
        isSession = isinstance(self.oracleClient.session, Session)
        self.assertTrue(isSession)

    def testWorkflow(self) -> None:
        """Workflow is a model to the table in OracleDB"""
        workflow = OracleDB.Workflow
        isDeclarativeMeta = isinstance(workflow, DeclarativeMeta)
        self.assertTrue(isDeclarativeMeta)

        hasAllCols = all(
            col in self.oracleDBParams.get("workflow").get("columns") for col in workflow.__table__.columns.keys()
        )
        self.assertTrue(hasAllCols)

        response = (
            self.oracleClient.session.query(workflow)
            .filter(workflow.name == self.oracleDBParams.get("workflow").get("name"))
            .first()
        )
        isWorkflow = isinstance(response, workflow)
        self.assertTrue(isWorkflow)

        isFound = response.name == self.oracleDBParams.get("workflow").get("name")
        self.assertTrue(isFound)

    def testOutput(self) -> None:
        """Output is a model to the table in OracleDB"""
        output = OracleDB.Output
        isDeclarativeMeta = isinstance(output, DeclarativeMeta)
        self.assertTrue(isDeclarativeMeta)

        hasAllCols = all(
            col in self.oracleDBParams.get("output").get("columns") for col in output.__table__.columns.keys()
        )
        self.assertTrue(hasAllCols)

        response = (
            self.oracleClient.session.query(output)
            .filter(output.datasetname == self.oracleDBParams.get("output").get("name"))
            .first()
        )
        isOutput = isinstance(response, output)
        self.assertTrue(isOutput)

        isFound = response.datasetname == self.oracleDBParams.get("output").get("name")
        self.assertTrue(isFound)

    def testTransfer(self) -> None:
        """Transfer is a model to the table in OracleDB"""
        transfer = OracleDB.Transfer
        isDeclarativeMeta = isinstance(transfer, DeclarativeMeta)
        self.assertTrue(isDeclarativeMeta)

        hasAllCols = all(
            col in self.oracleDBParams.get("transfer").get("columns") for col in transfer.__table__.columns.keys()
        )
        self.assertTrue(hasAllCols)

        response = self.oracleClient.session.query(transfer).first()
        isTransfer = isinstance(response, transfer)
        self.assertTrue(isTransfer)

    def testTransferImp(self) -> None:
        """TransferImp is a model to the table in OracleDB"""
        transferImp = OracleDB.TransferImp
        isDeclarativeMeta = isinstance(transferImp, DeclarativeMeta)
        self.assertTrue(isDeclarativeMeta)

        hasAllCols = all(
            col in self.oracleDBParams.get("transferImp").get("columns")
            for col in transferImp.__table__.columns.keys()
        )
        self.assertTrue(hasAllCols)

        response = self.oracleClient.session.query(transferImp).first()
        isTransferImp = isinstance(response, transferImp)
        self.assertTrue(isTransferImp)

    def testLockOfLock(self) -> None:
        """LockOfLock is a model to the table in OracleDB"""
        lockOfLock = OracleDB.LockOfLock
        isDeclarativeMeta = isinstance(lockOfLock, DeclarativeMeta)
        self.assertTrue(isDeclarativeMeta)

        hasAllCols = all(
            col in self.oracleDBParams.get("lockOfLock").get("columns") for col in lockOfLock.__table__.columns.keys()
        )
        self.assertTrue(hasAllCols)

        response = self.oracleClient.session.query(lockOfLock).first()
        isLockOfLock = isinstance(response, lockOfLock)
        self.assertTrue(isLockOfLock)

    def testLock(self) -> None:
        """Lock is a model to the table in OracleDB"""
        lock = OracleDB.Lock
        isDeclarativeMeta = isinstance(lock, DeclarativeMeta)
        self.assertTrue(isDeclarativeMeta)

        hasAllCols = all(
            col in self.oracleDBParams.get("lock").get("columns") for col in lock.__table__.columns.keys()
        )
        self.assertTrue(hasAllCols)

        response = self.oracleClient.session.query(lock).first()
        isLock = isinstance(response, lock)
        self.assertTrue(isLock)

    def testLogRecord(self) -> None:
        """LogRecord is a model to the table in OracleDB"""
        logRecord = OracleDB.LogRecord
        isDeclarativeMeta = isinstance(logRecord, DeclarativeMeta)
        self.assertTrue(isDeclarativeMeta)

        hasAllCols = all(
            col in self.oracleDBParams.get("logRecord").get("columns") for col in logRecord.__table__.columns.keys()
        )
        self.assertTrue(hasAllCols)

        response = (
            self.oracleClient.session.query(logRecord)
            .filter(logRecord.workflow == self.oracleDBParams.get("logRecord").get("name"))
            .first()
        )
        isLogRecord = isinstance(response, logRecord)
        self.assertTrue(isLogRecord)

        isFound = response.workflow == self.oracleDBParams.get("logRecord").get("name")
        self.assertTrue(isFound)


if __name__ == "__main__":
    unittest.main()
