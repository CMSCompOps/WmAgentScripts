#!/usr/bin/env python
"""
_WorkQueueReader_t_
Unit test for WorkQueueReader helper class.
"""

import unittest
from Services.WorkQueue.WorkQueueReader import WorkQueueReader


class WorkQueueReaderTest(unittest.TestCase):
    # For now only test if the request is working
    params = {"workflow": "pdmvserv_SMP-RunIISummer15wmLHEGS-00016_00051_v0__160525_042701_9941"}

    def setUp(self) -> None:
        self.workqueueReader = WorkQueueReader()
        super().setUp()
        return

    def tearDown(self) -> None:
        super().tearDown()
        return

    def testGetWorkQueue(self) -> None:
        """getWorkQueue gets the workqueue for a given workflow"""
        queue = self.workqueueReader.getWorkQueue(self.params.get("workflow"))
        isList = isinstance(queue, list)
        self.assertTrue(isList)


if __name__ == "__main__":
    unittest.main()
