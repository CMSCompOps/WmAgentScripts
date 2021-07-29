from logging import Logger

from Components.Workload.BaseWorkloadHandler import BaseWorkloadHandler
from Components.Workload.NonChainWorkloadHandler import NonChainWorkloadHandler
from Components.Workload.StepChainWorkloadHandler import StepChainWorkloadHandler
from Components.Workload.TaskChainWorkloadHandler import TaskChainWorkloadHandler

from Services.ReqMgr.ReqMgrReader import ReqMgrReader

from typing import Optional


class WorkloadInterface(object):
    """
    __WorkloadInterface__
    General API for getting the workload handler for a given workflow
    """

    def __init__(
        self, wf: str, wfSchema: Optional[dict] = None, logger: Optional[Logger] = None
    ) -> BaseWorkloadHandler:
        try:
            super().__init__()
            reqmgrReader = ReqMgrReader()
            wfSchema = wfSchema or reqmgrReader.getWorkflowSchema(wf, makeCopy=True)

            if wfSchema.get("RequestType") == "TaskChain":
                return TaskChainWorkloadHandler(wfSchema, logger=logger)
            if wfSchema.get("RequestType") == "StepChain":
                return StepChainWorkloadHandler(wfSchema, logger=logger)
            return NonChainWorkloadHandler(wfSchema, logger=logger)

        except Exception as error:
            raise Exception(f"Error initializing WorkloadInterface\n{str(error)}")
