import logging
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
    ) -> None:
        try:
            super().__init__()
            reqmgrReader = ReqMgrReader()
            self.wfSchema = wfSchema or reqmgrReader.getWorkflowSchema(wf, makeCopy=True)

            logging.basicConfig(level=logging.INFO)
            self.logger = logger or logging.getLogger(self.__class__.__name__)

        except Exception as error:
            raise Exception(f"Error initializing WorkloadInterface\n{str(error)}")
    
    def __call__(self) -> BaseWorkloadHandler:
        try:
            if self.wfSchema.get("RequestType") == "TaskChain":
                return TaskChainWorkloadHandler(self.wfSchema, logger=self.logger)
            if self.wfSchema.get("RequestType") == "StepChain":
                return StepChainWorkloadHandler(self.wfSchema, logger=self.logger)
            return NonChainWorkloadHandler(self.wfSchema, logger=self.logger)
        
        except Exception as error:
            self.logger.error("Failed to get workload handler")
            self.logger.error(str(error))
        

