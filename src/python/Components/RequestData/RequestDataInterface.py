from logging import Logger

from Components.RequestData.BaseRequestDataHandler import BaseRequestDataHandler
from Components.RequestData.NonChainRequestDataHandler import NonChainRequestDataHandler
from Components.RequestData.StepChainRequestDataHandler import StepChainRequestDataHandler
from Components.RequestData.TaskChainRequestDataHandler import TaskChainRequestDataHandler
from Services.ReqMgr.ReqMgrReader import ReqMgrReader

from typing import Optional


class RequestDataInterface(object):
    """
    __RequestDataInterface__
    General API for getting the RequestDataHandler for a given workflow
    """

    def __init__(
        self, wf: str, wfSchema: Optional[dict] = None, logger: Optional[Logger] = None
    ) -> BaseRequestDataHandler:
        try:
            super().__init__()
            reqmgrReader = ReqMgrReader()
            wfSchema = wfSchema or reqmgrReader.getWorkflowSchema(wf, makeCopy=True)

            if wfSchema.get("RequestType") == "TaskChain":
                return TaskChainRequestDataHandler(wfSchema, logger=logger)
            if wfSchema.get("RequestType") == "StepChain":
                return StepChainRequestDataHandler(wfSchema, logger=logger)
            return NonChainRequestDataHandler(wfSchema, logger=logger)

        except Exception as error:
            raise Exception(f"Error initializing RequestDataInterface\n{str(error)}")
