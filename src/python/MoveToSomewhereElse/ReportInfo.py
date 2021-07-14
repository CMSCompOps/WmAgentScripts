from time import struct_time, gmtime, mktime, asctime
from pymongo.collection import Collection

from typing import Any

from Utils.IteratorTools import mapKeys, mapValues
from Services.Mongo.MongoClient import MongoClient


class ReportInfo(MongoClient):
    """
    __ReportInfo__
    General API for reporting worflows info
    """

    def _setMongoCollection(self) -> Collection:
        return self.client.unified.reportInfo

    def _convertValues(self, value: Any) -> Any:
        return (
            list(value) if isinstance(value, set) else self._convertValues(value) if isinstance(value, dict) else value
        )

    def _buildMongoDocument(self, data: dict, now: struct_time = gmtime()) -> dict:
        data.update({"time": mktime(now), "date": asctime(now)})
        data = mapKeys(lambda x: x.replace(".", "__dot__"), data)
        data = mapValues(self._convertValues, data)

        document = self.get(data.get("workflow")) or {}
        if document:
            document.setdefault("tasks", {})
            tasks = data.pop("tasks", {})
            for task in sorted(set(document["tasks"].keys() + tasks.keys())):
                document["tasks"].update({task: tasks.get(task, {})})

        document.update(data)
        return document

    def set(self, data: dict) -> None:
        """
        The function to set new data in the report info
        :param data: report data
        """
        try:
            super()._set(data, workflow=data.get("workflow"))

        except Exception as error:
            self.logger.error("Failed to set report info")
            self.logger.error(str(error))

    def setWorkflowIO(self, wf: str, IO: dict) -> None:
        """
        The function to set IO data in the report info of a given workflow
        :param wf: workflow name
        :param IO: IO data
        """
        self.set({**{"workflow": wf}, **IO})

    def setWorkflowTask(self, wf: str, task: str, data: dict) -> None:
        """
        The function to set task data in the report info of a given workflow
        :param wf: workflow name
        :param taks: task name
        :param data: task data
        """
        self.set({"workflow": wf, "tasks": {task.split("/")[-1]: data}})

    def setWorkflowErrors(self, wf: str, task: str, errors: dict) -> None:
        """
        The function to set errors data in the report info of a given workflow
        :param wf: workflow name
        :param task: task name
        :param errors: errors data
        """
        self.setWorkflowTask(wf, task, {"errors": errors})

    def setWorkflowBlocks(self, wf: str, task: str, blocks: dict) -> None:
        """
        The function to set blocks data in the report info of a given workflow
        :param wf: workflow name
        :param task: task name
        :param blocks: blocks data
        """
        self.setWorkflowTask(wf, task, {"needed_blocks": blocks})

    def setWorkflowFiles(self, wf: str, task: str, files: dict) -> None:
        """
        The function to set files data in the report info of a given workflow
        :param wf: workflow name
        :param task: task name
        :param files: files data
        """
        self.setWorkflowTask(wf, task, {"files": files})

    def setWorkflowUFiles(self, wf: str, task: str, ufiles: dict) -> None:
        """
        The function to set ufiles data in the report info of a given workflow
        :param wf: workflow name
        :param task: task name
        :param uFiles: ufiles data
        """
        self.setWorkflowTask(wf, task, {"ufiles": ufiles})

    def setWorkflowMissing(self, wf: str, task: str, missing: dict) -> None:
        """
        The function to set missing data in the report info of a given workflow
        :param wf: workflow name
        :param task: task name
        :param missing: missing data
        """
        self.setWorkflowTask(wf, task, {"missing": missing})

    def setWorkflowLogs(self, wf: str, task: str, logs: dict) -> None:
        """
        The function to set logging data in the report info of a given workflow
        :param wf: workflow name
        :param task: task name
        :param logs: logging data
        """
        self.setWorkflowTask(wf, task, {"logs": logs})

    def get(self, wf: str) -> dict:
        """
        The function to get the report info for a given workflow
        :param wf: workflow name
        :return: report info
        """
        try:
            return super()._getOne(dropParams=["_id"], workflow=wf)

        except Exception as error:
            self.logger.error("Failed to get the report info for workflow %s", wf)
            self.logger.error(str(error))

    def clean(self) -> None:
        """
        The function to delete all report info
        """
        try:
            super()._clean()

        except Exception as error:
            self.logger.error("Failed to clean all report info")
            self.logger.error(str(error))

    def cleanWorkflow(self, wf: str) -> None:
        """
        The function to delete all report info for a given workflow
        :param wf: workflow name
        """
        try:
            super()._clean(workflow=wf)

        except Exception as error:
            self.logger.error("Failed to clean the report info for workflow %s", wf)
            self.logger.error(str(error))

    def purge(self, expiredDays: int = 30) -> None:
        """
        The function to delete all report info if it is expired
        :param expiredDays: passed days from expiration time so that data can be deleted
        """
        try:
            super()._purge("time", expiredDays)

        except Exception as error:
            self.logger.error("Failed to purge report info expired for more than %s days", expiredDays)
            self.logger.error(str(error))
