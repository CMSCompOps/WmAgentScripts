import os
import socket
from logging import Logger
from collections import defaultdict
from time import gmtime, localtime, asctime
from pymongo.collection import Collection
from jinja2 import Template

from typing import List, Optional

from Utilities.Logging import displayNumber
from Utilities.ConfigurationHandler import ConfigurationHandler
from Services.EOS.EOSWriter import EOSWriter
from Services.Mongo.MongoClient import MongoClient


class CloseoutInfo(MongoClient):
    """
    __CloseoutInfo__
    General API for monitoring workflows closeout info
    """

    def __init__(self, logger: Optional[Logger] = None) -> None:
        try:
            super().__init__(logger=logger)
            configurationHandler = ConfigurationHandler()
            self.reqmgrUrl = os.getenv("REQMGR_URL", configurationHandler.get("reqmgr_url"))
            self.unifiedUrl = os.getenv("UNIFIED_URL", configurationHandler.get("unified_url"))
            self.monitorEOSDirectory = configurationHandler.get("monitor_eos_dir")
            self.templateDirectory = configurationHandler.get("template_dir")

            self.template = {
                "summary": self.templateDirectory + "/CloseoutInfo/Summary.jinja",
                "assistance": self.templateDirectory + "/CloseoutInfo/Assistance.jinja",
            }

            self.removed = set()
            self.record = {}

        except Exception as e:
            msg = "Error initializing CloseoutInfo\n"
            msg += str(e)
            raise Exception(msg)

    def _setMongoCollection(self) -> Collection:
        return self.client.unified.closeoutInfo

    def _buildMongoDocument(self, wf: str, data: dict) -> dict:
        document = super()._getOne(name=wf) or {}
        document.update(data)
        self.record[wf] = document
        return document

    def _buildHtmlRow(self, wf: str) -> Optional[dict]:
        """
        The function to build the data for a row of the html table
        :param wf: workflow name
        :return: workflow data if any, None o/w
        """
        record = self.get(wf)
        if not record:
            return None

        wfData = {}
        wfData["name"] = wf
        wfData["priority"] = record["priority"]
        wfData["taskPrepId"] = record["prepId"]
        wfData["prepId"] = wfData["taskPrepId"].replace("task_", "")
        wfData["nDatasets"] = len(record["datasets"])
        wfData["datasets"] = []

        datasetsProgress = sorted(
            [(dataset, value.get("percentage")) for dataset, value in record["datasets"].items()],
            key=lambda x: x[1],
        )
        for dataset, _ in datasetsProgress:
            datasetData = {}
            datasetData["name"] = dataset
            datasetData["updated"] = record["datasets"][dataset]["updated"]
            datasetData["percentage"] = {
                "value": record["datasets"].get("percentage", "-NA-"),
                "fractionPass": record["datasets"][dataset].get("fractionpass", 0),
            }
            datasetData["events"] = {
                "produced": displayNumber(record["datasets"][dataset].get("producedN", "NA")),
                "expected": displayNumber(record["datasets"][dataset].get("expectedN", "NA")),
            }
            datasetData["lumis"] = {
                "produced": displayNumber(record["datasets"][dataset].get("producedL", "NA")),
                "expected": displayNumber(record["datasets"][dataset].get("expectedL", "NA")),
            }
            for col in ["acdc", "dbsFiles", "dbsInvFiles", "phedexFiles"]:
                datasetData[col] = {"value": record["datasets"][dataset].get(col, "-NA-")}
            wfData["datasets"].append(datasetData)

        return wfData

    def _renderSummaryHtml(self, sessionWorkflows: List[dict]) -> str:
        """
        The function to render the summary html from jinja template
        :param sessionWorkflows: a list of workflows in the session
        """
        workflows = []
        for i, wf in enumerate(sorted(self.getWorkflows())):
            sessionWf = [sessionWf for sessionWf in sessionWorkflows if sessionWf["name"] == wf]
            if not sessionWf:
                continue
            if sessionWf["status"] != "away" and not sessionWf["status"].startswaith("assistance"):
                self.logger.info("Taking %s out of the closeout info", wf)
                self.clean(wf)
                continue

            wfData = self._buildHtmlRow(wf)
            wfData["status"] = sessionWf["status"]
            wfData["bgColor"] = "lightblue" if i % 2 else "white"
            workflows.append(wfData)

        with open(self.template["summary"]) as tmpl:
            template = Template(tmpl.read())

        return template.render(
            updateCETime=asctime(localtime()),
            updateGMTime=asctime(gmtime()),
            wfs=workflows,
            reqmgrUrl=self.reqmgrUrl,
            unifiedUrl=self.unifiedUrl,
            cols=["percentage", "acdc", "events", "lumis", "dbsFiles", "dbsInvFiles", "phedexFiles"],
        )

    def _renderAssistanceHtml(self, sessionWorkflows: List[dict], details: bool = False) -> str:
        """
        The function to render the assistance html from jinja template
        :param sessionWorkflows: a list of workflows in the session
        :param details: if True return html with detailed table, summary table o/w
        """
        wfsByStatus = defaultdict(list)
        for wf in sessionWorkflows:
            if wf["status"].startswith("assistance"):
                wfsByStatus[wf["status"]].append(wf)

        assistanceStatus = []
        for status, wfs in dict(sorted(wfsByStatus.items())).items():
            statusData = {}
            statusData["name"] = status
            statusData["wfs"] = []

            for i, wf in enumerate(
                sorted(wfs, key=lambda x: self.record[x["name"]]["priority"] if x["name"] else None, reverse=True)
            ):
                wfData = self._buildHtmlRow(wf["name"])
                if not wfData:
                    continue

                wfData["status"] = status
                wfData["bgColor"] = "lightblue" if i % 2 else "white"
                statusData["wfs"].append(wfData)

            assistanceStatus.append(statusData)

        with open(self.template["assistance"]) as tmpl:
            template = Template(tmpl.read())

        return template.render(
            updateGMTime=asctime(gmtime()),
            assistanceStatus=assistanceStatus,
            reqmgrUrl=self.reqmgrUrl,
            unifiedUrl=self.unifiedUrl,
            cols=["percentage", "acdc", "events", "lumis", "dbsFiles", "dbsInvFiles", "phedexFiles"],
            details=details,
        )

    def set(self, wf: str, data: dict) -> None:
        """
        The function to set new data in the closeout info for a given workflow
        :param wf: workflow name
        :param data: workflow data
        """
        try:
            super()._set(wf, data, name=wf)

        except Exception as error:
            self.logger.error("Failed to set closeout info for workflow %s", wf)
            self.logger.error(str(error))

    def get(self, wf: str) -> Optional[dict]:
        """
        The function to get the closeout info for a given workflow
        :param wf: workflow name
        :return: collection content if any, None o/w
        """
        try:
            if wf not in self.record:
                content = super()._getOne(dropParams=["name", "_id"], name=wf)
                if not content:
                    return None
                self.record[wf] = content
            return self.record[wf]

        except Exception as error:
            self.logger.error("Failed to get closeout info for workflow %s", wf)
            self.logger.error(str(error))

    def getWorkflows(self) -> List[str]:
        """
        The function to get the workflows in closeout info
        :param wf: workflow name
        :return: workflows
        """
        try:
            return super()._get("name")

        except Exception as error:
            self.logger.error("Failed to get workflows name in closeout info")
            self.logger.error(str(error))

    def clean(self, wf: str) -> None:
        """
        The function to delete all the closeout info for a given workflow
        :param wf: workflow name
        """
        try:
            self.record.pop(wf, None)
            self.removed.add(wf)
            super()._clean(name=wf)

        except Exception as error:
            self.logger.error("Failed to clean closeout info for workflow %s", wf)
            self.logger.error(str(error))

    def summary(self, sessionWorkflows: List[dict]) -> None:
        """
        The function to write the closeout info summary in a html file and save it in EOS
        :param sessionWorkflows: a list of workflows in the session
        """
        try:
            summaryFile = EOSWriter(f"{self.monitorEOSDirectory}/closeout.html", self.logger)
            summaryFile.write(self._renderSummary(sessionWorkflows))
            summaryFile.save()

        except Exception as error:
            self.logger.error("Failed to save summary closeout info")
            self.logger.error(str(error))

    def assistance(self, sessionWorkflows: List[dict]) -> None:
        """
        The function to write the closeout info assistance in a html file and save it in EOS
        :param sessionWorkflows: a list of workflows in the session
        """
        try:
            for filename, details in [("assistance.html", True), ("assistance_summary.html", False)]:
                assistanceFile = EOSWriter(f"{self.monitorEOSDirectory}/{filename}", self.logger)
                assistanceFile.write(self._renderSummaryHtml(sessionWorkflows, details=details))
                assistanceFile.save()

        except Exception as error:
            self.logger.error("Failed to save assistance closeout info")
            self.logger.error(str(error))

    def html(self, sessionWorkflows: List[dict]) -> None:
        """
        The function to write the html closeout info and save it in EOS
        :param sessionWorkflows: a list of workflows in the session
        """
        self.summary(sessionWorkflows)
        self.assistance(sessionWorkflows)