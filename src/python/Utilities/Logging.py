import os
import logging
import logging.handlers
from logging import Logger, LogRecord
import smtplib
from time import gmtime, mktime, asctime, struct_time
from typing import Optional
from urllib.parse import urlencode
from base64 import encodestring
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
from smtplib import SMTP

from Components.Workflow.WorkflowController import WorkflowController
from Utilities.WebTools import sendResponse
from Utilities.ConfigurationHandler import ConfigurationHandler


class ElasticSearchHandler(logging.Handler):
    """
    _ElasticSearchHandler_
    Custom class for handling log for elastic search
    """

    def __init__(self, level: str = "INFO", flushEveryLog: bool = True) -> None:
        try:
            super().__init__(level=level)

            configurationHandler = ConfigurationHandler()
            self.logUrl = configurationHandler.get("logging_url")
            self.logEndpoint = "/es/unified-logs/_doc/"

            self.flushEveryLog = flushEveryLog
            self.logs = []

            self._setHeaders()

        except Exception as error:
            raise Exception(f"Error initializing ElasticSearchHandler\n{str(error)}")

    def __del__(self) -> None:
        self.flush()

    def _setHeaders(self) -> None:
        """
        The function to set the header params
        """
        with open("config/secret_es.txt", "r") as file:
            entryPointName, password = file.readline().split(":")
        auth = encodestring(f"{entryPointName}:{password}".replace("\n", "")).replace("\n", "")
        self.headers = {"Authorization": f"Basic {auth}", "Content-Type": "application/json"}

    def _formatWorkflowMetadata(workflowController: WorkflowController) -> str:
        """
        The function to format the workflow metadata for logging
        :param workflowController: instance of WorkflowController
        :return: workflow metadata
        """
        prepIds = workflowController.getPrepIDs()
        _, primaries, _, secondaries = workflowController.request.getIO()

        outputs = workflowController.request.get("OutputDatasets")
        for key in ["FAKE", "None"]:
            outputs = [*(filter(lambda x: key not in x, outputs))]

        meta = ""
        for key, value in [("id", prepIds), ("in", primaries), ("pu", secondaries), ("out", outputs)]:
            if value:
                value = "\n".join([*map(lambda x: f"{key}: {x}", value)])
                meta += value

        meta += f"\n\n{workflowController.request.get('RequestName')}"
        return meta

    def _getDocument(self, record: LogRecord, now: struct_time = gmtime()) -> dict:
        """
        The function to get the log document from a record
        :param record: log record
        :param now: now time
        """
        meta = ""
        if hasattr(record, "workflow"):
            meta = self._formatWorkflowMetadata(record.workflow)

        return {
            "author": os.getenv("USER"),
            "subject": record.name,
            "text": record.msg,
            "meta": f"level:{record.levelname}\n{meta}",
            "timestamp": int(mktime(now)),
            "date": asctime(now),
        }

    def emit(self, record: LogRecord) -> None:
        """
        The function to send the log document
        :param record: log record
        """
        try:
            self.logs.append(self._getDocument(record))
            if self.flushEveryLog:
                self.flush()

        except Exception as error:
            print("Failed to emit log")
            print(str(error))
            self.handleError(record)

    def flush(self) -> None:
        """
        The function to flush the logs
        """
        try:
            while self.logs:
                doc = self.logs.pop(0)
                _ = urlencode(doc)
                return sendResponse(url=self.logUrl, endpoint=self.logEndpoint, param=doc, headers=self.headers)

        except Exception as error:
            print("Failed to send log to Elastic Search")
            print(str(error))


class EmailHandler(logging.Handler):
    """
    _EmailHandler_
    Custom class for handling log for email
    """

    def __init__(self, level: str = "INFO", flushEveryLog: bool = True) -> None:
        try:
            super().__init__(level=level)

            unifiedConfiguration = ConfigurationHandler("config/unifiedConfiguration.json")

            self.defaultEmailDestination = unifiedConfiguration.get("email_destination")
            self.defaultsender = (
                unifiedConfiguration.get("email_sender").get(os.getenv("USER")) or "cmsunified@cern.ch"
            )

            self.flushEveryLog = flushEveryLog
            self.logs = []

        except Exception as error:
            raise Exception(f"Error initializing EmailHandler\n{str(error)}")

    def __del__(self) -> None:
        self.flush()

    def _getMessage(self, record: LogRecord) -> MIMEMultipart:
        """
        The function to get the log email message from a record
        :param record: log record
        """
        msg = MIMEMultipart()
        msg["From"] = record.sender if hasattr(record, "sender") else self.defaultsender
        msg["To"] = COMMASPACE.join(
            record.emailDestination if hasattr(record, "emailDestination") else self.defaultEmailDestination
        )
        msg["Date"] = formatdate(localtime=True)
        msg["Subject"] = f"[Ops] {record.name}"
        msg.attach(MIMEText(record.msg))
        return msg

    def emit(self, record: LogRecord) -> None:
        """
        The function to send the log email
        :param record: log record
        """
        try:
            self.logs.append(self._getMessage(record))
            if self.flushEveryLog:
                self.flush()

        except Exception as error:
            print("Failed to send log to emails")
            print(str(error))
            self.handleError(record)

    def flush(self) -> None:
        """
        The function to flush the logs
        """
        try:
            while self.logs:
                msg = self.logs.pop(0)
                smtpObj = SMTP()
                smtpObj.connect()
                smtpObj.sendmail(self.sender, self.emailDestination, msg.as_string())
                smtpObj.quit()

        except Exception as error:
            print("Failed to send log to Elastic Search")
            print(str(error))


def getWorkflowLogLevel(logger: Logger, msg: str, *args, **kwargs) -> None:
    """
    The function to add a custom workflow level to the logger configuration
    :param logger: logger
    :param msg: message
    """
    if logger.isEnabledFor(100):
        logger._log(100, msg, args, **kwargs)


def getLogger(name: str, level: str = "INFO", flushEveryLog: bool = True, **kwargs) -> Logger:
    """
    The function to get the logger
    :param name: logger name
    :param level: logger level
    :param flushEveryLog: if to flush every log
    :return: a logger
    """
    logging.basicConfig(level=level, format="%(asctime)s:%(levelname)s:%(module)s:%(message)s")
    logger = logging.getLogger(name)

    if kwargs.get("addWfLevel"):
        logging.addLevelName(100, "WORKFLOW")
        logging.workflow = getWorkflowLogLevel
        logging.Logger.workflow = getWorkflowLogLevel

    if kwargs.get("elasticSearch"):
        esHandler = ElasticSearchHandler(flushEveryLog=flushEveryLog)
        logger.addHandler(esHandler)

    if kwargs.get("email"):
        emailHandler = EmailHandler(flushEveryLog=flushEveryLog)
        logger.addHandler(emailHandler)

    return logger
