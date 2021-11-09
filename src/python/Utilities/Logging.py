import os
import traceback
import logging
from logging import Logger, LogRecord
from copy import deepcopy
from time import gmtime, mktime, asctime, struct_time
from urllib.parse import urlencode
from base64 import encodestring
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
from smtplib import SMTP

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

    def _setHeaders(self) -> None:
        """
        The function to set the header params
        """
        with open("config/secret_es.txt", "r") as file:
            entryPointName, password = file.readline().split(":")
        auth = encodestring(f"{entryPointName}:{password}".replace("\n", "")).replace("\n", "")
        self.headers = {"Authorization": f"Basic {auth}", "Content-Type": "application/json"}

    def _formatWorkflowMetadata(workflowController) -> str:
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

    def handleError(self, record: LogRecord) -> None:
        """
        The function to handle log errors
        :param record: log record
        """
        try:
            emailRecord = deepcopy(record)
            emailRecord.name += " - Failed logging"
            emailRecord.msg += f"\n{traceback.format_exc()}"
            EmailHandler().emit(emailRecord)

        except Exception as error:
            print("Failed to emit log error to email")
            print(str(error))

        return super().handleError(record)

    def close(self) -> None:
        """
        The function to properly close the handler
        """
        self.flush()
        super().close()


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

    def _getMessage(self, record: LogRecord) -> MIMEMultipart:
        """
        The function to get the log email message from a record
        :param record: log record
        """
        sender = record.sender if hasattr(record, "sender") else self.defaultsender
        emailDestination = (
            record.emailDestination if hasattr(record, "emailDestination") else self.defaultEmailDestination
        )

        msg = MIMEMultipart()
        msg["From"] = sender
        msg["To"] = COMMASPACE.join(emailDestination)
        msg["Date"] = formatdate(localtime=True)
        msg["Subject"] = f"[Ops] {record.name}"
        msg.attach(MIMEText(record.msg))
        return (msg, sender, emailDestination)

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
                msg, sender, emailDestination = self.logs.pop(0)
                smtpObj = SMTP()
                smtpObj.connect()
                smtpObj.sendmail(sender, emailDestination, msg.as_string())
                smtpObj.quit()

        except Exception as error:
            print("Failed to send log to Elastic Search")
            print(str(error))

    def close(self) -> None:
        """
        The function to properly close the handler
        """
        self.flush()
        super().close()


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
    logger = logging.getLogger(name)
    logger.setLevel(level)

    streamHandler = logging.StreamHandler()
    streamHandler.setLevel(level=level)
    streamHandler.setFormatter(logging.Formatter("[%(asctime)s:%(name)s:%(module)s] %(levelname)s: %(message)s"))
    logger.addHandler(streamHandler)

    if kwargs.get("addWfLevel"):
        logging.addLevelName(100, "WORKFLOW")
        logging.workflow = getWorkflowLogLevel
        logging.Logger.workflow = getWorkflowLogLevel

    if kwargs.get("elasticSearch"):
        esHandler = ElasticSearchHandler(level=kwargs.get("elasticSearch") or level, flushEveryLog=flushEveryLog)
        logger.addHandler(esHandler)

    if kwargs.get("email"):
        emailHandler = EmailHandler(level=kwargs.get("email") or level, flushEveryLog=flushEveryLog)
        logger.addHandler(emailHandler)

    return logger


def displayTime(seconds: int) -> str:
    """
    The function to display time for logging
    :param seconds: time in seconds
    :return: time in days, hours, minutes and seconds
    """
    try:
        if not seconds:
            return seconds

        minutes, seconds = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        days, hours = divmod(hours, 24)
        return f"{days} [d] {hours} [h] {minutes} [m] {seconds} [s]"

    except Exception as error:
        print(f"Failed to display time of {seconds} [s]\n{str(error)}")


def displayNumber(n: int) -> str:
    """
    The function to display a number for logging
    :param n: number
    :return: number in K, M or B
    """
    try:
        if not str(n).isdigit():
            return str(n)

        k, _ = divmod(n, 1000)
        m, k = divmod(k, 1000)
        b, m = divmod(m, 1000)

        return f"{b}B" if b else f"{m}M" if m else f"{k}K" if k else str(n)

    except Exception as error:
        print(f"Failed to display number {n}\n{str(error)}")
