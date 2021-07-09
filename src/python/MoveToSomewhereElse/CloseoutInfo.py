import os
import socket
from logging import Logger
from pymongo.collection import Collection

from typing import Optional

from Services.Mongo.MongoCollectionHandler import MongoCollectionHandler


class CloseoutInfo(MongoCollectionHandler):
    """
    __CloseoutInfo__
    General API for monitoring workflows closeout info
    """

    def __init__(self, logger: Optional[Logger]) -> None:
        super().__init__(logger=logger)
        self.owner = f"{socket.gethostname()}-{os.getpid()}"
        self.removedKeys = set()
        self.record = {}

    def _setMongoCollection(self) -> Collection:
        return self.client.unified.closeoutInfo

    def _buildMongoDocument(self, wf: str, data: dict) -> dict:
        document = super()._getOne(name=wf) or {}
        document.update(data)
        self.record[wf] = document
        return document

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

    def clean(self, wf: str) -> None:
        """
        The function to delete all the closeout info for a given workflow
        :param wf: workflow name
        """
        try:
            self.record.pop(wf, None)
            self.removedKeys.add(wf)
            super()._clean(name=wf)

        except Exception as error:
            self.logger.error("Failed to clean closeout info for workflow %s", wf)
            self.logger.error(str(error))

    # TODO: all the html settings
