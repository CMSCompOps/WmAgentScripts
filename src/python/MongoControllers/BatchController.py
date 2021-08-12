from collections import defaultdict
from pymongo.collection import Collection

from Databases.Mongo.MongoClient import MongoClient


class BatchController(MongoClient):
    """
    __BatchController__
    General API for controlling the batches info
    """

    def _setMongoCollection(self) -> Collection:
        return self.client.unified.batchInfo

    def _buildMongoDocument(self, name: str, prepIds: list) -> dict:
        document = super()._getOne(name=name) or defaultdict(list)
        document["ids"] = list(set(list(document["ids"]) + list(prepIds)))
        return dict(document)

    def set(self, name: str, prepIds: list) -> None:
        """
        The function to set new data in batch info
        :param name: batch name
        :param prepIds: prep ids
        """
        try:
            super()._set(name, prepIds, name=name)

        except Exception as error:
            self.logger.error("Failed to set batch info for %s", name)
            self.logger.error(str(error))

    def get(self) -> dict:
        """
        The function to get all the batch info
        :return: batch info
        """
        try:
            content = super()._get("name", details=True)
            return dict((name, doc["ids"]) for name, doc in content.items())

        except Exception as error:
            self.logger.error("Failed to get the batch info")
            self.logger.error(str(error))

    def getBatches(self) -> list:
        """
        The function to get all the batches names
        :return: batches names
        """
        try:
            return super()._get("name")

        except Exception as error:
            self.logger.error("Failed to get the batches names")
            self.logger.error(str(error))

    def pop(self, name: str) -> None:
        """
        The function to delete the info for a given batch name
        :param name: batch name
        """
        try:
            super()._pop(name=name)

        except Exception as error:
            self.logger.error("Failed to pop info for batch %s", name)
            self.logger.error(str(error))
