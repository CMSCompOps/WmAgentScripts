from time import struct_time, gmtime, mktime, asctime
from pymongo.collection import Collection

from Databases.Mongo.MongoClient import MongoClient
from Utilities.IteratorTools import mapKeys


class StatusHistoryController(MongoClient):
    """
    __StatusHistoryController__
    General API for monitoring the workflows status history
    """

    def _setMongoCollection(self) -> Collection:
        return self.client.unified.statusHistory

    def _buildMongoDocument(self, data: dict, now: struct_time = gmtime()) -> dict:
        data.update({"time": int(mktime(now)), "date": asctime(now)})
        return data

    def set(self, data: dict) -> None:
        """
        The function to set new data in the status history
        :param data: data
        """
        try:
            super()._set(data)

        except Exception as error:
            self.logger.error("Failed to set status history")
            self.logger.error(str(error))

    def get(self) -> dict:
        """
        The function to get all the status history
        :return: status history
        """
        try:
            return mapKeys(int, super()._get("time", details=True))

        except Exception as error:
            self.logger.error("Failed to get the status history")
            self.logger.error(str(error))

    def purge(self, expiredDays: int = 7) -> None:
        """
        The function to delete all data from status history if it is expired
        :param expiredDays: passed days from expiration time so that data can be deleted
        """
        try:
            super()._purge("time", expiredDays)

        except Exception as error:
            self.logger.error("Failed to purge status history expired for more than %s days", expiredDays)
            self.logger.error(str(error))
