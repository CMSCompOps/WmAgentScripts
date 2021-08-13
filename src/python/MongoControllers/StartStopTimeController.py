from time import localtime
from pymongo.collection import Collection

from Databases.Mongo.MongoClient import MongoClient

from typing import Optional


class StartStopTimeController(MongoClient):
    """
    __StartStopTimeController__
    General API for monitoring modules runtimes
    """

    def _setMongoCollection(self) -> Collection:
        return self.client.unified.startStopTime

    def _buildMongoDocument(self, component: str, start: int, stop: Optional[int] = None) -> dict:
        document = {"component": component, "start": int(start)}
        if stop:
            document.update({"stop": int(stop), "lap": int(stop) - int(start)})
        return document

    def set(self, component: str, start: int, stop: Optional[int] = None) -> None:
        """
        The function to set start/stop times for a given component
        :param component: component name
        :param start: start time
        :param stop: stop time, if any
        """
        try:
            super()._set(component, start, stop, component=component, start=int(start))

        except Exception as error:
            self.logger.error("Failed to set start/stop times for component %s", component)
            self.logger.error(str(error))

    def get(self, component: str, metric: str = "lap") -> list:
        """
        The function to get the metric for a given component
        :param component: component name
        :param metric: metric name, e. g. lap, start or stop
        :return: metric times
        """
        try:
            return super()._get(metric, component=component)

        except Exception as error:
            self.logger.error("Failed to get %s times for component %s", metric, component)
            self.logger.error(str(error))

    def purge(self, expiredDays: int = 15) -> None:
        """
        The function to delete all start/stop data if it is expired
        :param expiredDays: passed days from expiration time so that data can be deleted
        """
        try:
            super()._purge("start", expiredDays, localtime())

        except Exception as error:
            self.logger.error("Failed to purge start/stop times expired for more than %s days", expiredDays)
            self.logger.error(str(error))
