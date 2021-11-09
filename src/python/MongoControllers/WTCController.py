import os
from time import struct_time, gmtime, mktime, asctime
from pymongo.collection import Collection
from collections import defaultdict

from Databases.Mongo.MongoClient import MongoClient
from Services.ReqMgr.ReqMgrReader import ReqMgrReader

from typing import Optional


class WTCController(MongoClient):
    """
    __WTCController__
    General API for monitoring the workflow traffic controller
    """

    def _setMongoCollection(self) -> Collection:
        return self.client.unified.wtcInfo

    def _buildMongoDocument(self, action: str, keyword: str, user: str, now: struct_time = gmtime()) -> dict:
        return {
            "user": user,
            "keyword": keyword,
            "action": action,
            "time": int(mktime(now)),
            "date": asctime(now),
        }

    def set(self, action: str, keyword: str, user: Optional[str] = None) -> None:
        """
        The function to set new content in the collection
        :param action: action name
        :param keyword: keyword name
        :param user: user name
        """
        try:
            if not keyword:
                self.logger.warning("Blank keyword is not allowed")
                return None
            super()._set(action, keyword, user or os.environ.get("USER"), keyword=keyword)

        except Exception as error:
            self.logger.error("Failed to set workflow traffic for %s, %s", action, keyword)
            self.logger.error(str(error))

    def get(self, action: str) -> dict:
        """
        The function to get the workflow traffic for a given action
        :param action: action name
        :return: workflow traffic
        """
        try:
            content = defaultdict(list)
            allDocuments = super()._get("_id", details=True, action=action)
            for doc in allDocuments.values():
                content[doc["user"]].append(doc["keyword"])
            return dict(content)

        except Exception as error:
            self.logger.error("Failed to get the workflow traffic for action %s", action)
            self.logger.error(str(error))

    def getHold(self) -> dict:
        """
        The function to get on hold workflows in the workflow traffic
        :return: workflow traffic
        """
        return self.get("hold")

    def getBypass(self) -> dict:
        """
        The function to get bypass workflows in the workflow traffic
        :return: workflow traffic
        """
        return self.get("bypass")

    def getForce(self) -> dict:
        """
        The function to get force workflows in the workflow traffic
        :return: workflow traffic
        """
        return self.get("force")

    def clean(self) -> None:
        """
        The function to delete all archived/aborted/rejected workflow traffic info
        """
        try:
            workflows = []
            reqMgrReader = ReqMgrReader()
            for status in [
                "announced",
                "normal-archived",
                "rejected",
                "aborted",
                "aborted-archived",
                "rejected-archived",
            ]:
                workflows.extend(reqMgrReader.getWorkflowsByStatus(status))

            super()._clean(keyword={"$regex": "|".join(workflows)})

        except Exception as error:
            self.logger.error("Failed to clean the workflow traffic")
            self.logger.error(str(error))

    def cleanKeyword(self, keyword: str) -> None:
        """
        The function to delete the workflow traffic matching a given keyword
        :param keyword: keyword name
        """
        try:
            super()._clean(keyword={"$regex:": keyword})

        except Exception as error:
            self.logger.error("Failed to clean the workflow traffic for keyword %s", keyword)
            self.logger.error(str(error))
