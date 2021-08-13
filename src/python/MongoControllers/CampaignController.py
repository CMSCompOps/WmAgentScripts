import copy
from logging import Logger
from pymongo.collection import Collection

from Databases.Mongo.MongoClient import MongoClient
from WorkflowMgmt.SiteController import SiteController

from typing import Optional, Any


class CampaignController(MongoClient):
    """
    __CampaignController__
    General API for controlling the campaigns configuration info
    """

    def __init__(self, logger: Optional[Logger] = None) -> None:
        try:
            super().__init__(logger=logger)
            siteController = SiteController()
            self.allSites = siteController.allSites
            self.campaigns = self._setCampaigns()

        except Exception as error:
            raise Exception(f"Error initializing CampaignController\n{str(error)}")

    def _setMongoCollection(self) -> Collection:
        return self.client.unified.campaignsConfiguration

    def _setCampaigns(self) -> dict:
        """
        The function to set the campaigns in the collection, updating the SiteBlacklist
        :return: campaigns
        """
        try:
            campaigns = self.get()
            for name, document in campaigns.items():
                if "parameters" in document and "SiteBlacklist" in document["parameters"]:
                    for item in copy.deepcopy(document["parameters"]["SiteBlacklist"]):
                        if item.endswith("*"):
                            campaigns[name]["parameters"]["SiteBlacklist"].remove(item)
                            campaigns[name]["parameters"]["SiteBlacklist"].extend(
                                site for site in self.allSites if site.startswith(item[:-1])
                            )
            return campaigns

        except Exception as error:
            self.logger.error("Failed to set campaigns")
            self.logger.error(str(error))

    def _buildMongoDocument(self, name: str, data: dict, campaignType: Optional[str] = None) -> dict:
        data["name"] = name
        if campaignType:
            data["type"] = campaignType
        return data

    def set(self, data: dict, campaignType: Optional[str] = None) -> None:
        """
        The function to set new data in the campaigns info
        :param data: campaigns data
        :param campaignType: campaign type
        """
        try:
            for name, campaignData in data.items():
                super()._set(name, campaignData, campaignType, name=name)

        except Exception as error:
            self.logger.error("Failed to set campaign info")
            self.logger.error(str(error))

    def get(self) -> dict:
        """
        The function to get all the campaigns info
        :return: campaigns info
        """
        try:
            return super()._get("name", details=True)

        except Exception as error:
            self.logger.error("Failed to get the campaigns info")
            self.logger.error(str(error))

    def getCampaignValue(self, name: str, key: str, defaultValue: Any) -> Any:
        """
        The function to get the value of a given campaign for a given key
        :param name: campaign name
        :param key: key name
        :param defaultValue: default value if nothing is found
        :return: key value if any, default value o/w
        """
        try:
            if name in self.campaigns and key in self.campaigns[name]:
                return copy.deepcopy(self.campaigns[name][key])
            return copy.deepcopy(defaultValue)

        except Exception as error:
            self.logger.error("Failed to get campaign %s for %s", key, name)
            self.logger.error(str(error))

    def getCampaignParameters(self, name: str) -> dict:
        """
        The function to get the parameters for a given campaign
        :param name: campaign name
        :return: parameters
        """
        return self.getCampaignValue(name, "parameters", {})

    def getCampaigns(self, campaignType: Optional[str] = None) -> list:
        """
        The function to get all the campaigns of given type
        :param campaignType: campaign type, if any
        :return: campaigns names
        """
        try:
            return super()._get("name", type=campaignType) if campaignType else super()._get("name")

        except Exception as error:
            self.logger.error("Failed to get campaigns of type %s", campaignType)
            self.logger.error(str(error))

    def getSecondaries(self) -> list:
        """
        The function to get all the campaigns secondaries
        :return: secondaries
        """
        try:
            secondaries = set()
            for _, content in self.campaigns.items():
                if content.get("go"):
                    campaignSecondaries = content.get("secondaries", {})
                    secondaries.update(campaignSecondaries.keys())
            return list(sorted(secondaries))

        except Exception as error:
            self.logger.error("Failed to get secondaries from campaign info")
            self.logger.error(str(error))

    def pop(self, name: str) -> None:
        """
        The function to delete the info of a given campaign
        :param name: campaign name
        """
        try:
            super()._pop(name=name)

        except Exception as error:
            self.logger.error("Failed to delete info for campaign %s", name)
            self.logger.error(str(error))

    def go(self, name: str, label: Optional[str] = None) -> bool:
        """
        The function to check if a campaign is allowed to go
        :param name: campaign name
        :param label: label name
        :return: True if allowed to go, False o/w
        """
        try:
            if name in self.campaigns and self.campaigns[name]["go"]:
                if "labels" in self.campaigns[name]:
                    if label:
                        return (label in self.campaigns[name]["labels"]) or any(
                            l in label for l in self.campaigns[name]["labels"]
                        )
                    return False
                return True
            elif name in self.campaigns and not self.campaigns[name]["go"]:
                if label and "pilot" in label.lower():
                    return True
            return False

        except Exception as error:
            self.logger.error("Failed to go for %s, %s", name, label)
            self.logger.error(str(error))
