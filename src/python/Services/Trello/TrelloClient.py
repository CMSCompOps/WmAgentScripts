import os
import json
import logging
from logging import Logger

from typing import Optional


class TrelloClient(object):
    """
    __TrelloClient__
    General API for connecting to Trello board
    """

    def __init__(self, configFile: str = "Unified/secret_trello.txt", logger: Optional[Logger] = None) -> None:
        try:
            super().__init__()
            self.configs = {}
            with open(configFile) as file:
                for line in file:
                    try:
                        k, v = line.replace("\n", "").split(":")
                        self.configs[k] = v
                    except Exception as e:
                        pass

            logging.basicConfig(level=logging.INFO)
            self.logger = logger or logging.getLogger(self.__class__.__name__)

        except Exception as error:
            raise Exception(f"Error initializing TrelloClient\n{str(error)}")

    def _get(self, key: str, id: str, endpoint: str = "?") -> dict:
        """
        The function to get data from the Trello board
        :param key: key name
        :param id: key id
        :param endpoint: endpoint
        :return: data in Trello
        """
        try:
            url = f"https://api.trello.com/1/{key}/{id}{endpoint}&key={self.configs['key']}&token={self.configs['token']}"
            with os.popen(f'curl -s "{url}"') as file:
                data = json.loads(file.read())
            return data

        except Exception as error:
            self.logger.error("Failed to get %s with id %s", key, id)
            self.logger.error(str(error))

    def getAgents(self) -> dict:
        """
        The function to get all agents in the Trello board
        :return: agents
        """
        try:
            agents = {}
            for card in self.getBoard():
                name = card.get("name")
                domaine = ".cern.ch" if "vocms" in name.split(" - ")[0] else ".fnal.gov"
                agents[name + domaine] = card.get("id")
            return agents

        except Exception as error:
            self.logger.error("Failed to get agents")
            self.logger.error(str(error))

    def getBoard(self) -> dict:
        """
        The function to get all cards in the Trello board
        :return: board info
        """
        return self._get("boards", "4np6TByB", "/cards?fields=name,url")

    def getCard(self, name: str) -> dict:
        """
        The function to get the card info for a given agent
        :param name: agent name
        :return: card info
        """
        return self._get("cards", self.getAgents().get(name, name))

    def getListId(self, status: str) -> str:
        """
        The function to get the list id for a given status
        :param status: status name
        :return: list id
        """
        try:
            trelloListIds = {
                "draining": "58da314ad8064a3772a8b2b7",
                "running": "58da313230415813a0f3c31c",
                "standby": "58da314194946756ba09b66e",
                "drained": "58da315628847a392b663927",
                "offline": "58da315628847a392b663927",
            }
            return trelloListIds.get(status)

        except Exception as error:
            self.logger.error("Failed to get list id for %s", status)
            self.logger.error(str(error))

    def _set(self, key: str, id: str, param=dict) -> None:
        """
        The function to set data in the Trello board
        :param key: key name
        :param id: key id
        :param param: param values
        """
        try:
            param = "&".join(f"{k}={v}" for k, v in param.items())
            url = f"https://api.trello.com/1/{key}/{id}{param}&key={self.configs['key']}&token={self.configs['token']}"
            with os.popen(f'curl -s --request PUT --url "{url}"') as file:
                data = json.loads(file.read())
            return data

        except Exception as error:
            self.logger.error("Failed to set %s with %s, %s", key, id, param)
            self.logger.error(str(error))

    def setList(self, name: str, status: str) -> None:
        """
        The function to set a given agent into the list of a given status
        :param name: agent name
        :param status: status name
        """
        return self._set("cards", self.getCard(name).get("id"), {"idList": self.getListId(status)})
