import os
import json
from logging import Logger

from Utilities.Logging import getLogger

from typing import Optional


class TrelloClient(object):
    """
    __TrelloClient__
    General API for connecting to Trello board
    """

    def __init__(self, logger: Optional[Logger] = None) -> None:
        try:
            super().__init__()
            self.logger = logger or getLogger(self.__class__.__name__)

            self.configs = {}
            with open("config/secret_trello.txt") as file:
                for line in file:
                    try:
                        k, v = line.replace("\n", "").split(":")
                        self.configs[k] = v
                    except Exception as e:
                        print(f"{file} does not follow convention of key:value")

            self._syncAgents()

        except Exception as error:
            raise Exception(f"Error initializing TrelloClient\n{str(error)}")

    def _getResponse(self, key: str, id: str, param: str = "?") -> dict:
        """
        The function to get data from the Trello board
        :param key: key name
        :param id: key id
        :param param: optional params
        :return: data in Trello
        """
        try:
            url = f"https://api.trello.com/1/{key}/{id}{param}&key={self.configs['key']}&token={self.configs['token']}"
            with os.popen(f'curl -s "{url}"') as file:
                data = json.loads(file.read())
            return data

        except Exception as error:
            self.logger.error("Failed to get %s with id %s", key, id)
            self.logger.error(str(error))

    def _sendResponse(self, key: str, id: str, param=dict) -> None:
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

    def _syncAgents(self) -> None:
        """
        The function to get all agents in the Trello board
        :return: agents
        """
        try:
            self.agents = {}
            for card in self.getBoard():
                name = card.get("name", "").split(" - ")[0].strip()
                domaine = ".cern.ch" if "vocms" in name else ".fnal.gov"
                self.agents[name + domaine] = card.get("id")

        except Exception as error:
            self.logger.error("Failed to get agents")
            self.logger.error(str(error))

    def getBoard(self) -> dict:
        """
        The function to get all cards in the Trello board
        :return: board info
        """
        return self._getResponse("boards", "4np6TByB", "/cards?fields=name,url")

    def getCard(self, name: str) -> dict:
        """
        The function to get the card info for a given agent
        :param name: agent name
        :return: card info
        """
        return self._getResponse("cards", self.agents.get(name, name))

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

    def setList(self, name: str, status: str) -> None:
        """
        The function to set a given agent into the list of a given status
        :param name: agent name
        :param status: status name
        """
        return self._sendResponse("cards", self.getCard(name).get("id"), {"idList": self.getListId(status)})
