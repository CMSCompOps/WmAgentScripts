"""
File       : ConfigurationHandler.py
Author     : Hasan Ozturk <haozturk AT cern dot com>
Description: Class for Unified configuration
"""

import json
import sys

from typing import Any


class ConfigurationHandler(object):
    """
    _ConfigurationHandler_
    General API for handling a configuration
    """

    def __init__(self, configFile: str = "config/serviceConfiguration.json") -> None:
        super().__init__()

        self.configFile = configFile
        if self.configFile is None:
            self.configs = self.configFile
        else:
            try:
                with open(self.configFile, "r") as file:
                    self.configs = json.loads(file.read())
            except Exception as ex:
                print(f"Could not read configuration file: {self.configFile}\nException: {str(ex)}")
                sys.exit(124)

    def get(self, parameter: str) -> Any:
        """
        The function to get a given parameter from the configuration
        :param parameter: parameter name
        :return: configuration
        """
        if self.configs:
            if parameter in self.configs:
                return self.configs[parameter]["value"]
            else:
                print(parameter, "is not defined in global configuration")
                print(",".join(list(self.configs.keys())), "possible")
                sys.exit(124)
