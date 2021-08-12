"""
File       : ConfigurationHandler.py
Author     : Hasan Ozturk <haozturk AT cern dot com>
Description: Class for Unified configuration
"""

import json
import sys


class ConfigurationHandler(object):
    """
    _ConfigurationHandler_
    General API for handling a configuration
    """

    def __init__(self, configFile: str = "config/serviceConfiguration.json"):
        super().__init__()

        self.configFile = configFile
        if self.configFile is None:
            self.configs = self.configFile
        else:
            try:
                with open(self.configFile) as file:
                    self.configs = json.loads(file.read())
            except Exception as ex:
                print(("Could not read configuration file: %s\nException: %s" % (self.configFile, str(ex))))
                sys.exit(124)

    def get(self, parameter: str):
        """
        The function to get a given parameter from the configuration
        """
        if self.configs:
            if parameter in self.configs:
                return self.configs[parameter]["value"]
            else:
                print(parameter, "is not defined in global configuration")
                print(",".join(list(self.configs.keys())), "possible")
                sys.exit(124)
