import os
import sys
import logging
from logging import Logger

from typing import Optional


class UserLockController(object):
    """
    _UserLockController_
    General API for controlling the user lock
    """

    def __init__(self, logger: Optional[Logger] = None, **kwargs) -> None:
        try:
            super().__init__()
            self.component = kwargs.get("component") or sys._getframe(1).f_code.co_name

            logging.basicConfig(level=logging.INFO)
            self.logger = logger or logging.getLogger(self.__class__.__name__)

        except Exception as error:
            raise Exception(f"Error initializing UserLockController\n{str(error)}")

    def __call__(self) -> bool:
        return not self.go()

    def go(self) -> bool:
        """
        The function to check if an user is allowed to go
        :return: True if allowed to go, False o/w
        """
        try:
            lockers = ["dmytro", "mcremone", "vlimant"]
            for lock in lockers:
                if os.path.isfile(f"/afs/cern.ch/user/{lock[0]}/{lock}/public/ops/{self.component}"):
                    self.logger.info("Disabled by %s", lock)
                    return False

            return True

        except Exception as error:
            self.logger.error("Failed to go for %s", self.component)
            self.logger.error(str(error))
