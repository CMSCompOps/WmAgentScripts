import os
import sys
import socket
from logging import Logger
from bson.objectid import ObjectId
from pymongo.collection import Collection
from time import struct_time, gmtime, mktime, asctime, sleep

from Utilities.Logging import displayTime
from Databases.Mongo.MongoClient import MongoClient

from typing import Optional, List


class ModuleLockController(MongoClient):
    """
    __ModuleLockController__
    General API for locking (or not) a module
    """

    def __init__(self, logger: Optional[Logger] = None, **kwargs) -> None:
        try:
            super().__init__(logger=logger)
            self.poll = 30
            self.pid = os.getpid()
            self.host = socket.gethostname()

            self.component = kwargs.get("component") or sys._getframe(1).f_code.co_name
            self.wait = kwargs.get("wait") or False
            self.maxWait = kwargs.get("maxWait") or 18000
            self.silent = kwargs.get("silent") or False
            self.locking = kwargs.get("locking") or True

            self.logMsg = {
                "noGo": "There are %s instances running. Possible deadlock. Tried for %s [s]: %s",
                "killPid": "Process %s on %s for module %s is running for %s: killing",
                "popPid": "Process %s not present on %s",
            }

        except Exception as error:
            raise Exception(f"Error initializing ModuleLockController\n{str(error)}")

    def __del__(self) -> None:
        self.clean(component=self.component, pid=self.pid, host=self.host)

    def _setMongoCollection(self) -> Collection:
        return self.client.unified.moduleLock

    def _buildMongoDocument(self, now: struct_time = gmtime()) -> dict:
        return {
            "component": self.component,
            "host": self.host,
            "pid": self.pid,
            "time": int(mktime(now)),
            "date": asctime(now),
        }

    def isLocked(self) -> bool:
        """
        The function to check if the component is locked
        :return: True if locked, False o/w
        """
        self.logger.info("Module lock for component %s from MongoDB", self.component)
        go = self.go()
        if go:
            self.set()
        return not go

    def set(self) -> None:
        """
        The function to set a module in the module lock
        """
        try:
            super()._set()

        except Exception as error:
            self.logger.error("Failed to set module lock")
            self.logger.error(str(error))

    def get(self, **query) -> List[dict]:
        """
        The function to get the module locks
        :param query: optional query params
        :return: module locks
        """
        try:
            return list(super()._get("_id", details=True, **query).values())

        except Exception as error:
            self.logger.error("Failed to get the module locks")
            self.logger.error(str(error))

    def pop(self, id: ObjectId) -> None:
        """
        The function to delete a process from the module lock
        :param id: process id
        """
        try:
            super()._pop(_id=id)

        except Exception as error:
            self.logger.error("Failed to pop the process from the module lock")
            self.logger.error(str(error))

    def clean(self, component: Optional[str] = None, **query) -> None:
        """
        The function to delete data from the module lock for a given component
        :param component: component name
        :param query: optional query params, such as pid or host
        """
        try:
            super()._clean(component=component, **query)

        except Exception as error:
            self.logger.error("Failed to clean the module lock for component %s", component)
            self.logger.error(str(error))

    def check(self, hoursBeforeKill: int = 24) -> None:
        """
        The function to check processes running on host
        :param hoursBeforeKill: passed hours since a process started running so that it can be killed
        """
        try:
            now = mktime(gmtime())
            hostLocks = self.get(host=self.host)
            for lock in hostLocks:
                pid = lock.get("pid")
                self.logger.info(f"Checking on {pid} on {self.host}")
                runningSince = now - lock.get("time", now)
                if runningSince > hoursBeforeKill * 3600:
                    self.logger.critical(
                        self.logMsg["killPid"],
                        pid,
                        self.host,
                        lock.get("component"),
                        displayTime(runningSince),
                    )
                    os.system(f"sudo kill -9 {pid}")
                    sleep(2)

                if not os.path.isdir(f"/proc/{pid}"):
                    self.logger.critical(self.logMsg["popPid"], pid, self.host)
                    self.pop(lock.get("_id"))

        except Exception as error:
            self.logger.error("Failed to check module locks")
            self.logger.error(str(error))

    def go(self, polled: int = 0, locks: list = []) -> bool:
        """
        The function to check if a module is allowed to go.
        :param polled: polled time
        :param locks: locking components
        :return: True if allowed to go, False o/w
        """
        try:
            if not self.locking:
                return True

            if self.maxWait and polled > self.maxWait:
                self.logger.info("Stop waiting for %s to be released", self.component)
                if not self.silent:
                    self.logger.critical(self.logMsg["noGo"], len(locks), polled, locks)
                return False

            locks = self.get(component=self.component)
            if not locks:
                return True

            if not self.wait:
                if not self.silent:
                    self.logger.critical(self.logMsg["noGo"], len(locks), polled, locks)
                return False

            self.logger.info("Waiting for other %s components to stop running: %s", self.component, locks)
            sleep(self.poll)
            polled += self.poll
            return self.go(polled, locks)

        except Exception as error:
            self.logger.error("Failed to go for %s", self.component)
            self.logger.error(str(error))
