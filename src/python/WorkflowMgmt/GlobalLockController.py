import os
import socket
from logging import Logger
from time import mktime, gmtime

from Databases.Oracle.OracleClient import OracleClient
from Databases.Oracle.OracleDB import Lock, LockOfLock

from typing import Optional


class GlobalLockController(OracleClient):
    """
    _GlobalLockController_
    General API for controlling locks
    """

    def __init__(self, acquire: bool = True, logger: Optional[Logger] = None) -> None:
        try:
            super().__init__(logger=logger)

            self.owner = f"{socket.gethostname()}-{os.getpid()}"
            self.host = os.getenv("HOST", os.getenv("HOSTNAME", socket.gethostname()))

            if acquire:
                self.acquire()

        except Exception as error:
            raise Exception(f"Error initializing GlobalLockController\n{str(error)}")

    def __del__(self):
        self.release()

    def isLocked(self, item: str) -> bool:
        """
        The function to check if an item is locked
        :param item: item name
        :return: True if locked, False o/w
        """
        try:
            lock = self.session.query(Lock).filter(Lock.item == item).first()
            return bool(lock and lock.lock)

        except Exception as error:
            self.logger.error("Failed to check if %s is locked", item)
            self.logger.error(str(error))

    def isDeadlock(self, pid: str) -> bool:
        """
        The function to check if a process is a deadlock or not
        :param pid: process id
        :return: True if deadlock, False o/w
        """
        try:
            with os.popen(f"ps -e -f | grep {pid} | grep -v grep", "r") as file:
                process = file.read()

            isDeadlock = True if not process else False
            self.logger.info("The lock on %s is %s", pid, "a deadlock" if isDeadlock else "good")

        except Exception as error:
            self.logger.error("Failed to check if %s is a deadlock", pid)
            self.logger.error(str(error))

    def tell(self) -> None:
        """
        The function to print items and if locked
        """
        try:
            for lock in self.session.query(Lock).all():
                self.logger.info("Item: %s, Locked: %s", lock.item, lock.lock)

        except Exception as error:
            self.logger.error("Failed to tell all items")
            self.logger.error(str(error))

    def acquire(self) -> None:
        """
        The function to set a new lock of lock object with the current timestamp and owner
        """
        try:
            lockOfLock = LockOfLock(lock=True, time=int(mktime(gmtime())), owner=self.owner)
            self.session.add(lockOfLock)
            self.session.commit()

        except Exception as error:
            self.logger.error("Failed to acquire %s", self.owner)
            self.logger.error(str(error))

    def deadlock(self) -> None:
        """
        The function to clean deadlocks
        """
        try:
            for lockOfLock in (
                self.session.query(LockOfLock)
                .filter(LockOfLock.lock == True)
                .filter(LockOfLock.owner.contains(self.host))
                .all()
            ):
                _, lockPid = lockOfLock.owner.split("-")
                if self.isDeadlock(lockPid):
                    self.session.delete(lockOfLock)

            self.session.commit()

        except Exception as error:
            self.logger.error("Failed to clean %s locks", self.owner)
            self.logger.error(str(error))

    def lockItem(self, item: str, reason: Optional[str] = None) -> None:
        """
        The function to lock a item
        :param item: item name
        :param reason: optional reason
        """
        try:
            if not item:
                self.logger.error("Trying to lock %s", item)
                return

            commit = False
            lock = self.session.query(Lock).filter(Lock.item == item).first()

            if not lock:
                self.logger.info("In lock, making a new object for %s", item)
                lock = Lock(lock=False)
                lock.item = item
                lock.is_block = "#" in item
                self.session.add(lock)
                commit = True

            else:
                self.logger.info("Lock for %s already exists", item)

            logMsg = f"{item}"
            if lock.lock is not True:
                lock.lock, commit = True, True
                logMsg += " being locked"

            if reason and reason != lock.reason:
                lock.reason, commit = reason, True
                logMsg += f" because of {reason}"

            if commit:
                lock.time = int(mktime(gmtime))
                self.session.commit()
                self.logger.info(logMsg)

        except Exception as error:
            self.logger.error("Failed to lock item %s", item)
            self.logger.error(str(error))

    def release(self) -> None:
        """
        The function to release current owner lock
        """
        try:
            for lockOfLock in self.session.query(LockOfLock).filter(LockOfLock.owner == self.owner).all():
                lockOfLock.lock = False
                lockOfLock.endtime = int(mktime(gmtime()))

            self.session.commit()

        except Exception as error:
            self.logger.error("Failed to release %s locks", self.owner)
            self.logger.error(str(error))

    def releaseItem(self, item: str) -> None:
        """
        The function to release the lock of a given item
        :param item: item name
        """
        try:
            lock = self.session.query(Lock).filter(Lock.item == item).first()

            if not lock:
                self.logger.info("%s to be released is not locked", item)

            else:
                self.logger.info("Releasing %s", item)
                lock.lock = False
                self.session.commit()

        except Exception as error:
            self.logger.error("Failed to release item %s", item)
            self.logger.error(str(error))

    def getItems(self, locked: bool = True) -> list:
        """
        The function to get all items
        :param locked: return only locked items if True, o/w return only non-locked items
        :return: items
        """
        try:
            return sorted([lock.item for lock in self.session.query(Lock).all() if lock.lock == locked])

        except Exception as error:
            self.logger.error("Failed to get items")
            self.logger.error(str(error))
