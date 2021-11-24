from logging import Logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from Databases.Oracle import OracleDB
from Utilities.Logging import getLogger

from typing import Optional


class OracleClient(object):
    """
    __OracleClient__
    General API for connecting to Oracle
    """

    def __init__(self, logger: Optional[Logger] = None) -> None:
        try:
            super().__init__()
            self.logger = logger or getLogger(self.__class__.__name__)
            self.base = OracleDB.Base

            with open(f"/data/unifiedPy3/secrets/secret_cmsr_rw.txt", "r") as file:
                self.oracleUrl = file.read().strip()

            self.engine = create_engine(self.oracleUrl)
            self.base.metadata.create_all(self.engine)
            self.base.metadata.bind = self.engine
            self.session = self.startSession()

        except Exception as error:
            raise Exception(f"Error initializing OracleClient\n{str(error)}")

    def __del__(self):
        if isinstance(self.session, Session):
            self.session.close()

    def startSession(self) -> Session:
        """
        The function to start the session
        :return: session
        """
        try:
            return sessionmaker(bind=self.engine)()

        except Exception as error:
            self.logger.error("Failed to start Oracle session")
            self.logger.error(str(error))
