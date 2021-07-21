import logging
from logging import Logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from typing import Optional

from Databases.Oracle import OracleDB


class OracleClient(object):
    """
    __OracleClient__
    General API for connecting to Oracle
    """

    def __init__(self, adminMode: bool = False, logger: Optional[Logger] = None) -> None:
        try:
            super().__init__()
            self.base = OracleDB.Base
            self.adminMode = adminMode

            with open(f"src/python/Utilities/secret_cmsr_{'admin' if self.adminMode else 'rw'}.txt", "r") as file:
                self.oracleUrl = file.read().strip()

            self.engine = create_engine(self.oracleUrl)
            if self.adminMode:
                self.engine = self.engine.execution_options(schema_translate_map={self.base.schema: None})

            self.base.metadata.create_all(self.engine)
            self.base.metadata.bind = self.engine

            self.session = self.startSession()

            logging.basicConfig(level=logging.INFO)
            self.logger = logger or logging.getLogger(self.__class__.__name__)

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
