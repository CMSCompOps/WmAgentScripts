from assignSchema import Base, Workflow, Output, Transfer, Lock, engine, TransferImp, LogRecord, LockOfLock
from sqlalchemy.orm import sessionmaker

Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)
session = DBSession()

