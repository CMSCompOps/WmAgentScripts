#from assignSchema import * ## this is the old afs-sqlite based database
from assignSchemaTest import Base, Workflow, Output, Transfer, engine, TransferImp#, LogRecord, Lock
from sqlalchemy.orm import sessionmaker
import time
import copy 
import random

Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)
session = DBSession()

