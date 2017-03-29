#from assignSchema import * ## this is the old afs-sqlite based database
from assignSchema2 import Base, Workflow, Output, Transfer, Lock, engine, TransferImp
from sqlalchemy.orm import sessionmaker
import time
import copy 
import random

Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)
session = DBSession()

