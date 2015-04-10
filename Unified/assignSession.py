from assignSchema import *
from sqlalchemy.orm import sessionmaker
import time
import copy 
import random

Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)
session = DBSession()

