import os
import sys
from sqlalchemy import Column, ForeignKey, Integer, String, PickleType, Float, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine

Base = declarative_base()

class Lock(Base):
    __tablename__ = 'lock'
    id = Column(Integer, primary_key=True)
    item = Column(String(500))
    lock = Column(Boolean)
    is_block = Column(Boolean)
    #site = Column(String(400))
    time = Column(Integer)
    reason = Column(String(400))

try:
    lockengine = create_engine('sqlite:///Unified/lockRecord.db')
    Base.metadata.create_all(lockengine)

    from sqlalchemy.orm import sessionmaker
    
    lDBSession = sessionmaker(bind=lockengine)
    locksession = lDBSession()
except:
    print "ignoring dataLock"
