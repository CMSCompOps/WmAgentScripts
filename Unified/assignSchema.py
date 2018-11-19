import os
import sys
from sqlalchemy import Column, ForeignKey, Integer, String, PickleType, Float, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine
from sqlalchemy.schema import Sequence

Admin_Mode = False

Base = declarative_base()

def schema():
    return None if Admin_Mode else 'cms_unified_admin'
def prefix():
    return '' if Admin_Mode else schema()+'.'
def table_args():
    return {} if Admin_Mode else { "schema" : schema() }

class Workflow(Base):
    __tablename__ = 'WORKFLOW'
    __table_args__ = table_args()
    id = Column(Integer, Sequence('WORKFLOW_ID_SEQ', schema=schema()), primary_key=True)
    name = Column(String(400))
    status = Column(String(100),default='considered') ## internal status
    wm_status = Column(String(100),default='assignment-approved') ## status in req manager : we might not be carrying much actually since we are between ass-approved and assigned, although announced is coming afterwards


class Output(Base):
    __tablename__ = 'OUTPUT'
    __table_args__ = table_args()
    id = Column(Integer, Sequence('OUTPUT_ID_SEQ', schema=schema()), primary_key=True)
    datasetname = Column(String(400))
    nlumis = Column(Integer)
    expectedlumis = Column(Integer)
    nevents = Column(Integer)
    nblocks = Column(Integer)
    dsb_status = Column(String(30)) ## in DBS ?
    status = Column(String(30))
    ## workflow it belongs to
    workfow_id = Column(Integer,ForeignKey(prefix()+'WORKFLOW.id'))
    workflow = relationship(Workflow)
    date = Column(Integer)

class Transfer(Base):
    __tablename__ = 'TRANSFER'
    __table_args__ = table_args()
    id = Column(Integer, Sequence('TRANSFER_ID_SEQ', schema=schema()), primary_key=True)
    phedexid = Column(Integer)
    workflows_id = Column(PickleType)
    #status = Column(String(30))  ## to be added ?

class TransferImp(Base):
    __tablename__ = 'TRANSFERIMP'
    __table_args__ = table_args()
    id = Column(Integer, Sequence('TRANSFERIMP_ID_SEQ', schema=schema()), primary_key=True)
    phedexid = Column(Integer)
    workflow_id = Column(Integer,ForeignKey(prefix()+'WORKFLOW.id'))
    workflow = relationship(Workflow)
    active = Column(Boolean, default=True)

class LockOfLock(Base):
    __tablename__ = 'LOCKOFLOCK'
    __table_args__ = table_args()
    id = Column(Integer, Sequence('LOCKOFLOCK_ID_SEQ', schema=schema()), primary_key=True)
    lock = Column(Boolean)
    time = Column(Integer)
    endtime = Column(Integer)
    owner = Column(String(300))

class Lock(Base):
    __tablename__ = 'lock'
    __table_args__ = table_args()
    id = Column(Integer, Sequence('lock_id_seq', schema=schema()), primary_key=True)
    item = Column(String(500))
    lock = Column(Boolean)
    is_block = Column(Boolean)
    #site = Column(String(400))
    time = Column(Integer)
    reason = Column(String(400))

class LogRecord(Base):
    __tablename__ = 'logrecord'
    __table_args__ = table_args()
    id = Column(Integer, Sequence('logrecord_id_seq', schema=schema()), primary_key=True)
    workflow = Column(String(400))
    logfile = Column(String(400))
    path = Column(String(400))
    task = Column(String(40))
    year = Column(Integer)
    month = Column(Integer)

if Admin_Mode:
    print "Using the admin account"
    secret = open('Unified/secret_cmsr_admin.txt','r').read().strip()
else:
    print "Using the rw account"
    secret = open('Unified/secret_cmsr_rw.txt','r').read().strip()    

engine = create_engine(secret)
Base.metadata.create_all(engine)
