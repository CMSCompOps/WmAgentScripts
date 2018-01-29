import os
import sys
from sqlalchemy import Column, ForeignKey, Integer, String, PickleType, Float, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine
from sqlalchemy.schema import Sequence

Admin_Mode = True

Base = declarative_base()

#class McMID(Base):
#    __tablename__ = 'mcm'
#    id = Column(Integer, primary_key=True)
#    pid = Column(String(400))
#    ## and whatever else you want

def prefix():
    return '' if Admin_Mode else 'cms_unified_admin.'

class Workflow(Base):
    __tablename__ = prefix()+'workflow'
    id = Column(Integer, Sequence(prefix()+'workflow_id_seq'), primary_key=True)
    name = Column(String(400))
    status = Column(String(100),default='considered') ## internal status
    wm_status = Column(String(100),default='assignment-approved') ## status in req manager : we might not be carrying much actually since we are between ass-approved and assigned, although announced is coming afterwards


class Output(Base):
    __tablename__ = prefix()+'output'
    id = Column(Integer, Sequence(prefix()+'output_id_seq'), primary_key=True)
    datasetname = Column(String(400))
    nlumis = Column(Integer)
    expectedlumis = Column(Integer)
    nevents = Column(Integer)
    nblocks = Column(Integer)
    dsb_status = Column(String(30)) ## in DBS ?
    status = Column(String(30))
    ## workflow it belongs to
    workfow_id = Column(Integer,ForeignKey(prefix()+'workflow.id'))
    workflow = relationship(Workflow)
    date = Column(Integer)

class Transfer(Base):
    __tablename__ = prefix()+'transfer'
    id = Column(Integer, Sequence(prefix()+'transfer_id_seq'), primary_key=True)
    phedexid = Column(Integer)
    workflows_id = Column(PickleType)
    #status = Column(String(30))  ## to be added ?

class TransferImp(Base):
    __tablename__ = prefix()+'transferimp'
    id = Column(Integer, Sequence(prefix()+'transferimp_id_seq'), primary_key=True)
    phedexid = Column(Integer)
    workflow_id = Column(Integer,ForeignKey(prefix()+'workflow.id'))
    workflow = relationship(Workflow)
    active = Column(Boolean, default=True)

class Lock(Base):
    __tablename__ = prefix()+'lock'
    id = Column(Integer, Sequence(prefix()+'lock_id_seq'), primary_key=True)
    item = Column(String(500))
    lock = Column(Boolean)
    is_block = Column(Boolean)
    #site = Column(String(400))
    time = Column(Integer)
    reason = Column(String(400))

class LogRecord(Base):
    __tablename__ = prefix()+'logrecord'
    id = Column(Integer, Sequence(prefix()+'logrecord_id_seq'), primary_key=True)
    workflow = Column(String(400))
    logfile = Column(String(400))
    path = Column(String(400))
    task = Column(String(40))
    year = Column(Integer)
    month = Column(Integer)

if Admin_Mode:
    secret = open('Unified/secret_cmsr_admin.txt','r').read().strip()
else:
    secret = open('Unified/secret_cmsr_rw.txt','r').read().strip()    

engine = create_engine(secret)
Base.metadata.create_all(engine)
