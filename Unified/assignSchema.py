import os
import sys
from sqlalchemy import Column, ForeignKey, Integer, String, PickleType, Float, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine

Base = declarative_base()

#class McMID(Base):
#    __tablename__ = 'mcm'
#    id = Column(Integer, primary_key=True)
#    pid = Column(String(400))
#    ## and whatever else you want

class Workflow(Base):
    __tablename__ = 'workflow'
    id = Column(Integer, primary_key=True)
    name = Column(String(400))
    status = Column(String(30),default='considered') ## internal status
    wm_status = Column(String(30),default='assignment-approved') ## status in req manager : we might not be carrying much actually since we are between ass-approved and assigned, although announced is coming afterwards
    fraction_for_closing = Column(Float,default=0.90)

class Output(Base):
    __tablename__ = 'output'
    id = Column(Integer, primary_key=True)
    datasetname = Column(String(400))
    nlumis = Column(Integer)
    expectedlumis = Column(Integer)
    nevents = Column(Integer)
    nblocks = Column(Integer)
    dsb_status = Column(String(30)) ## in DBS ?
    status = Column(String(30))
    ## workflow it belongs to
    workfow_id = Column(Integer,ForeignKey('workflow.id'))
    workflow = relationship(Workflow)
    date = Column(Integer)

class Transfer(Base):
    __tablename__ = 'transfer'
    id = Column(Integer, primary_key=True)
    phedexid = Column(Integer)
    workflows_id = Column(PickleType)
    #status = Column(String(30))  ## to be added ?

 
engine = create_engine('sqlite:///Unified/assignRecord.db')
Base.metadata.create_all(engine)

