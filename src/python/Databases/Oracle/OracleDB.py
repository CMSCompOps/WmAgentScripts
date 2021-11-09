from sqlalchemy import Column, Integer, String, ForeignKey, PickleType, Boolean
from sqlalchemy.schema import Sequence
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base


class Base(object):
    """
    __Base__
    Declarative Base Model for Oracle Tables
    """

    schema = "cms_unified_admin"
    __table_args__ = {"schema": "cms_unified_admin"}


Base = declarative_base(cls=Base)


class Workflow(Base):
    """
    __Workflow__
    Oracle Workflow Table Model
    """

    __tablename__ = "WORKFLOW"
    id = Column(Integer, Sequence("WORKFLOW_ID_SEQ", schema=Base.schema), primary_key=True)
    name = Column(String(400))
    status = Column(String(100), default="considered")
    wm_status = Column(String(100), default="assignment-approved")


class Output(Base):
    """
    __Output__
    Oracle Output Table Model
    """

    __tablename__ = "OUTPUT"
    id = Column(Integer, Sequence("OUTPUT_ID_SEQ", schema=Base.schema), primary_key=True)
    datasetname = Column(String(400))
    nlumis = Column(Integer)
    expectedlumis = Column(Integer)
    nevents = Column(Integer)
    nblocks = Column(Integer)
    dsb_status = Column(String(30))
    status = Column(String(30))
    workfow_id = Column(Integer, ForeignKey(Base.schema + ".WORKFLOW.id"))
    workflow = relationship(Workflow)
    date = Column(Integer)


class Transfer(Base):
    """
    __Transfer__
    Oracle Transfer Table Model
    """

    __tablename__ = "TRANSFER"
    id = Column(Integer, Sequence("TRANSFER_ID_SEQ", schema=Base.schema), primary_key=True)
    phedexid = Column(Integer)
    workflows_id = Column(PickleType)


class LockOfLock(Base):
    """
    __LockOfLock__
    Oracle LockOfLock Table Model
    """

    __tablename__ = "LOCKOFLOCK"
    id = Column(Integer, Sequence("LOCKOFLOCK_ID_SEQ", schema=Base.schema), primary_key=True)
    lock = Column(Boolean)
    time = Column(Integer)
    endtime = Column(Integer)
    owner = Column(String(300))


class Lock(Base):
    """
    __Lock__
    Oracle Lock Table Model
    """

    __tablename__ = "lock"
    id = Column(Integer, Sequence("lock_id_seq", schema=Base.schema), primary_key=True)
    item = Column(String(500))
    lock = Column(Boolean)
    is_block = Column(Boolean)
    time = Column(Integer)
    reason = Column(String(400))


class LogRecord(Base):
    """
    __LogRecord__
    Oracle LogRecord Table Model
    """

    __tablename__ = "logrecord"
    id = Column(Integer, Sequence("logrecord_id_seq", schema=Base.schema), primary_key=True)
    workflow = Column(String(400))
    logfile = Column(String(400))
    path = Column(String(400))
    task = Column(String(40))
    year = Column(Integer)
    month = Column(Integer)
