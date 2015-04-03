from assignSession import *

def phinject( pid, wfs):
    ## and remove fakes
    for ts in session.query(Transfer).filter(Transfer.phedexid<=0).all():
        session.delete( ts )
        session.commit()
        pass

    if not pid: return

    ts = session.query(Transfer).filter(Transfer.phedexid == pid).first() 
    if not ts:
        ts = Transfer( phedexid = pid )
        session.add (ts )
    else:
        print "updating",pid


    wfs_id = []
    for wf in wfs:
        w = session.query(Workflow).filter(Workflow.name == wf).first()
        if w:
            print w.id
            wfs_id.append( w.id )
        else:
            print wf,"is not to be found"
    ts.workflows_id = wfs_id

    session.commit()

if __name__ == "__main__":
    phid = sys.argv[1]
    wfs = sys.argv[2]
    phinject( int(phid), wfs.split(','))
    

    
