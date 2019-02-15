#!/usr/bin/env python
from collections import deque
from assignSession import *
from utils import closeoutInfo, reportInfo, reqmgr_url, componentInfo, moduleLock
from wtcClient import wtcClient
from JIRAClient import JIRAClient
import optparse

def rulor(spec=None, options=None):
    
    mlock = moduleLock()
    if mlock(): return 

    up = componentInfo(soft=['mcm','wtc'])
    if not up.check(): return
    
    if spec:
        wfs = session.query(Workflow).filter(Workflow.status.contains('manual')).filter(Workflow.name.contains(spec)).all()
    else:
        wfs = session.query(Workflow).filter(Workflow.status.contains('manual')).all()
    
    COI = closeoutInfo()
    RI = reportInfo()
    WC = wtcClient()
    JC = JIRAClient()
    
    for wfo in wfs:
        record = COI.get( wfo.name )
        report = RI.get( wfo.name )
        if not record: 
            print "no information to look at"
            continue
        print record
        print report

        
        ## parse the information and produce an action document
        ### a rule for on-going issue with memory in campaign ...
        acted = False
        if "some conditions":
            action_doc =  { 'workflow' : wfo.name,
                            'name' : "a task name",
                            'parameters' : { 'action' : 'acdc',
                                             'memory' : 5000}
                        }
            acted = True
        if acted: continue
        if "some other conditions":
            pass
        if acted: continue

if __name__ == "__main__":
    url = reqmgr_url
    
    print "yup"
    parser = optparse.OptionParser()
    parser.add_option('--dummy',help='what?',default=False)
    (options,args) = parser.parse_args()
    spec= args[0] if len(args)!=0 else None

    rulor(spec, options)
