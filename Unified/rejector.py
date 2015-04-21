from assignSession import *
import sys
import reqMgrClient
import resubmit
import setDatasetStatusDBS3
from utils import getWorkLoad
import optparse

def rejector(url, specific, options=None):
    
    if specific.startswith('/'):
        pass
    else:
        wfo = session.query(Workflow).filter(Workflow.name == specific).first()
        if not wfo:
            print "cannot reject",spec
            return
        results=[]
        wl = getWorkLoad(url, wfo.name)
        if wl['RequestStatus'] in ['assignment-approved','new','completed']:
            #results.append( reqMgrClient.rejectWorkflow(url, wfo.name))
            reqMgrClient.rejectWorkflow(url, wfo.name)
        else:
            #results.append( reqMgrClient.abortWorkflow(url, wfo.name))
            reqMgrClient.abortWorkflow(url, wfo.name)
        
        datasets = wl['OutputDatasets']
        for dataset in datasets:
            #oo = session.query(Output).filter(Output.datasetname==dataset).first()
            #if oo:                oo.status = 'forget'
            results.append( setDatasetStatusDBS3.setStatusDBS3('https://cmsweb.cern.ch/dbs/prod/global/DBSWriter', dataset, 'INVALID', None) )
        if all(map(lambda result : result in ['None',None],results)):
            wfo.status = 'forget'
            session.commit()
            print wfo.name,"and",datasets,"are rejected"
            if options and options.clone:
                user = os.getenv('USER')
                group = 'DATAOPS'
                clone = resubmit.cloneWorkflow(wfo.name, user, group)
                wfo.status = 'trouble'
                session.commit()
        else:
            print "error in rejecting",wfo.name,results

if __name__ == "__main__":
    url = 'cmsweb.cern.ch'

    parser = optparse.OptionParser()
    parser.add_option('-c','--clone',help="clone the workflow",default=False,action="store_true")
    (options,args) = parser.parse_args()

    spec=None
    if len(args):
        spec = args[0]
    
    rejector(url, spec, options)
