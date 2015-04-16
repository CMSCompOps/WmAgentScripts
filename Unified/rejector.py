from assignSession import *
import sys
import reqMgrClient
import setDatasetStatusDBS3
from utils import getWorkLoad

def rejector(url, specific ):

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
            results.append( setDatasetStatusDBS3.setStatusDBS3('https://cmsweb.cern.ch/dbs/prod/global/DBSWriter', dataset, 'INVALID', None) )
        if all(map(lambda result : result in ['None',None],results)):
            wfo.status = 'forget'
            session.commit()
            print wfo.name,"and",datasets,"are rejected"
        else:
            print "error in rejecting",wfo.name,results

if __name__ == "__main__":
    url = 'cmsweb.cern.ch'
    spec = sys.argv[1]
    rejector(url, spec)
