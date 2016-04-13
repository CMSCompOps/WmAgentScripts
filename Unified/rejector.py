#!/usr/bin/env python
from assignSession import *
import sys
import reqMgrClient
from utils import workflowInfo, setDatasetStatus
from utils import componentInfo, reqmgr_url, getWorkflowById
from utils import componentInfo, getWorkflowById
import optparse
import json
import re

def rejector(url, specific, options=None):

    up = componentInfo()

    if specific and specific.startswith('/'):
        return

    if options.filelist:
        wfs = []
        for line in filter(None, open(options.filelist).read().split('\n')):
            print line
            wfs.extend( session.query(Workflow).filter(Workflow.name.contains(line)).all())
    elif specific:
        wfs = session.query(Workflow).filter(Workflow.name.contains(specific)).all()
    else:
        return 

    print len(wfs),"to reject"

    if len(wfs)>1:
        print "\n".join( [wfo.name for wfo in wfs] )
        answer = raw_input('Reject these')
        if not answer.lower() in ['y','yes']:
            return
        
    for wfo in wfs:
        #wfo = session.query(Workflow).filter(Workflow.name == specific).first()
        if not wfo:
            print "cannot reject",spec
            return
        results=[]
        wfi = workflowInfo(url, wfo.name)
        if wfi.request['RequestStatus'] in ['rejected','rejected-archived','aborted','aborted-archived']:
            print 'already',wfi.request['RequestStatus']
            if not options.clone:
                wfo.status = 'forget'
                session.commit()
                continue

        datasets = set(wfi.request['OutputDatasets'])
        reqMgrClient.invalidateWorkflow(url, wfo.name, current_status=wfi.request['RequestStatus'])
        ## need to find the whole familly and reject the whole gang
        familly = getWorkflowById( url, wfi.request['PrepID'] , details=True)
        for fwl in familly:
            if fwl['RequestDate'] < wfi.request['RequestDate']: continue
            if fwl['RequestType']!='Resubmission': continue
            print "rejecting",fwl['RequestName']
            reqMgrClient.invalidateWorkflow(url, fwl['RequestName'], current_status=fwl['RequestStatus'])
            datasets.update( fwl['OutputDatasets'] )

        for dataset in datasets:
            if options.keep:
                print "keeping",dataset,"in its current status"
            else:
                results.append( setDatasetStatus(dataset, 'INVALID') )

        if all(map(lambda result : result in ['None',None,True],results)):
            wfo.status = 'forget'
            session.commit()
            print wfo.name,"and",datasets,"are rejected"
            if options and options.clone:
                schema = wfi.getSchema()
                schema['Requestor'] = os.getenv('USER')
                schema['Group'] = 'DATAOPS'
                schema['OriginalRequestName'] = wfo.name
                if 'ProcessingVersion' in schema:
                    schema['ProcessingVersion'] = int(schema['ProcessingVersion'])+1 ## dubious str->int conversion
                else:
                    schema['ProcessingVersion']=2
                for k in schema.keys():
                    if k.startswith('Team'):
                        schema.pop(k)
                    if k.startswith('checkbox'):
                        schema.pop(k)

                ## a few tampering of the original request
                if options.Memory:
                    schema['Memory'] = options.Memory
                if options.deterministic:
                    if schema['RequestType'] == 'TaskChain':
                        schema['Task1']['DeterministicPileup']  = True
                if options.EventsPerJob:
                    if schema['RequestType'] == 'TaskChain':
                        schema['Task1']['EventsPerJob'] = options.EventsPerJob
                    else:
                        schema['EventsPerJob'] = options.EventsPerJob
                if options.TimePerEvent:
                    schema['TimePerEvent'] = options.TimePerEvent

                if options.ProcessingString:
                    schema['ProcessingString'] = options.ProcessingString
                if options.AcquisitionEra:
                    schema['AcquisitionEra'] = options.AcquisitionEra
                if options.runs:
                    schema['RunWhitelist'] = map(int,options.runs.split(','))
                if options.PrepID:
                    schema['PrepID'] =options.PrepID

                if schema['RequestType'] == 'TaskChain' and options.no_output:
                    ntask = schema['TaskChain']
                    for it in range(1,ntask-1):
                        schema['Task%d'%it]['KeepOutput'] = False
                    schema['TaskChain'] = ntask-1
                    schema.pop('Task%d'%ntask)

                ## update to the current priority
                schema['RequestPriority'] = wfi.request['RequestPriority']

                ## drop shit on the way to reqmgr2
                for p in ['RequestStatus',
                          'RequestTransition',
                          'RequestorDN',
                          'RequestWorkflow',
                          'OutputDatasets',
                          'ReqMgr2Only',
                          #'Group',
                          'RequestDate',
                          #'ConfigCacheUrl',
                          'RequestName',
                          'timeStamp',
                          'SoftwareVersions',
                          'CouchURL'
                          ]:
                    if p in schema:
                        schema.pop( p )
                        #pass
                print "submitting"
                print json.dumps( schema, indent=2 )
                newWorkflow = reqMgrClient.submitWorkflow(url, schema)
                if not newWorkflow:
                    print "error in cloning",wfo.name
                    print json.dumps( schema, indent=2 )
                    return 
                print newWorkflow
                #m = re.search("details\/(.*)\'",response)
                #if not m:
                #    print "error in cloning",wfo.name
                #    print response
                #    print json.dumps( schema, indent=2 )
                #    return 
                #newWorkflow = m.group(1)

                data = reqMgrClient.setWorkflowApproved(url, newWorkflow)
                print data
                wfo.status = 'trouble'
                session.commit()
        else:
            print "error in rejecting",wfo.name,results

if __name__ == "__main__":
    url = reqmgr_url

    parser = optparse.OptionParser()
    parser.add_option('-c','--clone',help="clone the workflow",default=False,action="store_true")
    parser.add_option('-k','--keep',help="keep the outpuy in current status", default=False,action="store_true")
    parser.add_option('--Memory',help="memory parameter of the clone", default=0, type=int)
    parser.add_option('--ProcessingString',help="change the proc string", default=None)
    parser.add_option('--AcquisitionEra',help="change the acq era", default=None)
    parser.add_option('--PrepID',help='change the prepid',default=None)
    parser.add_option('--EventsPerJob', help="set the events/job on the clone", default=0, type=int)
    parser.add_option('--TimePerEvent', help="set the time/event on the clone", default=0, type=float)
    parser.add_option('--filelist',help='a file with a list of workflows',default=None)
    parser.add_option('--no_output',help='keep only the output of the last task of TaskChain',default=False,action='store_true')
    parser.add_option('--deterministic',help='set the splitting to deterministic in the clone',default=False,action='store_true')
    parser.add_option('--runs',help='set the run whitelist in the clone',default=None)
    (options,args) = parser.parse_args()

    spec=None
    if len(args):
        spec = args[0]
    
    rejector(url, spec, options)
