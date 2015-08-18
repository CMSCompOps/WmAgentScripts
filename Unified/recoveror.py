#!/usr/bin/env python
from assignSession import *
from utils import workflowInfo
import reqMgrClient
import json
import optparse
import copy

def singleRecovery(url, task , initial):
    payload = {
        "RequestType" : "Resubmission",
        "ACDCServer" : "https://cmsweb.cern.ch/couchdb",
        "ACDCDatabase" : "acdcserver",
        "OriginalRequestName" : initial['RequestName']
        }
    copy_over = ['PrepID','RequestPriority', 'TimePerEvent', 'SizePerEvent', 'Group', 'Memory', 'RequestString' ]        
    for c in copy_over:
        payload[c] = copy.deepcopy(initial[c])

    if payload['RequestString'].startswith('ACDC'):
        print "This is not allowed yet"
        return None
    payload['RequestString'] += 'ACDC_'
    payload['InitialTaskPath'] = task 

    ## submit
    response = reqMgrClient.submitWorkflow(url, payload)
    m = re.search("details\/(.*)\'",response)
    if not m:
        print "Error in making ACDC for",initial["RequestName"]
        return None
    acdc = m.group(1)
    data = reqMgrClient.setWorkflowApproved(url, acdc)
    print data
    return acdc

    

def recoveror(url,specific,options=None):

    error_codes_to_recover = {
        50664 : { "legend" : "time-out",
                  "solution" : "split" },
        8028 : { "legend" : "read error",
                 "solution" : "recover" },
        }
    ## CMSSW failures should just be reported right away and the workflow left on the side
    error_codes_to_notify = {
        8021 : { "message" : "Please take a look and come back to Ops." }
        }

    for wfo in session.query(Workflow).filter(Workflow.status == 'assistance-recovery').all():
        print wfo.name 
        if specific and not specific in wfo.name:continue

        wfi = workflowInfo(url, wfo.name)
        all_errors = None
        try:
            wfi.getSummary()
            all_errors = wfi.summary['errors']
        except:
            pass

        print '-'*100        
        if not len(all_errors): 
            print "no error for",wfo.name
            continue
        task_to_recover = defaultdict(set)
        notify_me = False

        for task,errors in all_errors.items():
            print task
            for name,codes in errors.items():
                if type(codes)==int: continue
                for errorCode,info in codes.items():
                    print "Task",task,"had",info['jobs'],"failures with error code",errorCode,"in stage",name
                    if int(errorCode) in error_codes_to_recover:
                        task_to_recover[task].add( int(errorCode) )
                    if int(errorCode) in error_codes_to_notify:
                        notify_me = True

        if notify_me:
            print wfo.name,"to be notified (DUMMY)"

        if task_to_recover:
            print wfo.name,"recovering"
            print ', '.join(task_to_recover.keys()),"to be recovered"

            recovering=set()
            for task in task_to_recover:
                print "Will be making a recovery workflow for",task
                continue
                acdc = singleRecovery(url, task, wfi.request )
                ## and assign it ?
                team = wfi.request['Teams'][0]
                parameters={
                    'SiteWhitelist' : wfi.request['SiteWhitelist'],
                    'AcquisitionEra' : wfi.request['AcquisitionEra'],
                    'ProcessingString' :  wfi.request['ProcessingString'],
                    'MergedLFNBase' : wfi.request['MergedLFNBase'],
                    'ProcessingVersion' : wfi.request['ProcessingVersion'],
                    }
                
                codes = task_to_recover[task]
                ## from here you can fetch known solutions, to known error codes
                for code in codes:
                    solution = error_codes_to_recover[code]['solution']
                    if solution == 'split':
                        ## reduce the splitting adequately
                        pass
                ###parameters['execute']=True ## to enable auto-assigning
                result = reqMgrClient.assignWorkflow(url, acdc, team, parameters)
                recovering.add( acdc )


            if recovering:
                #if all went well, set the status to -recovering ; which will create a lag in the assistance.html page
                current = wfo.status 
                current.replace('recovery','recovering')
                print wfo.name,"setting the status to",current
                wfo.status = current
                session.commit()


if __name__ == '__main__':
    url='cmsweb.cern.ch'
    parser = optparse.OptionParser()
    (options,args) = parser.parse_args()
    spec=None
    if len(args)!=0:
        spec = args[0]
        
    recoveror(url,spec,options=options)
