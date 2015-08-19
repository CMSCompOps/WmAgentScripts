#!/usr/bin/env python
from assignSession import *
from utils import workflowInfo, sendEmail, componentInfo, userLock, closeoutInfo
import reqMgrClient
import json
import optparse
import copy
from collections import defaultdict
import re
import os

def singleRecovery(url, task , initial, do=False):
    payload = {
        "Requestor" : os.getenv('USER'),
        "Group" : 'DATAOPS',
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
    payload['RequestString'] = 'ACDC_'+payload['RequestString']
    payload['InitialTaskPath'] = task 

    if not do:
        print json.dumps( payload, indent=2)
        return None

    ## submit
    response = reqMgrClient.submitWorkflow(url, payload)
    m = re.search("details\/(.*)\'",response)
    if not m:
        print "Error in making ACDC for",initial["RequestName"]
        print response
        response = reqMgrClient.submitWorkflow(url, payload)
        m = re.search("details\/(.*)\'",response)
        if not m:
            print "Error twice in making ACDC for",initial["RequestName"]
            print response
            return None
    acdc = m.group(1)
    data = reqMgrClient.setWorkflowApproved(url, acdc)
    print data
    return acdc

    

def recoveror(url,specific,options=None):
    if userLock('recoveror'): return

    up = componentInfo()

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

        wfi = workflowInfo(url, wfo.name, deprecated=True) ## need deprecated info for mergedlfnbase
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
                    errorCode = int(errorCode)
                    print "Task",task,"had",info['jobs'],"failures with error code",errorCode,"in stage",name
                    if errorCode in error_codes_to_recover and not errorCode in task_to_recover[task]:
                        print "\twe should be able to recover that"
                        task_to_recover[task].add( errorCode )
                    if errorCode in error_codes_to_notify and not notify_me:
                        print "\twe should notify people on this"
                        notify_me = True

        if notify_me:
            print wfo.name,"to be notified (DUMMY)"

        if task_to_recover:
            print wfo.name,"recovering"
            print ', '.join(task_to_recover.keys()),"to be recovered"

            recovering=set()
            for task in task_to_recover:
                print "Will be making a recovery workflow for",task
                acdc = singleRecovery(url, task, wfi.request , do = options.do)
                if not acdc:
                    if options.do:
                        if recovering:
                            print wfo.name,"has been partially ACDCed. Needs manual attention"
                            sendEmail( "failed ACDC partial recovery","%s has had %s/%s recoveries %s only"%( wfo.name, len(recovering), len(task_to_recover), list(recovering)))
                            continue
                        else:
                            print wfo.name,"failed recovery once"
                            break
                    else:
                        print "no action to take further"
                        continue
                        
                
                ## and assign it ?
                team = wfi.request['Teams'][0]
                parameters={
                    'SiteWhitelist' : wfi.request['SiteWhitelist'],
                    'AcquisitionEra' : wfi.request['AcquisitionEra'],
                    'ProcessingString' :  wfi.request['ProcessingString'],
                    'MergedLFNBase' : wfi.deprecated_request['MergedLFNBase'],
                    'ProcessingVersion' : wfi.request['ProcessingVersion'],
                    }
                
                codes = task_to_recover[task]
                ## from here you can fetch known solutions, to known error codes
                for code in codes:
                    solution = error_codes_to_recover[code]['solution']
                    if solution == 'split':
                        ## reduce the splitting adequately
                        pass
                if options.ass:
                    print "really doing the assignment of the ACDC",acdc
                    parameters['execute']=True

                result = reqMgrClient.assignWorkflow(url, acdc, team, parameters)
                if acdc:
                    recovering.add( acdc )

            if recovering:
                #if all went well, set the status to -recovering ; which will create a lag in the assistance.html page
                current = wfo.status 
                current = current.replace('recovery','recovering')
                print wfo.name,"setting the status to",current
                wfo.status = current
                session.commit()


if __name__ == '__main__':
    url='cmsweb.cern.ch'
    parser = optparse.OptionParser()
    parser.add_option('--do',default=False,action='store_true')
    parser.add_option('--ass',default=False,action='store_true')
    (options,args) = parser.parse_args()
    spec=None
    if len(args)!=0:
        spec = args[0]
        
    recoveror(url,spec,options=options)

    fdb = closeoutInfo()
    fdb.assistance( session.query(Workflow).filter(Workflow.status.startswith('assistance')).all() )
