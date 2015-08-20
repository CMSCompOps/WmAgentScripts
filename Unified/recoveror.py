#!/usr/bin/env python
from assignSession import *
from utils import workflowInfo, sendEmail, componentInfo, userLock, closeoutInfo, campaignInfo
import reqMgrClient
import json
import optparse
import copy
from collections import defaultdict
import re
import os

def singleRecovery(url, task , initial, actions, do=False):
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

    if actions:
        for action in actions:
            if action.startswith('split'):
                factor = int(action.split('-')[-1]) if '-' in action else 4
                print "Changing time per event (%s) by a factor %d"%( payload['TimePerEvent'], factor)
                ## mention it's taking 4 times longer to have a 4 times finer splitting
                payload['TimePerEvent'] = factor*payload['TimePerEvent']
            elif action == 'mem':
                ## increase the memory requirement by 1G
                payload['Memory'] = payload['Memory'] + 1000

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
    CI = campaignInfo()

    error_codes_to_recover = {
        50664 : { "legend" : "time-out",
                  "solution" : "split" ,
                  "rate" : 30 
                  },
        50660 : { "legend" : "memory excess",
                  "solution" : "mem" ,
                  "rate" : 20
                  },
        61104 : { "legend" : "failed submit",
                  "solution" : "recover" ,
                  "rate" : 20 
                  },
        8028 : { "legend" : "read error",
                 "solution" : "recover" ,
                 "rate" : 20 
                 },
        }
    max_legend = max([ len(e['legend']) for e in error_codes_to_recover.values()])

    ## CMSSW failures should just be reported right away and the workflow left on the side
    error_codes_to_notify = {
        8021 : { "message" : "Please take a look and come back to Ops." }
        }

    wfs = session.query(Workflow).filter(Workflow.status == 'assistance-recovery').all()
    if specific:
        wfs.extend( session.query(Workflow).filter(Workflow.status == 'assistance-manual').all() )

    for wfo in wfs:
        if specific and not specific in wfo.name:continue

        if 'manual' in wfo.status: continue
        
        wfi = workflowInfo(url, wfo.name, deprecated=True) ## need deprecated info for mergedlfnbase

        ## need a way to verify that this is the first round of ACDC, since the second round will have to be on the ACDC themselves

        all_errors = None
        try:
            wfi.getSummary()
            all_errors = wfi.summary['errors']
        except:
            pass

        print '-'*100        
        print "Looking at",wfo.name,"for recovery options"
        
        if not len(all_errors): 
            print "\tno error for",wfo.name

        task_to_recover = defaultdict(set)
        notify_me = False

        recover=True
        if 'LheInputFilese' in wfi.request and wfi.request['LheInputFiles']:
            ## we do not try to recover pLHE
            recover = False

        if 'Campaign' in wfi.request:
            c = wfi.request['Campaign']
            if c in CI.campaigns and 'recover' in CI.campaigns[c]:
                recover=CI.campaigns[c]['recover']

        for task,errors in all_errors.items():
            print "\tTask",task
            ## collect all error codes and #jobs regardless of step at which it occured
            all_codes = []
            for name, codes in errors.items():
                if type(codes)==int: continue
                all_codes.extend( [(int(code),info['jobs'],name,list(set([e['type'] for e in info['errors']]))) for code,info in codes.items()] )

            all_codes.sort(key=lambda i:i[1], reverse=True)
            sum_failed = sum([l[1] for l in all_codes])

            for errorCode,njobs,name,types in all_codes:
                legend = error_codes_to_recover[errorCode]['legend'] if errorCode in error_codes_to_recover else ','.join(types)
                                  
                rate = 100*njobs/float(sum_failed)
                print ("\t\t %10d (%6s%%) failures with error code %10d (%"+str(max_legend)+"s) at stage %s")%(njobs, "%4.2f"%rate, errorCode, legend, name)

                if errorCode in error_codes_to_recover and rate > error_codes_to_recover[errorCode]['rate']:
                    print "\t\t => we should be able to recover that",legend
                    task_to_recover[task].add( errorCode )

                if errorCode in error_codes_to_notify and not notify_me:
                    print "\t\t => we should notify people on this"
                    notify_me = True



        if notify_me:
            print wfo.name,"to be notified (DUMMY)"

        if task_to_recover and recover:
            print "Initiating recovery"
            print ', '.join(task_to_recover.keys()),"to be recovered"

            recovering=set()
            for task in task_to_recover:
                print "Will be making a recovery workflow for",task

                ## from here you can fetch known solutions, to known error codes
                actions = [error_codes_to_recover[code]['solution'] for code in task_to_recover[task]  ]
                acdc = singleRecovery(url, task, wfi.request , actions, do = options.do)

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
                        sendEmail("an ACDC that can be done automatically","please check https://cmst2.web.cern.ch/cmst2/unified/logs/recoveror/last.log for details", destination=['julian.badillo.rojas@cern.ch'])
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
                
                if options.ass:
                    print "really doing the assignment of the ACDC",acdc
                    parameters['execute']=True

                result = reqMgrClient.assignWorkflow(url, acdc, team, parameters)
                recovering.add( acdc )

            if recovering:
                #if all went well, set the status to -recovering 
                current = wfo.status 
                current = current.replace('recovery','recovering')
                print wfo.name,"setting the status to",current
                print ', '.join( recovering )
                wfo.status = current
                session.commit()
        else:
            ## this workflow should be handled manually at that point
            print wfo.name,"needs manual intervention"
            wfo.status = 'assistance-manual'
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
    fdb.html()
