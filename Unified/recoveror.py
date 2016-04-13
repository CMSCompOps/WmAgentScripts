#!/usr/bin/env python
from assignSession import *
from utils import workflowInfo, sendEmail, componentInfo, userLock, closeoutInfo, campaignInfo, unifiedConfiguration, siteInfo, componentInfo
import reqMgrClient
import json
import optparse
import copy
from collections import defaultdict
import re
import os
from utils import reqmgr_url

def singleRecovery(url, task , initial, actions, do=False):
    payload = {
        "Requestor" : os.getenv('USER'),
        "Group" : 'DATAOPS',
        "RequestType" : "Resubmission",
        "ACDCServer" : initial['CouchURL'],
        "ACDCDatabase" : "acdcserver",
        "OriginalRequestName" : initial['RequestName']
        }
    copy_over = ['PrepID','RequestPriority', 'TimePerEvent', 'SizePerEvent', 'Group', 'Memory', 'RequestString' ,'CMSSWVersion']        
    for c in copy_over:
        payload[c] = copy.deepcopy(initial[c])

    #a massage ? boost the recovery over the initial wf
    payload['RequestPriority'] *= 10

    if actions:
        for action in actions:
            #if action.startswith('split'):
            #    factor = int(action.split('-')[-1]) if '-' in action else 2
            #    print "Changing time per event (%s) by a factor %d"%( payload['TimePerEvent'], factor)
            #    ## mention it's taking 2 times longer to have a 2 times finer splitting
            #    payload['TimePerEvent'] = factor*payload['TimePerEvent']
            if action.startswith('mem'):
                increase = int(action.split('-')[-1]) if '-' in action else 1000
                ## increase the memory requirement by 1G
                payload['Memory'] += increase
            if action.startswith('split') and initial['RequestType'] in ['MonteCarlo','TaskChain']:
                print "I should not be doing splitting for this type of request",initial['RequestName']
                return None

    if payload['RequestString'].startswith('ACDC'):
        print "This is not allowed yet"
        return None
    payload['RequestString'] = 'ACDC_'+payload['RequestString']
    payload['InitialTaskPath'] = task 

    if not do:
        print json.dumps( payload, indent=2)
        return None

    print json.dumps( payload , indent=2)

    ## submit
    acdc = reqMgrClient.submitWorkflow(url, payload)
    if not acdc:
        print "Error in making ACDC for",initial["RequestName"]
        acdc = reqMgrClient.submitWorkflow(url, payload)
        if not acdc:
            print "Error twice in making ACDC for",initial["RequestName"]
            return None
    
    ## perform modifications
    if actions:
        for action in actions:
            if action.startswith('split'):
                factor = int(action.split('-')[-1]) if '-' in action else 2
                acdcInfo = workflowInfo(url, acdc)
                splittings = acdcInfo.getSplittings()
                for split in splittings:
                    for act in ['avg_events_per_job','events_per_job','lumis_per_job']:
                        if act in split:
                            print "Changing %s (%d) by a factor %d"%( act, split[act], factor),
                            split[act] /= factor
                            print "to",split[act]
                            break
                    split['requestName'] = acdc
                    print "changing the splitting of",acdc
                    print json.dumps( split, indent=2 )
                    print reqMgrClient.setWorkflowSplitting(url, acdc, split )
                
    data = reqMgrClient.setWorkflowApproved(url, acdc)
    print data
    return acdc

    

def recoveror(url,specific,options=None):
    if userLock('recoveror'): return

    up = componentInfo(mcm=False, soft=['mcm'])
    if not up.check(): return

    CI = campaignInfo()
    SI = siteInfo()
    UC = unifiedConfiguration()

    def make_int_keys( d ):
        for code in d:
            d[int(code)] = d.pop(code)

    error_codes_to_recover = UC.get('error_codes_to_recover')
    error_codes_to_block = UC.get('error_codes_to_block')
    error_codes_to_notify = UC.get('error_codes_to_notify')
    make_int_keys( error_codes_to_recover )
    make_int_keys( error_codes_to_block )
    make_int_keys( error_codes_to_notify )

    #wfs = session.query(Workflow).filter(Workflow.status == 'assistance-recovery').all()
    wfs = session.query(Workflow).filter(Workflow.status.contains('recovery')).all()
    if specific:
        wfs.extend( session.query(Workflow).filter(Workflow.status == 'assistance-manual').all() )

    for wfo in wfs:
        if specific and not specific in wfo.name:continue

        if not specific and 'manual' in wfo.status: continue
        
        wfi = workflowInfo(url, wfo.name)

        ## need a way to verify that this is the first round of ACDC, since the second round will have to be on the ACDC themselves

        all_errors = {}
        try:
            wfi.getSummary()
            all_errors = wfi.summary['errors']
        except:
            pass

        print '-'*100        
        print "Looking at",wfo.name,"for recovery options"

        recover = True       

        if not 'MergedLFNBase' in wfi.request:
            print "fucked up"
            sendEmail('missing lfn','%s wl cache is screwed up'%wfo.name)
            recover = False
 
        if not len(all_errors): 
            print "\tno error for",wfo.name
            recover = False

        task_to_recover = defaultdict(list)
        message_to_ops = ""
        message_to_user = ""

        if 'LheInputFilese' in wfi.request and wfi.request['LheInputFiles']:
            ## we do not try to recover pLHE
            recover = False

        if wfi.request['RequestType'] in  ['MonteCarlo','ReReco']:
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
                all_codes.extend( [(int(code),info['jobs'],name,list(set([e['type'] for e in info['errors']])),list(set([e['details'] for e in info['errors']])) ) for code,info in codes.items()] )

            all_codes.sort(key=lambda i:i[1], reverse=True)
            sum_failed = sum([l[1] for l in all_codes])

            for errorCode,njobs,name,types,details in all_codes:
                rate = 100*njobs/float(sum_failed)
                #print ("\t\t %10d (%6s%%) failures with error code %10d (%"+str(max_legend)+"s) at stage %s")%(njobs, "%4.2f"%rate, errorCode, legend, name)
                print ("\t\t %10d (%6s%%) failures with error code %10d (%30s) at stage %s")%(njobs, "%4.2f"%rate, errorCode, ','.join(types), name)
                    
                added_in_recover=False

                #if options.go:
                # force the recovery of any task with error ?

                if errorCode in error_codes_to_recover:
                    ## the error code is registered
                    for case in error_codes_to_recover[errorCode]:
                        match = case['details']
                        matched= (match==None)
                        if not matched:
                            matched=False
                            for detail in details:
                                if match in detail:
                                    print "[recover] Could find keyword",match,"in"
                                    print 50*"#"
                                    print detail
                                    print 50*"#"
                                    matched = True
                                    break
                        if matched and rate > case['rate']:
                            print "\t\t => we should be able to recover that", case['legend']
                            task_to_recover[task].append( (code,case) )
                            added_in_recover=True
                            message_to_user = ""
                        else:
                            print "\t\t recoverable but not frequent enough, needs",case['rate']

                if errorCode in error_codes_to_block:
                    for case in error_codes_to_block[errorCode]:
                        match = case['details']
                        matched= (match==None)
                        if not matched:
                            matched=False
                            for detail in details:
                                if match in detail:
                                    print "[block] Could find keyword",match,"in"
                                    print 50*"#"
                                    print detail
                                    print 50*"#"
                                    matched = True
                                    break
                        if matched and rate > case['rate']:
                            print "\t\t => that error means no ACDC on that workflow", case['legend']
                            if not options.go:
                                message_to_ops += "%s has an error %s blocking an ACDC.\n%s\n "%( wfo.name, errorCode, '#'*50 )
                                recover = False
                                added_in_recover=False

                            
                
                if errorCode in error_codes_to_notify and not added_in_recover:
                    print "\t\t => we should notify people on this"
                    message_to_user += "%s has an error %s in processing.\n%s\n" %( wfo.name, errorCode, '#'*50 )



        if message_to_user:
            print wfo.name,"to be notified to user(DUMMY)",message_to_user

        if message_to_ops:
            sendEmail( "notification in recoveror" , message_to_ops, destination=['jen_a@fnal.gov'])


        if task_to_recover and recover:
            print "Initiating recovery"
            print ', '.join(task_to_recover.keys()),"to be recovered"

            recovering=set()
            for task in task_to_recover:
                print "Will be making a recovery workflow for",task

                ## from here you can fetch known solutions, to known error codes
                actions = list(set([case['solution'] for code,case in task_to_recover[task]  ]))
                acdc = singleRecovery(url, task, wfi.request , actions, do = options.do)

                if not acdc:
                    if options.do:
                        if recovering:
                            print wfo.name,"has been partially ACDCed. Needs manual attention"
                            sendEmail( "failed ACDC partial recovery","%s has had %s/%s recoveries %s only"%( wfo.name, len(recovering), len(task_to_recover), list(recovering)), destination=['jen_a@fnal.gov'])
                            continue
                        else:
                            print wfo.name,"failed recovery once"
                            break
                    else:
                        print "no action to take further"
                        sendEmail("an ACDC that can be done automatically","please check https://cmst2.web.cern.ch/cmst2/unified/logs/recoveror/last.log for details", destination=['jen_a@fnal.gov'])
                        continue
                        
                
                ## and assign it ?
                team = wfi.request['Teams'][0]
                parameters={
                    #'SiteWhitelist' : wfi.request['SiteWhitelist'],
                    'SiteWhitelist' : SI.sites_ready,
                    'AcquisitionEra' : wfi.acquisitionEra(),
                    'ProcessingString' :  wfi.processingString(),
                    'MergedLFNBase' : wfi.request['MergedLFNBase'],
                    'ProcessingVersion' : wfi.request['ProcessingVersion'],
                    }
                ## hackery for ACDC merge assignment
                if wfi.request['RequestType'] == 'TaskChain' and 'Merge' in task.split('/')[-1]:
                    parameters['AcquisitionEra'] = None
                    parameters['ProcessingString'] = None

                if options.ass:
                    print "really doing the assignment of the ACDC",acdc
                    parameters['execute']=True
                    sendEmail("an ACDC was done and WAS assigned", "%s  was assigned, please check https://cmst2.web.cern.ch/cmst2/unified/logs/recoveror/last.log for details"%( acdc ), destination=['jen_a@fnal.gov'])
                else:
                    print "no assignment done with this ACDC",acdc
                    sendEmail("an ACDC was done and need to be assigned", "%s needs to be assigned, please check https://cmst2.web.cern.ch/cmst2/unified/logs/recoveror/last.log for details"%( acdc ), destination=['jen_a@fnal.gov'])

                result = reqMgrClient.assignWorkflow(url, acdc, team, parameters)
                if not result:
                    print acdc,"was not asigned"
                    sendEmail("an ACDC was done and need to be assigned","%s needs to be assigned, please check https://cmst2.web.cern.ch/cmst2/unified/logs/recoveror/last.log for details"%( acdc ), destination=['jen_a@fnal.gov'])
                else:
                    recovering.add( acdc )

            if recovering:
                #if all went well, set the status to -recovering 
                current = wfo.status 
                if options.ass:
                    current = current.replace('recovery','recovering')
                else:
                    current = 'assistance-manual'
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
    url=reqmgr_url
    parser = optparse.OptionParser()
    #parser.add_option('--do',default=False,action='store_true')
    parser.add_option('--test', dest='do', default=True,action='store_false')
    parser.add_option('--leave',dest='ass',default=True,action='store_false')
    parser.add_option('--go',default=False,action='store_true',help="override possible blocking conditions")
    (options,args) = parser.parse_args()
    spec=None
    if len(args)!=0:
        spec = args[0]

    if not options.do: options.ass=False

    recoveror(url,spec,options=options)

    fdb = closeoutInfo()
    fdb.html()
