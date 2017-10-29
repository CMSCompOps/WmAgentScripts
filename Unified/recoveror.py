#!/usr/bin/env python
try:
    from assignSession import *
except:
    print "no DB connection"
    
from utils import workflowInfo, sendEmail, componentInfo, userLock, closeoutInfo, campaignInfo, unifiedConfiguration, siteInfo, componentInfo, sendLog
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
        "OriginalRequestName" : initial['RequestName'],
        "OpenRunningTimeout" : 0
        }
    copy_over = ['PrepID', 'Campaign', 'RequestPriority', 'TimePerEvent', 'SizePerEvent', 'Group', 'Memory', 'RequestString' ,'CMSSWVersion']        
    for c in copy_over:
        if c in initial:
            payload[c] = copy.deepcopy(initial[c])
        else:
            print c,"not in the initial payload"

    #a massage ? boost the recovery over the initial wf
    payload['RequestPriority'] *= 2
    payload['RequestPriority'] = min(500000, payload['RequestPriority'])

    if actions:
        for action in actions:
            #if action.startswith('split'):
            #    factor = int(action.split('-')[-1]) if '-' in action else 2
            #    print "Changing time per event (%s) by a factor %d"%( payload['TimePerEvent'], factor)
            #    ## mention it's taking 2 times longer to have a 2 times finer splitting
            #    payload['TimePerEvent'] = factor*payload['TimePerEvent']
            if action.startswith('mem'):
                arg = action.split('-',1)[-1]
                increase = set_to = None
                tasks,set_to = arg.split(':') if ':' in arg else (None,arg)
                tasks = tasks.split(',') if tasks else []
                if set_to.startswith('+'):
                    increase = int(set_to[1:])
                else:
                    set_to = int(set_to)
                ## increase the memory requirement by 1G

                if 'TaskChain' in initial:
                    mem_dict = {} 
                    it = 1
                    while True:
                        t = 'Task%d'%it
                        it += 1
                        if t in initial:
                            tname = payload.setdefault(t, initial[t])['TaskName']
                            mem = mem_dict.setdefault( tname, payload[t]['Memory'])
                            if tasks and not tname in tasks:
                                print tname,"not concerned"
                                continue
                            if set_to:
                                mem_dict[tname] = set_to
                            else:
                                mem_dict[tname] += increase
                        else:
                            break
                    payload['Memory'] = mem_dict
                else:
                    payload['Memory'] = set_to
                #increase = int(action.split('-')[-1]) if '-' in action else 1000
                ## increase the memory requirement by 1G
                #payload['Memory'] += increase

            if action.startswith('split') and (initial['RequestType'] in ['MonteCarlo'] or (initial['RequestType'] in ['TaskChain'] and not 'InputDataset' in initial['Task1'])):
                print "I should not be doing splitting for this type of request",initial['RequestName']
                return None
            if action.startswith('core'):
                arg = action.split('-',1)[-1]
                tasks,set_to = arg.split(':') if ':' in arg else (None,arg)
                tasks = tasks.split(',') if tasks else []
                set_to = int(set_to)
                if 'TaskChain' in initial:
                    core_dict = {}
                    mem_dict = payload['Memory'] if type(payload['Memory'])==dict else {}
                    it = 1
                    while True:
                        t = 'Task%d'%it
                        it += 1
                        if t in initial:
                            tname = payload.setdefault(t, initial[t])['TaskName']
                            mcore = core_dict.setdefault(tname, payload[t]['Multicore'])
                            mem = mem_dict.setdefault(tname, payload[t]['Memory'])
                            if tasks and not tname in tasks:
                                print tname,"not concerned"
                                continue

                            factor = (set_to / float(mcore))
                            fraction_constant = 0.4 
                            mem_per_core_c = int((1-fraction_constant) * mem / float(mcore))
                            ##scale the memory 
                            mem_dict[tname] += (set_to-mcore)*mem_per_core_c
                            ## scale time/event
                            time_dict[tname] = payload[t]['TimePerEvent'] /factor
                            ## set the number of cores
                            core_dict[tname] = set_to
                        else: 
                            break
                    payload['Multicore'] = core_dict
                    ##payload['TimePerEvent'] = time_dict ## cannot be used yet
                else:
                    payload['Multicore'] = increase

    acdc_round = 0
    initial_string = payload['RequestString']
    if initial_string.startswith('ACDC'):
        if initial_string[4].isdigit():
            acdc_round = int(initial_string[4])
        acdc_round += 1
        #print acdc_round
        #print "This is not allowed yet"
        #return None
    initial_string = initial_string.replace('ACDC_','').replace('ACDC%d_'%(acdc_round-1),'')
    payload['RequestString'] = 'ACDC%d_%s'%(acdc_round,initial_string)
    payload['InitialTaskPath'] = task 

    if not do:
        print json.dumps( payload, indent=2)
        return None

    print "ACDC payload"
    print json.dumps( payload , indent=2)
    print actions

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

def new_recoveror(url, specific, options=None):
    if userLock('recoveror'): return

    up = componentInfo(mcm=False, soft=['mcm'])
    if not up.check(): return

    CI = campaignInfo()
    SI = siteInfo()
    UC = unifiedConfiguration()

    wfs = session.query(Workflow).filter(Workflow.status.contains('recovery')).all()
    if specific:
        wfs.extend( session.query(Workflow).filter(Workflow.status == 'assistance-manual').all() )    

    try:
        from_operator = json.loads(os.popen('curl -s http://vocms0113.cern.ch/actions/test.json').read())
        ## now we have a list of things that we can take action on
    except:
        pass




    for wfo in wfs:
        if specific and not specific in wfo.name:continue

        if not specific and 'manual' in wfo.status: continue
        
        wfi = workflowInfo(url, wfo.name)
    
        send_recovery = False ## will make all acdc
        send_clone = False ## will make a clone
        send_back = False ## should just reject. manual ?
        send_manual = False ## will set in manual

        where_to_run, missing_to_run = wfi.getRecoveryInfo()

        task_to_recover = where_to_run.keys()

        ## if the site at which the recovery could run in drain or out ?
        for task in task_to_recover:
            not_ready = set(where_to_run[task]) - set(SI.sites_ready)
            if not_ready:
                print "the following sites are not ready for the ACDC",",".join( sorted(not_ready) )
                ## do we have a way of telling if a site is going to be out for a long time ?
                # check on priority: high prio, restart
                if wfi.request['RequestPriority'] >= 85000:
                    send_clone = True
                # check on age of the request
                injection_time = time.mktime(time.strptime('.'.join(map(str,wfi.request['RequestDate'])),"%Y.%m.%d.%H.%M.%S")) / (60.*60.)
                now = time.mktime(time.gmtime()) / (60.*60.)
                if float(now - injection_time) <14.:
                    ## less than 14 days, start over
                    send_clone = True
                else:
                    send_manual = True

        
        if not send_recovery:
            ## check on whether the stats is very low
            pass

        if send_recovery:
            ## make acdc for all tasks
            for task in task_to_recover:
                actions = list(set([case['solution'] for code,case in task_to_recover[task]  ]))
                acdc = singleRecovery(url, task, wfi.request , actions, do = True)
        elif send_clone:
            ## this will get it cloned
            wfo.status = 'assistance-clone'
            session.commit()
        elif send_manual:
            wfo.status = 'assistance-manual'


def recoveror(url,specific,options=None):
    if userLock('recoveror'): return

    up = componentInfo(mcm=False, soft=['mcm'])
    if not up.check(): return

    CI = campaignInfo()
    SI = siteInfo()
    UC = unifiedConfiguration()
    use_recoveror = UC.get('use_recoveror')

    if not use_recoveror and not options.go:
        print "We are told not to run recoveror"
        return 

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
            #sendEmail( "notification in recoveror" , message_to_ops, destination=['jen_a@fnal.gov'])
            sendLog('recoveror',message_to_ops,level='warning')

        if len(task_to_recover) != len(all_errors):
            print "Should not be doing partial ACDC. skipping"
            #sendEmail('recoveror','do not want to make partial acdc on %s'%wfo.name)
            sendLog('recoveror','do not want to make partial acdc on %s'%wfo.name, level='warning')
            recover = False

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
                            #sendEmail( "failed ACDC partial recovery","%s has had %s/%s recoveries %s only"%( wfo.name, len(recovering), len(task_to_recover), list(recovering)), destination=['jen_a@fnal.gov'])
                            sendLog('recoveror', "%s has had %s/%s recoveries %s only"%( wfo.name, len(recovering), len(task_to_recover), list(recovering)), level='critical')
                            continue
                        else:
                            print wfo.name,"failed recovery once"
                            #break
                            continue
                    else:
                        print "no action to take further"
                        sendLog('recoveror', "ACDC for %s can be done automatically"% wfo.name, level='critical')
                        continue
                        
                
                ## and assign it ?
                team = wfi.request['Teams'][0]
                #assign_to_sites = set(SI.sites_ready) ## that needs to be massaged to prevent assigning to something out.
                assign_to_sites = set(SI.all_sites)
                parameters={
                    #'SiteWhitelist' : wfi.request['SiteWhitelist'],
                    'SiteWhitelist' : sorted(assign_to_sites),
                    'AcquisitionEra' : wfi.acquisitionEra(),
                    'ProcessingString' :  wfi.processingString(),
                    'MergedLFNBase' : wfi.request['MergedLFNBase'],
                    'ProcessingVersion' : wfi.request['ProcessingVersion'],
                    }
                ## hackery for ACDC merge assignment
                if wfi.request['RequestType'] == 'TaskChain' and 'Merge' in task.split('/')[-1]:
                    parameters['AcquisitionEra'] = None
                    parameters['ProcessingString'] = None

                ## xrootd setttings on primary and secondary
                if 'TrustSitelists' in wfi.request and wfi.request['TrustSitelists']:
                    parameters['TrustSitelists'] = True
                if 'TrustPUSitelists' in wfi.request and wfi.request['TrustPUSitelists']:
                    parameters['TrustPUSitelists'] = True

                if options.ass:
                    print "really doing the assignment of the ACDC",acdc
                    parameters['execute']=True
                    wfi.sendLog('recoveror',"%s  was assigned for recovery"% acdc)
                else:
                    print "no assignment done with this ACDC",acdc
                    sendLog('recoveror',"%s needs to be assigned"%(acdc), level='critical')


                result = reqMgrClient.assignWorkflow(url, acdc, team, parameters)
                if not result:
                    print acdc,"was not asigned"
                    sendLog('recoveror',"%s needs to be assigned"%(acdc), level='critical')
                else:
                    recovering.add( acdc )

            current = None
            if recovering:
                #if all went well, set the status to -recovering 
                current = wfo.status 
                if options.ass:
                    current = current.replace('recovery','recovering')
                else:
                    current = 'assistance-manual'
                print 'created ACDC: '+', '.join( recovering )
            else:
                ## was set to be recovered, and no acdc was made
                current = 'assistance-manual'

            if current:
                print wfo.name,"setting the status to",current
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
    parser.add_option('--test', dest='do', default=True,action='store_false')
    parser.add_option('--leave',dest='ass',default=True,action='store_false')
    parser.add_option('--go',default=False,action='store_true',help="override possible blocking conditions")
    parser.add_option('--new',default=False,action='store_true')
    (options,args) = parser.parse_args()
    spec=None
    if len(args)!=0:
        spec = args[0]

    if not options.do: options.ass=False

    if options.new:
        new_recoveror(url,spec,options=options)
    else:
        recoveror(url,spec,options=options)        

    fdb = closeoutInfo()
    fdb.html()

    #from showError import parse_all
    #parse_all(url)
