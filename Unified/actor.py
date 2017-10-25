#!/usr/bin/env python
from utils import workflowInfo, sendEmail, componentInfo, campaignInfo, unifiedConfiguration, siteInfo, sendLog, setDatasetStatus
from utils import closeoutInfo, userLock
import reqMgrClient
import json
import optparse
import copy
from collections import defaultdict
import re
import os
from utils import reqmgr_url
import httplib
import ssl

def remove_action(*args):
    if not os.path.exists('key.json'):
        print 'Needs to be called from same directory as key.json'
        exit()

    with open('key.json', 'r') as key_file:
        key_info = json.load(key_file)

    conn = httplib.HTTPSConnection(key_info['url'], key_info['port'],
                                   context=ssl._create_unverified_context())

    conn.request(
        'POST', key_info['path'],
        json.dumps({'key': key_info['key'], 'workflows': args}),
        {'Content-type': 'application/json'})

    print conn.getresponse().read()
    conn.close()
  

def singleRecovery(url, task, initial, actions, do=False):
    print "Inside single recovery!"
    payload = {
        "Requestor" : os.getenv('USER'),
        "Group" : 'DATAOPS',
        "RequestType" : "Resubmission",
        "ACDCServer" : initial['CouchURL'],
        "ACDCDatabase" : "acdcserver",
        "OriginalRequestName" : initial['RequestName'],
        "OpenRunningTimeout" : 0
    }
    copy_over = ['PrepID','Campaign','RequestPriority', 'TimePerEvent', 'SizePerEvent', 'Group', 'Memory', 'RequestString' ,'CMSSWVersion']
    for c in copy_over:
        if c in initial:
            payload[c] = copy.deepcopy(initial[c])
        else:
            print c,"not in the initial payload"

    #a massage ? boost the recovery over the initial wf
#    payload['RequestPriority'] *= 10
    #Max priority is 1M
    payload['RequestPriority'] = min(500000,  payload['RequestPriority']*2 ) ## never above 500k

    #change parameters based on actions here
    if actions:
        for action in actions:
            if action.startswith('mem') and actions[action] != "" and actions[action] != 'Same':
                payload['Memory'] = actions[action]
                print "Memory set to " + actions[action]
                ## Taskchains needs to be treated special to set the memory to all tasks
                if 'TaskChain' in initial:
                    it = 1
                    while True:
                        t = 'Task%d'%it
                        it += 1
                        if t in initial:
                            payload[t] = copy.deepcopy(initial[t])
                            payload[t]['Memory'] = actions[action]
                        else:
                            break

            if action.startswith('split'):
                split_alert = (initial['RequestType'] in ['MonteCarlo'] )
                for key in initial:
                    if key == 'SplittingAlgo' and (initial[key] in ['EventBased']):
                        split_alert = True
                    elif key.startswith('Task') and key != 'TaskChain':
                        for key2 in initial[key]:
                            if key2 == 'TaskName':
                                print "task",task.split('/')[-1]
                                print "TaskName",initial[key][key2]
                                if (initial[key][key2] == task) and (initial[key][key2] in ['EventBased']):
                                    split_alert = True
                if split_alert:
                    sendLog('actor','Cannot change splitting for %s'%initial['RequestName'],level='warning')
                    print "I should not be doing splitting for this type of request",initial['RequestName']
                    return None

    acdc_round = 0
    initial_string = payload['RequestString']
    if initial_string.startswith('ACDC'):
        if initial_string[4].isdigit():
            acdc_round = int(initial_string[4])
        acdc_round += 1

    initial_string = initial_string.replace('ACDC_','').replace('ACDC%d_'%(acdc_round-1),'')
    payload['RequestString'] = 'ACDC%d_%s'%(acdc_round,initial_string)
    payload['InitialTaskPath'] = task

    if not do:
        print json.dumps( payload, indent=2)
        return None

    print "ACDC payload"
#    print json.dumps( payload , indent=2)
    print actions

    ## submit here
    acdc = reqMgrClient.submitWorkflow(url, payload)
    if not acdc:
        print "Error in making ACDC for",initial["RequestName"]
        acdc = reqMgrClient.submitWorkflow(url, payload)
        if not acdc:
            print "Error twice in making ACDC for",initial["RequestName"]
            sendLog('actor','Failed twice in making ACDCs for %s!'%initial['RequestName'],level='critical')                
            return None

    ## change splitting if requested
    if actions:
        for action in actions:
            if action.startswith('split'):
                acdcInfo = workflowInfo(url, acdc)
                splittings = acdcInfo.getSplittings()
                if actions[action] != 'Same' and actions[action] != 'max':
                    factor = int(actions[action][0:-1]) if 'x' in actions[action] else 2
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
                elif 'max' in actions[action]:
                    for split in splittings:
                        for act in ['avg_events_per_job','events_per_job','lumis_per_job']:
                            if act in split:
                                print "Changing %s (%d) "%( act, split[act]),
                                split[act] = 1
                                print "to max splitting ",split[act]
                                break
                        split['requestName'] = acdc
                        print "changing the splitting of",acdc
                        print json.dumps( split, indent=2 )
                        print reqMgrClient.setWorkflowSplitting(url, acdc, split )

    data = reqMgrClient.setWorkflowApproved(url, acdc)
    

    print data
    return acdc


def singleClone(url, wfname, actions, comment, do=False):
    
    wfi = workflowInfo(url, wfname)
    payload = wfi.getSchema()
    initial = wfi.request

    payload['Requestor']           = os.getenv('USER')
    payload['Group']               = 'DATAOPS'
    payload['OriginalRequestName'] = initial['RequestName']
    payload['RequestPriority'] = initial['RequestPriority']

    if 'ProcessingVersion' in initial:
        payload['ProcessingVersion'] = int(initial['ProcessingVersion']) +1
    else:
        payload['ProcessingVersion'] = 2


## drop parameters on the way to reqmgr2
    paramBlacklist = ['BlockCloseMaxEvents', 'BlockCloseMaxFiles', 'BlockCloseMaxSize', 'BlockCloseMaxWaitTime',
                  'CouchWorkloadDBName', 'CustodialGroup', 'CustodialSubType', 'Dashboard',
                  'GracePeriod', 'HardTimeout', 'InitialPriority', 'inputMode', 'MaxMergeEvents', 'MaxMergeSize',
                  'MaxRSS', 'MaxVSize', 'MinMergeSize', 'NonCustodialGroup', 'NonCustodialSubType',
                  'OutputDatasets', 'ReqMgr2Only', 'RequestDate' 'RequestorDN', 'RequestName', 'RequestStatus',
                  'RequestTransition', 'RequestWorkflow', 'SiteWhitelist', 'SoftTimeout', 'SoftwareVersions',
                  'SubscriptionPriority', 'Team', 'timeStamp', 'TrustSitelists', 'TrustPUSitelists',
                  'TotalEstimatedJobs', 'TotalInputEvents', 'TotalInputLumis', 'TotalInputFiles','checkbox',
                  'DN', 'AutoApproveSubscriptionSites', 'NonCustodialSites', 'CustodialSites', 'OriginalRequestName', 'Teams', 'OutputModulesLFNBases', 
                  'SiteBlacklist', 'AllowOpportunistic', '_id']
    for p in paramBlacklist:
        if p in payload:
            payload.pop( p )
            pass

    if actions:
        for action in actions:
            if action.startswith('mem') and actions[action] != "" and actions[action] != 'Same':
                if 'TaskChain' in payload:
                    print "Setting memory for clone of task chain"
                    it=1
                    while True:
                        t = 'Task%d'%it
                        it+=1
                        if t in payload:
                            payload[t]['Memory'] = actions[action]
                            print "Memory set for Task%d"%it
                        else:
                            break
                else:
                    print "Setting memory for non-taskchain workflow"
                    payload['Memory'] = actions[action]
                print "Memory set to " + actions[action]
                #This line is doesn't work for some reason
#                wfi.sendLog('actor','Memory of clone set to %d'%actions[action])

    print "Clone payload"
#    print json.dumps( payload , indent=2)
    print actions

    #Create clone
    clone = reqMgrClient.submitWorkflow(url, payload)
    if not clone:
        print "Error in making clone for",initial["RequestName"]
        clone = reqMgrClient.submitWorkflow(url, payload)
        if not clone:
            print "Error twice in making clone for",initial["RequestName"]
            sendLog('actor','Failed to make a clone twice for %s!'%initial["RequestName"],level='critical')
            wfi.sendLog('actor','Failed to make a clone twice for %s!'%initial["RequestName"])
            return None

    if actions:
        for action in actions:
            if action.startswith('split'):
                cloneinfo = workflowInfo(url, clone)
                splittings = cloneinfo.getSplittings()
                if actions[action] != 'Same' and actions[action] != 'max' and actions[action] != '':
                    factor = int(actions[action][0:-1]) if 'x' in actions[action] else 2
                    for split in splittings:
                        for act in ['avg_events_per_job','events_per_job','lumis_per_job']:
                            if act in split:
                                wfi.sendLog('actor','Changing %s (%d) by a factor %d'%( act, split[act], factor))
                                print "Changing %s (%d) by a factor %d"%( act, split[act], factor),
                                split[act] /= factor
                                print "to",split[act]
                                break
                        split['requestName'] = clone
                        print "changing the splitting of",clone
                        print json.dumps( split, indent=2 )
                        print reqMgrClient.setWorkflowSplitting(url, clone, split )
                elif 'max' in actions[action]:
                    for split in splittings:
                        for act in ['avg_events_per_job','events_per_job','lumis_per_job']:
                            if act in split:
                                wfi.sendLog('actor','Max splitting set for %s (%d'%( act, split[act]))
                                print "Changing %s (%d) "%( act, split[act]),
                                split[act] = 1
                                print "to max splitting ",split[act]
                                break
                        split['requestName'] = clone
                        print "changing the splitting of",clone
                        print json.dumps( split, indent=2 )
                        print reqMgrClient.setWorkflowSplitting(url, clone, split )

    #Approve
    data = reqMgrClient.setWorkflowApproved(url, clone)
    wfi.sendLog('actor','Cloned into %s'%clone)

#    wfi.sendLog('actor','Cloned into %s by unified operator %s'%( clone, comment ))
#    wfi.notifyRequestor('Cloned into %s by unified operator %s'%( clone, comment ),do_batch=False)

    print data
    return clone



def actor(url,options=None):
    
    if userLock('actor'): return
    
    up = componentInfo(mcm=False, soft=['mcm'])
    if not up.check(): return
    
   # CI = campaignInfo()
    SI = siteInfo()
    UC = unifiedConfiguration()
    
    # Need to look at the actions page https://vocms0113.cern.ch:80/getaction (can add ?days=20) and perform any actions listed
    try:
        action_list = json.loads(os.popen('curl -s -k https://vocms0113.cern.ch:80/getaction?days=15').read())
        ## now we have a list of things that we can take action on
    except:
        print "Not able to load action list :("
        sendLog('actor','Not able to load action list', level='critical')
        return

    print action_list
    if not action_list:
        print "EMPTY!"
        return

    for wfname in action_list:
        print '-'*100
        print "Looking at",wfname,"for recovery options"

        to_clone = False
        to_acdc = False
        for key in action_list[wfname]:
            if key == 'Parameters':
                tasks =  action_list[wfname][key]
            elif key == 'Action' and action_list[wfname][key] == 'acdc':
                print "Going to create ACDCs for ", wfname
                to_acdc = True
            elif key == 'Action' and action_list[wfname][key] == 'clone':
                print "Going to clone ", wfname
                to_clone = True

        if not to_acdc and not to_clone:
            sendLog('actor','Action submitted for something other than acdc and clone for workflow %s'%wfname,level='critical')
            print "Can only do acdcs and clones! Skipping workflow ",wfname
            continue
        if not tasks:
            sendLog('actor','Empty action submitted for workflow %s'%wfname,level='critical')
            print "Moving on. Parameters is blank for " + wfname
            continue

        wfi = workflowInfo(url, wfname)

        recover = True
        message_to_ops = ""
        message_to_user = ""

#===========================================================
        if to_clone and options.do:
            print "Let's try kill and clone: "
            wfi.sendLog('actor','Going to clone %s'%wfname)
            results=[]
            datasets = set(wfi.request['OutputDatasets'])

            comment=""

            if 'comment' in tasks: comment = ", reason: "+ tasks['comment']
            wfi.sendLog('actor',"invalidating the workflow by traffic controller %s"%comment)

            #Reject all workflows in the family
            #first reject the original workflow.
            reqMgrClient.invalidateWorkflow(url, wfi.request['RequestName'], current_status=wfi.request['RequestStatus'], cascade=False)
            #Then reject any ACDCs associated with that workflow
            if 'ACDCs' in action_list[wfname]:
                children = action_list[wfname]['ACDCs']
                for child in children:
                    wfi.sendLog('actor',"rejecting %s"%child)
                    wfi_acdc = workflowInfo(url, child)
                    reqMgrClient.invalidateWorkflow(url, wfi_acdc.request['RequestName'], current_status=wfi_acdc.request['RequestStatus'], cascade=False)
                    datasets.update( wfi_acdc.request['OutputDatasets'] )
            #Invalidate all associated output datasets
            for dataset in datasets:
                results.append( setDatasetStatus(dataset, 'INVALID') )

            if all(map(lambda result : result in ['None',None,True],results)):
                wfi.sendLog('actor',"%s and children are rejected"%wfname)

            cloned = None
            try:    
                cloned =  singleClone(url, wfname, tasks, comment, options.do)
            except:
                sendLog('actor','Failed to create clone for %s! Check logs for more information. Action will need to be resubmitted.'%wfname,level='critical')
                wfi.sendLog('actor','Failed to create clone for %s!'%wfname)
                remove_action(wfname)
            if not cloned:
                recover = False
                wfi.sendLog('actor','Failed to create clone for %s!'%wfname)
                sendLog('actor','Failed to create clone for %s!'%wfname,level='critical')

            else:
                wfi.sendLog('actor',"Workflow %s cloned"%wfname)


#===========================================================
        elif to_acdc:
            if 'AllSteps' in tasks:
                allTasksDefaults = tasks['AllSteps']
                tasks.pop('AllSteps')
                for setting in allTasksDefaults:
                    for task in tasks:
                        if setting in tasks[task]:
                            tasks[task][setting] = allTasksDefaults[setting]
                        else:
                                tasks[task].append({setting:allTasksDefaults[setting]})
            print "Tasks is "
            print tasks

            all_tasks = wfi.getAllTasks()

            ## need a way to verify that this is the first round of ACDC, since the second round will have to be on the ACDC themselves
        
            try:
                WMErr = wfi.getWMErrors()
#               print WMErr
            except:
                sendLog('actor','Cannot create ACDCS for %s because WMErr cannot be reached.'%wfname,level='critical')
                continue
            if not WMErr:
                sendLog('actor','Cannot create ACDCS for %s because WMErr is blank.'%wfname,level='critical')
                print "Moving on. WMErr is blank"
                continue

            try:
                where_to_run, missing_to_run,missing_to_run_at =  wfi.getRecoveryInfo()
                print "Where to run = "
                print where_to_run
            except:
                sendLog('actor','Cannot create ACDCS for %s because recovery info cannot be found.'%wfname,level='critical')
                print "Moving on. Cannot access recovery info for " + wfname
                continue
            if not where_to_run:
                sendLog('actor','Cannot create ACDCS for %s because site list cannot be found.'%wfname,level='critical')
                print "Moving on. where to run is blank"
                continue

            message_to_ops = ""
            message_to_user = ""
        
            num_tasks_to_recover = 0
        
            for task in WMErr:
                if 'LogCollect' in task: continue
                if 'Cleanup' in task: continue
                if not 'jobfailed' in WMErr[task]:
                    continue
                else:
                    num_tasks_to_recover += 1
#                print "Task to recover: " + task

            if not num_tasks_to_recover:
                print "\tno error for",wfname
#            recover = False
        
            if 'LheInputFiles' in wfi.request and wfi.request['LheInputFiles']:
            ## we do not try to recover pLHE
                sendLog('actor','Cannot create ACDCS for %s because it is a pLHE workflow.'%wfname,level='critical')
                print "We don't try to recover pLHE. Moving on."
                recover = False
        #            sendEmail('cannot submit action', '%s is a pLHE workflow. We do not try to recover pLHE'%wfname)


#        if wfi.request['RequestType'] in ['ReReco']:
#            recover= False
#            print 'cannot submit action. ReReco'
        #   sendEmail('cannot submit action', '%s is request type ReReco'%wfname)

            recovering = set()
            for task in tasks:
                assign_to_sites = set()
                print "Task names is " + task
                fulltaskname = '/' + wfname + '/' + task
#                print "Full task name is " + fulltaskname
                wrong_task = False
                for task_info in all_tasks:
                    if fulltaskname == task_info.pathName:
                        if task_info.taskType not in ['Processing','Production','Merge']:
                            wrong_task=True
                            wfi.sendLog('actor', "Skipping task %s because the taskType is %s. Can only ACDC Processing, Production, or Merge tasks"%( fulltaskname, task_info.taskType))
                if wrong_task:
                    continue
                print tasks[task]
                actions = tasks[task]
                for action in actions:
                    if action.startswith('sites'):
                        if type(actions[action]) != list:
                            assign_to_sites=[SI.SE_to_CE(actions[action])]
                        else:
                            assign_to_sites=list(set([SI.SE_to_CE(site) for site in actions[action]]))
#                    if action.startswith('mem') and actions[action] != "" and actions[action] != 'Same' and wfi.request['RequestType'] in ['TaskChain']:
#                        recover = False;
#                        print  "Skipping %s for now until Allie fixes memory parameter for TaskChain ACDCs."%wfname
#                        wfi.sendLog('actor',"Skipping %s for now until Allie fixes memory parameter for TaskChain ACDCs."%wfname)
                if not 'sites' in actions:
                    assign_to_sites = list(set([SI.SE_to_CE(site) for site in where_to_run[task]]))
                    print "Found",sorted(assign_to_sites),"as sites where to run the ACDC at, from the acdc doc of ",wfname
                print "Going to run at",sorted(assign_to_sites)
                if recover:
                    print "Initiating recovery"
                    acdc = singleRecovery(url, fulltaskname, wfi.request, actions, do = options.do)
                    if not acdc:
                        if options.do:
                            if recovering:
                                print wfname + " has been partially ACDC'ed. Needs manual attention."
                                sendLog('actor', "%s has had %s/%s recoveries %s only"%( wfname, len(recovering), num_tasks_to_recover, list(recovering)), level='critical')
                                wfi.sendLog('actor', "%s has had %s/%s recoveries %s only"%( wfname, len(recovering), num_tasks_to_recover, list(recovering)))
                                break
                            else:
                                print wfname + " failed recovery once"
                                recover = False
                                break
                        else:
                            print "no action to take further"
#                        sendLog('recoveror', "ACDC for %s can be done automatically"% wfname, level='critical')
                            continue

                    else: #ACDC was made correctly. Now we have to assign it.
                        wfi.sendLog('actor','ACDC created for task %s. Actions taken \n%s'%(fulltaskname,list(actions)))
                        team = wfi.request['Teams'][0]
                        parameters={
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
                        if 'xrootd' in actions:
                            if actions['xrootd'] == 'enabled':
                                print "Going to assign via xrootd"
                                parameters['TrustSitelists'] = True
                            elif actions['xrootd'] == 'disabled':
                                parameters['TrustSitelists'] = False
                        elif ('TrustSitelists' in wfi.request and wfi.request['TrustSitelists']=='true'):
                            parameters['TrustSitelists'] = True
                        else:
                            parameters['TrustSitelists'] = False

                        if 'TrustPUSitelists' in wfi.request and wfi.request['TrustPUSitelists']:
                            parameters['TrustPUSitelists'] = True

                        if options.ass:
                            print "really doing the assignment of the ACDC",acdc
                            parameters['execute']=True
                            wfi.sendLog('actor',"%s  was assigned for recovery"% acdc)
                        else:
                            print "no assignment done with this ACDC",acdc
                            sendLog('actor',"%s needs to be assigned"%(acdc), level='critical')
                            continue
 #                       print parameters
                        result = reqMgrClient.assignWorkflow(url, acdc, team, parameters)
                        if not result:
                            print acdc,"was not assigned"
                            sendLog('actor',"%s needs to be assigned"%(acdc), level='critical')
                        else:
                            recovering.add( acdc )
                        wfi.sendLog('actor',"ACDCs created for %s"%wfname)
        #===========================================================
        
        
        if recover and options.do:
            remove_action(wfname)

        if message_to_user:
            print wfname,"to be notified to user(DUMMY)",message_to_user

        if message_to_ops:
            print 'message'
            #sendEmail( "notification in recoveror" , message_to_ops, destination=['jen_a@fnal.gov'])
        #            sendLog('recoveror',message_to_ops,level='warning')



    return



if __name__ == '__main__':
    url=reqmgr_url
    parser = optparse.OptionParser()
    parser.add_option('--test', dest='do', default=True,action='store_false')
    parser.add_option('--leave',dest='ass',default=True,action='store_false')
    parser.add_option('--go',default=False,action='store_true',help="override possible blocking conditions")
    (options,args) = parser.parse_args()
        
    if len(args)!=0:
        print "No arguments accepted."
    else:
        if not options.do: options.ass=False
        actor(url,options=options)

#    fdb = closeoutInfo()
#    fdb.html()

#    from showError import parse_all
 #   parse_all(url)

