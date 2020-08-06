#!/usr/bin/env python
from assignSession import *
from utils import workflowInfo, sendEmail, componentInfo, campaignInfo, unifiedConfiguration, siteInfo, sendLog, setDatasetStatus, moduleLock, invalidate, wtcInfo
from utils import closeoutInfo, userLock, base_eos_dir, getFailedJobs
import reqMgrClient
import wtcClient
import json
import optparse
import copy
from collections import defaultdict
import re
import os
from utils import reqmgr_url
import httplib
import ssl
import sys
import random
from wtcClient import wtcClient
from JIRAClient import JIRAClient


def singleRecovery(url, task, initial, actions, do=False, priority_change=False):
    print "Inside single recovery!"
    payload = {
        "Requestor" : os.getenv('USER'),
        "Group" : 'DATAOPS',
        "RequestType" : "Resubmission",
        "ACDCServer" : initial['ConfigCacheUrl'],
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

    original_priority = payload['RequestPriority']
    payload['RequestPriority'] = min(500000,  payload['RequestPriority']*2 ) ## never above 500k

    if priority_change:

        failjobs = getFailedJobs(task)
        if failjobs and failjobs>500:
            payload['RequestPriority'] = min(500000,  original_priority*1.2 )

    #change parameters based on actions here
    if actions:
        for action in actions:
            if action.startswith('mem') and actions[action] != "" and actions[action] != 'Same':
                #if multicore parameter is also used, need to scale memory by the new number of cores
                if 'multicore' in actions and actions['multicore'] != "":
                    continue
                ## Taskchains needs to be treated special to set the memory to all tasks
		try: 
                    set_to = int(actions[action])
		except ValueError:
		    if actions[action].lower().endswith('gb'):
			set_to = int(actions[action][:-2])*1024
		    elif actions[action].lower().endswith('kb'):
			set_to = int(actions[action][:-2])
		    else:
			# invalid parameter -- making no change
			continue
                if 'TaskChain' in initial:
                    mem_dict = {}
                    it = 1
                    while True:
                        t = 'Task%d'%it
                        it += 1
                        if t in initial:
                            tname = payload.setdefault(t, initial[t])['TaskName']
                            mem = mem_dict.setdefault( tname, payload[t]['Memory'])
                            mem_dict[tname] = set_to
                        else:
                            break
                    payload['Memory'] = mem_dict
                    print "Memory set to: ",json.dumps( mem_dict, indent=2)
                else: 
                    payload['Memory'] = set_to
                    print "Memory set to: ", set_to


            if action.startswith('multicore') and actions[action] != "":
                set_to = int(actions[action] )
                ## Taskchains needs to be treated special to set the multicore and memory values to all tasks
                if 'TaskChain' in initial:
                    mem_dict  = payload['Memory'] if type(payload['Memory'])==dict else {}
                    core_dict = {}
                    it = 1
                    while True:
                        t = 'Task%d'%it
                        it += 1
                        if t in initial:
                            tname = payload.setdefault(t, initial[t])['TaskName']
                            mem = mem_dict.setdefault( tname, payload[t]['Memory'])

                            #Need to scale the memory by the new number of cores
                            initial_cores = payload[t].setdefault('Multicore', 1) 

                            if 'memory' in actions and actions['memory'] != "" and actions['memory'] != 'Same':
                                mem = actions['memory']

                            fraction_constant = 0.4
                            mem_per_core_c = int (( 1 - fraction_constant) * mem / float(initial_cores) )
                            
                            mem_dict[tname] = int ( mem + (set_to - initial_cores)*mem_per_core_c )
                            core_dict[tname] = set_to

                            print "For ", t
                            print "Multicore set to ", set_to
                            print "Memory set to ", mem_dict[tname]
                        else:
                           break
                    payload['Memory']    = mem_dict
                    payload['Multicore'] = core_dict
                                        
                else:
                    #Need to scale the memory by the new number of cores
                    initial_cores = initial.setdefault('Multicore', 1) 

                    mem = payload['Memory']
                    if 'memory' in actions and actions['memory'] != "" and actions['memory'] != 'Same' :
                        mem = actions['memory']

                    fraction_constant = 0.4
                    mem_per_core_c = int (( 1 - fraction_constant) * mem / float(initial_cores) )

                    payload['Multicore'] = set_to
                    payload['Memory'] = int ( mem + (set_to - initial_cores)*mem_per_core_c )

                    print "Multicore set to ", set_to
                    print "Memory set to ", payload['Memory']


            if action.startswith('split'):
                
                split_alert = (initial['RequestType'] in ['MonteCarlo'] )
                for key in initial:
                    if key == 'SplittingAlgo' and (initial[key] in ['EventBased']):
                        split_alert = True
                    elif key.startswith('Task') and key != 'TaskChain':
                        for key2 in initial[key]:
                            if key2 == 'TaskName':
                                this_taskname = initial[key][key2]
                                recover_task = task.split('/')[-1]
                                print "For recovery of task",recover_task
                                print "Looking at task",this_taskname
                                if (recover_task == this_taskname) and (initial[key]['SplittingAlgo'] in ['EventBased']):
                                    ## the task to be recovered is actually of the wrong type to allow change of splitting
                                    sendLog('actor','To recover on %s, changing the splitting on %s is not really allowed and this will be ignored instead of failing acdc.'%( task, initial[key]['SplittingAlgo']), level='critical')
                                    ## do not send an alert and stop the acdc
                                    #split_alert = True

                if split_alert:
                    sendLog('actor','Cannot change splitting for %s'%initial['RequestName'],level='critical')
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
                splittings = acdcInfo.getSplittingsNew(strip=True)
                if actions[action] != 'Same' and actions[action] != 'max':
                    factor = int(actions[action][0:-1]) if 'x' in actions[action] else 2
                    for split in splittings:
                        split_par = split['splitParams']
                        if split['splitAlgo'] in ['EventBased']:
                            sendLog('actor',"Changing the splitting on %s for %s is not permitted. Not changing."%(split['splitAlgo'],initial["RequestName"]), level='critical')
                            continue
                        
                        for act in ['avg_events_per_job','events_per_job','lumis_per_job']:
                            if act in split_par:
                                print "Changing %s (%d) by a factor %d"%( act, split_par[act], factor),
                                split_par[act] /= factor
                                split_par[act] = max(split_par[act],1)
                                print "to",split_par[act]
                                break
                        #split['requestName'] = acdc
                        #print "changing the splitting of",acdc
                        #print json.dumps( split, indent=2 )
                        #print reqMgrClient.setWorkflowSplitting(url, acdc, split )

 #consider the splitting
                    if priority_change and failjobs and failjobs<500 and failjobs*factor > 500:
                        print "splitting causes jobs passing threshold"
                        new_priority = min(500000,  original_priority*1.2 )
                        split_change = reqMgrClient.changePriorityWorkflow(url, acdc, new_priority)

                elif 'max' in actions[action]:
                    for split in splittings:
                        split_par = split['splitParams']
                        for act in ['avg_events_per_job','events_per_job','lumis_per_job']:
                            if act in split_par:
                                print "Changing %s (%d) "%( act, split_par[act]),
                                split_par[act] = 1
                                print "to max splitting ",split_par[act]
                                break
                    if priority_change:
                        new_priority = min(500000,  original_priority*1.2 )
                        split_change = reqMgrClient.changePriorityWorkflow(url, acdc, new_priority)    


                print "changing the splitting of",acdc
                print json.dumps( splittings, indent=2 )                
                done = reqMgrClient.setWorkflowSplitting(url, acdc, splittings )
                ## check on done == True
                

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

        
    payload = reqMgrClient.purgeClonedSchema( payload )

    if actions:
        for action in actions:
            if action.startswith('mem') and actions[action] != "" and actions[action] != 'Same':
                if 'TaskChain' in payload:
                    print "Setting memory for clone of task chain"
                    mem_dict = {}
                    it=1
                    while True:
                        t = 'Task%d'%it
                        it+=1
                        if t in payload:
                            tname = payload[t]['TaskName']
                            mem_dict[tname] = int(actions[action])
                            print "Memory set for Task%d"%it
                        else:
                            break
                    payload['Memory'] = mem_dict
                else:
                    print "Setting memory for non-taskchain workflow"
                    payload['Memory'] = int(actions[action])
                print "Memory set to " + actions[action]

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
                splittings = cloneinfo.getSplittingsNew(strip=True)
                if actions[action] != 'Same' and actions[action] != 'max' and actions[action] != '':
                    factor = int(actions[action][0:-1]) if 'x' in actions[action] else 2
                    for split in splittings:
                        split_par = split['splitParams']
                        for act in ['avg_events_per_job','events_per_job','lumis_per_job']:
                            if act in split_par:
                                wfi.sendLog('actor','Changing %s (%d) by a factor %d'%( act, split_par[act], factor))
                                split_par[act] /= factor
                                print "to",split_par[act]
                                break
                        #split['requestName'] = clone
                        #print "changing the splitting of",clone
                        #print json.dumps( split, indent=2 )
                        #print reqMgrClient.setWorkflowSplitting(url, clone, split )
                elif 'max' in actions[action]:
                    for split in splittings:
                        split_par = split['splitParams']
                        for act in ['avg_events_per_job','events_per_job','lumis_per_job']:
                            if act in split_par:
                                wfi.sendLog('actor','Max splitting set for %s (%d'%( act, split_par[act]))
                                print "Changing %s (%d) "%( act, split_par[act]),
                                split_par[act] = 1
                                print "to max splitting ",split_par[act]
                                break
                        #split['requestName'] = clone
                        #print "changing the splitting of",clone
                        #print json.dumps( split, indent=2 )
                        #print reqMgrClient.setWorkflowSplitting(url, clone, split )

                print "changing the splitting of",clone
                print json.dumps( splittings, indent=2 )
                print reqMgrClient.setWorkflowSplitting(url, clone, splittings )
    #Approve
    data = reqMgrClient.setWorkflowApproved(url, clone)
    #wfi.sendLog('actor','Cloned into %s'%clone)

    
#    wfi.sendLog('actor','Cloned into %s by unified operator %s'%( clone, comment ))
#    wfi.notifyRequestor('Cloned into %s by unified operator %s'%( clone, comment ),do_batch=False)

    print data
    return clone



def actor(url,options=None):
    
    mlock = moduleLock(wait=False ,silent=True)
    if mlock(): return
    if userLock('actor'): return
    
    up = componentInfo(soft=['mcm'])
    if not up.check(): return
    
   # CI = campaignInfo()
    SI = siteInfo()
    UC = unifiedConfiguration()
    WC = wtcClient()
    WI = wtcInfo()
    JC = JIRAClient()

    action_list = WC.get_actions()
    if action_list is None:
        print "Not able to load action list"
        sendLog('actor','Not able to load action list', level='critical')
        return

    if options.actions:
        action_list = json.loads(open(options.actions).read())

    print json.dumps( action_list, indent=2)
    if not action_list:
        print "EMPTY!"
        return

    wf_list = action_list.keys()
    print json.dumps( sorted( wf_list), indent=2)
    if options.spec:
        wf_list = [wf for wf in wf_list if options.spec in wf]

    max_per_round = UC.get('max_per_round').get('actor', None)
    if max_per_round:
        random.shuffle( wf_list )
        wf_list = wf_list[:max_per_round]

    for wfname in wf_list:
        print '-'*100
        print "Looking at",wfname,"for recovery options"

        to_clone = False
        to_acdc = False
        to_force = False
        to_hold = False
        something_to_do = False
        tasks = action_list[wfname].get( 'Parameters' , None)
        to_acdc = action_list[wfname].get( 'Action', None) == 'acdc'
        to_clone = action_list[wfname].get( 'Action', None) == 'clone'
        to_force = action_list[wfname].get( 'Action', None) == 'special' and action_list[wfname].get( 'Parameters' ,{}).get('action',None) in ['by-pass', 'bypass']
        to_hold = action_list[wfname].get( 'Action', None) == 'special' and action_list[wfname].get( 'Parameters' ,{}).get('action',None) in ['onhold','on-hold']

        if not to_acdc and not to_clone and not to_force and not to_hold:
            sendLog('actor','Action submitted for something other than acdc, clone, bypass or hold for workflow %s'%wfname,level='critical')
            print json.dumps( action_list[wfname] , indent=2)
            continue

        if not tasks and to_acdc:
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
            comment=""
            if 'comment' in tasks: comment = ", reason: "+ tasks['comment']
            wfi.sendLog('actor',"invalidating the workflow by traffic controller %s"%comment)

            #Reject all workflows in the family
            inv_results = invalidate(url, wfi, only_resub=False, with_output=True)
            all_good = all(inv_results)
            if all_good:
                wfi.sendLog('actor',"%s and children are rejected"%wfname)
            else:
                wfi.sendLog('actor',"Failed to reject the request and dependents")
                sendLog('actor','Failed to reject the familly of %s'% wfname, level='critical')
                continue

            cloned = None
            try:  
                cloned =  singleClone(url, wfname, tasks, comment, options.do)
            except Exception as e:
                sendLog('actor','Failed to create clone for %s! Check logs for more information. Action will need to be resubmitted.'%wfname,level='critical')
                wfi.sendLog('actor','Failed to create clone for %s!'%wfname)
                print str(e)
                ##let's not remove the action other the workflow goes to "trouble" and the WTC cannot set the action again
            if not cloned:
                recover = False
                wfi.sendLog('actor','Failed to create clone for %s!'%wfname)
                sendLog('actor','Failed to create clone for %s!'%wfname,level='critical')

            else:
                wfi.sendLog('actor',"Workflow %s cloned into %s"%(wfname, cloned))
                ## set to trouble for swift replacement
                for wfo in  session.query(Workflow).filter(Workflow.name == wfname).all():
                    wfo.status = 'trouble'
                session.commit()
#===========================================================
        elif to_force:
            wfi.sendLog('actor','Force-completing from workflow traffic controler request')
            WI.add(action='force', keyword = wfname, user = action_list[wfname].get( 'user', 'unified'))
        elif to_hold:
            wfi.sendLog('actor','Holding on workflow traffic controler request')
            WI.add(action='hold', keyword = wfname, user = action_list[wfname].get( 'user', 'unified'))                   
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
            print json.dumps(tasks, indent=2)

            all_tasks = wfi.getAllTasks()

            ## need a way to verify that this is the first round of ACDC, since the second round will have to be on the ACDC themselves
        
            try:
                WMErr = wfi.getWMErrors()
#               print WMErr
            except:
                sendLog('actor','Cannot create ACDCS for %s because WMErr cannot be reached.'%wfname,level='critical')
                continue
            if not WMErr:
                wfi.sendLog('actor','WMErrors is blank for %s.'%wfname)
                print "FYI getWMErrors is blank. Presumably there are only unreported errors"
#                continue

            try:
                where_to_run, missing_to_run,missing_to_run_at =  wfi.getRecoveryInfo()
                print "Where to run = "
                print where_to_run
                if not where_to_run:
                    sendLog('actor','Cannot create ACDCS for %s because recovery info cannot be found.'%wfname,level='critical')
                    continue
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
        
            if WMErr:
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
                print "Full task name is " + fulltaskname
                print where_to_run.keys()
                wrong_task = False
                for task_info in all_tasks:
                    if fulltaskname == task_info.pathName:
                        if task_info.taskType not in ['Processing','Production','Merge']:
                            wrong_task= True
                            wfi.sendLog('actor', "Skipping task %s because the taskType is %s. Can only ACDC Processing, Production, or Merge tasks"%( fulltaskname, task_info.taskType))
                if not fulltaskname in where_to_run.keys():
                    wrong_task= True
                    wfi.sendLog('actor', "Skipping task %s because there is no acdc doc for it anyways."%(fulltaskname))
                if wrong_task:
                    continue
                print tasks[task]
                actions = tasks[task]
                for action in actions:
                    if action.startswith('sites'):
                        if type(actions[action]) != list:
                            assign_to_sites=SI.SE_to_CEs(actions[action])
                        else:
                            assign_to_sites=[]
                            for site in actions[action]:
                                assign_to_sites.extend( SI.SE_to_CEs(site))
                            assign_to_sites=sorted(set(assign_to_sites))
#                    if action.startswith('mem') and actions[action] != "" and actions[action] != 'Same' and wfi.request['RequestType'] in ['TaskChain']:
#                        recover = False;
#                        print  "Skipping %s for now until Allie fixes memory parameter for TaskChain ACDCs."%wfname
#                        wfi.sendLog('actor',"Skipping %s for now until Allie fixes memory parameter for TaskChain ACDCs."%wfname)
                if not 'sites' in actions:
                    assign_to_sites=[]
                    for site in where_to_run[task]:
                        assign_to_sites.extend( SI.SE_to_CEs(site))
                    assign_to_sites=sorted(set(assign_to_sites))
                    print "Found",sorted(assign_to_sites),"as sites where to run the ACDC at, from the acdc doc of ",wfname
                print "Going to run at",sorted(assign_to_sites)
                if recover:
                    print "Initiating recovery"

                    acdc = singleRecovery(url, fulltaskname, wfi.request, actions, do = options.do, priority_change = True)

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
                        wfi.sendLog('actor','ACDC created for task %s. Actions taken \n%s'%(fulltaskname,json.dumps(actions)))
                        jira_comment = "%s created ACDC for task %s with action %s"%( 
                            action_list[wfname].get( 'user', 'unified'),
                            task.split('/')[-1] , 
                            json.dumps(actions),
                        )
                        reason = action_list[wfname].get( 'Reason', None)
                        if reason:
                            jira_comment += '\ndue to: %s'%(reason)

                        #team = wfi.request['Teams'][0]
                        team = 'production'
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

                        if 'secondary' in actions:
                            if actions['secondary'] == 'enabled':
                                print 'Enabling reading the secondary input via xrootd'
                                parameters['TrustPUSitelists'] = True
                            elif actions['secondary'] == 'disabled':
                                parameters['TrustPUSitelists'] = False
                            #in case secondary is blank or not set to enabled or disabled
                            elif 'TrustPUSitelists' in wfi.request and wfi.request['TrustPUSitelists']:
                                parameters['TrustPUSitelists'] = True
                        elif 'TrustPUSitelists' in wfi.request and wfi.request['TrustPUSitelists']:
                            parameters['TrustPUSitelists'] = True

                        if options.ass:
                            print "really doing the assignment of the ACDC",acdc
                            parameters['execute']=True
                            #wfi.sendLog('actor',"%s  was assigned for recovery"% acdc)
                        else:
                            print "no assignment done with this ACDC",acdc
                            sendLog('actor',"%s needs to be assigned"%(acdc), level='critical')
                            wfi.sendLog('actor',"%s needs to be assigned by hand"%(acdc))
                            continue

 #                       print parameters
                        result = reqMgrClient.assignWorkflow(url, acdc, team, parameters)
                        if not result:
                            print acdc,"was not assigned"
                            sendLog('actor',"%s failed to be assigned"%(acdc), level='critical')
                            wfi.sendLog('actor',"%s failed to get assigned for recovery"% acdc)
                        else:
                            wfi.sendLog('actor',"%s was assigned for recovery"% acdc)
                            recovering.add( acdc )

                        #wfi.sendLog('actor',"ACDCs created for %s"%wfname)
                        try:
                            if jira_comment:
                                jiras = JC.find({'prepid' : wfi.request['PrepID']})
                                if len(jiras)==1:
                                    ## put a comment on the single corresponding ticket
                                    JC.comment(jiras[0].key, jira_comment)
                                    JC.progress(jiras[0].key)
                        except Exception as e:
                            print "failed with JIRA"
                            print str(e)
                        
    
        #===========================================================
        
        
        if recover and options.do:
            r = WC.remove_action(wfname)
            if not r:
                sendLog('actor','not able to remove the action, interlocking the module', level='critical')
                os.system('env EOS_MGM_URL=root://eoscms.cern.ch eos touch %s/actor.failed-%s.lock'%( base_eos_dir, os.getpid() ))
                sys.exit(-1)

        ## update the status with recovering removing manual
        for wfo in  session.query(Workflow).filter(Workflow.name == wfname).all():
            wfo.status = wfo.status.replace('manual','recovering')
        session.commit()                        

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
    parser.add_option('--spec',default=None,help='a specific workflow to consider')
    parser.add_option('--actions', default=None,help='a file name with the actions to be taken')
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

