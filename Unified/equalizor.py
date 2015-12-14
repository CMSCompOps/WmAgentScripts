#!/usr/bin/env python
from assignSession import *
from utils import workflowInfo, getWorkflows
import reqMgrClient
import json
import os
import optparse

def equalizor(url , specific = None):

    if not specific:
        workflows = getWorkflows(url, status='running-closed', details=True)
        workflows.extend(getWorkflows(url, status='running-open', details=True))

    ## start from scratch
    modifications = {}
    
    def running_idle( wfi , task_name):
        gmon = wfi.getGlideMon()
        if not gmon: return (0,0)
        if not task_name in gmon: return (0,0)
        return (gmon[task_name]['Running'], gmon[task_name]['Idle'])

    def needs_action( wfi, task):
        task_name = task.pathName.split('/')[-1]
        running, idled = running_idle( wfi, task_name)
        if not idled and not running : return False, task_name, running, idled
        if (not running and idled) or (idled / float(running) > 0.3):
            return True, task_name, running, idled
        else:
            return False, task_name, running, idled

    for wfo  in session.query(Workflow).filter(Workflow.status == 'away').all():
        if specific and not specific in wfo.name: 
            continue
        if specific:
            wfi = workflowInfo(url, wfo.name)
        else:
            cached = filter(lambda d : d['RequestName']==wfo.name, workflows)
            if not cached : continue
            wfi = workflowInfo(url, wfo.name, request = cached[0])
        

        if wfi.request['Campaign'] == 'RunIIWinter15wmLHE':
            ## then set all Processing job to the original whitelist            
            tasks = wfi.getWorkTasks()
            for task in tasks:
                if task.taskType == 'Processing':
                    needs, task_name, running, idled = needs_action(wfi, task)
                    if needs:
                        print task_name,"of",wfo.name,"running",running,"and pending",idled,"taking action : ReplaceSiteWhitelist"
                        set_to = wfi.request['SiteWhitelist']
                        modifications[wfo.name] = { task.pathName : { "ReplaceSiteWhitelist" : set_to }}

        if wfi.request['Campaign'] == 'RunIIFall15DR76':
            ## we should add all sites that hold the secondary input if any
            secondary_locations = ['T1_ES_PIC','T2_US_Purdue','T2_UK_SGrid_RALPP','T2_BE_IIHE','T2_DE_DESY','T2_IT_Legnaro','T2_US_Caltech','T1_DE_KIT','T2_UK_London_Brunel','T2_IT_Pisa','T1_US_FNAL','T2_IT_Rome','T2_US_Florida','T1_IT_CNAF','T1_RU_JINR','T2_UK_London_IC','T2_US_Nebraska','T2_FR_CCIN2P3','T2_US_UCSD','T2_ES_CIEMAT','T1_FR_CCIN2P3','T2_US_Wisconsin','T2_US_MIT','T2_DE_RWTH','T1_UK_RAL','T2_US_Vanderbilt','T2_CH_CERN']
            ## should discover the above from secondary location (remember to cache this)
            #(lheinput,primary,parent,secondary, sites_allowed) = wfh.getSiteWhiteList()

            ## removing the ones in the site whitelist already since they encode the primary input location
            augment_by = list(set(secondary_locations)- set(wfi.request['SiteWhitelist']))
            tasks = wfi.getWorkTasks() ## not tampering with merge and all
            for task in tasks:
                if hasattr(task.input, 'dataset'):
                    needs, task_name, running, idled = needs_action(wfi, task)
                    if needs:
                        ## the step with an input ought to be the digi part : make this one go anywhere
                        modifications[wfo.name] = {task.pathName : { "AddWhitelist" : augment_by }}
                        print task_name,"of",wfo.name,"running",running,"and pending",idled,"taking action : AddWhitelist"

    old_modifications = json.loads( open('/afs/cern.ch/user/c/cmst2/www/unified/equalizor.json').read())
    old_modifications.update( modifications )

    open('/afs/cern.ch/user/c/cmst2/www/unified/equalizor.json.new','w').write( json.dumps( old_modifications, indent=2))
    os.system('mv /afs/cern.ch/user/c/cmst2/www/unified/equalizor.json.new /afs/cern.ch/user/c/cmst2/www/unified/equalizor.json')
            

if __name__ == "__main__":
    url = 'cmsweb.cern.ch'

    parser = optparse.OptionParser()
    (options,args) = parser.parse_args()
    spec=None
    if len(args)!=0:
        spec = args[0]

    equalizor(url, spec)

