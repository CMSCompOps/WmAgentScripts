#!/usr/bin/env python
import sys
import os
import glob
from utils import getWorkflowById, reqmgr_url, workflowInfo

import optparse
parser = optparse.OptionParser()
parser.add_option('--go',default=False, action = 'store_true', help='Answer yes to all')
parser.add_option('--dry', default=False, action = 'store_true', help='Just show all')
def_assignoptions = '--site acdc --xrootd'
parser.add_option('--existing',default=False, action = 'store_true', help='Look for exsiting recovery json')
parser.add_option('--norecovery',default=False, action = 'store_true', help='Do not run the recovery process')

parser.add_option('--nocreation', default=False, action = 'store_true', help='Do not inject new request')
parser.add_option('--assignoptions',default=def_assignoptions, help='The options to pass to assign.py (default is %s)'%def_assignoptions)
parser.add_option('--workflows',help='Coma separated list of workflows to recover')
(options,args) = parser.parse_args()


wfs=options.workflows.split(',')
for wf in wfs:
    print "Starting recovery procedure for",wf
    json_files = []
    if options.existing:
        keyword = '_'.join(wf.split('_')[:2])
        keyword = 'recovery*-%s*.json'% (keyword)
        print "looking for",keyword
        json_files = glob.glob(keyword)
        print "Found",len(json_files),"already created json files"
        print '\n'.join(sorted(json_files))
    elif options.norecovery:
        print "skipping recovery process"
    else:
        com = 'python recoverMissingLumis.py -q %s -g DATAOPS -f -r %s'%( os.getenv('USER'), wf)
        print com
        y = (raw_input('go ?') if not options.dry else 'dry') if not options.go else 'y'
        if y.lower() in ['y','yes','go','ok']:
            go = os.popen(com)
            full_log = go.read().split('\n')
            this_json = None
            for line in full_log:
                if line.startswith('Created JSON'):
                    this_json = line.split()[2]
                    json_files.append( this_json )
                    print line
                if 'This will recover' in line:
                    print line
            if not this_json:
                print '\n'.join(full_log)
    createst_wfs = []
    if options.nocreation:
        ## look for it in reqmgr
        wfi = workflowInfo( reqmgr_url, wf)
        pid = wfi.request['PrepID']
        familly = filter(lambda r : r['RequestStatus'] == 'assignment-approved' and 'recovery' in r['RequestName'],
                         getWorkflowById( reqmgr_url, pid, details=True))
        createst_wfs = [r['RequestName'] for r in familly]
        print "Found",len(createst_wfs),"already existing recoveries"
        print '\n'.join(sorted(createst_wfs))
    else:
        for json in json_files:
            com = 'python reqMgrClient.py -j %s'%( json )
            print com
            y = (raw_input('go ?') if not options.dry else 'dry') if not options.go else 'y'
            if y.lower() in ['y','yes','go','ok']:
                go = os.popen(com)
                full_log = go.read().split('\n')
                wf_created = None
                for line in full_log:
                    if line.startswith("Created:"):
                        wf_created = line.split()[1]
                        createst_wfs.append( wf_created ) 
                        break
                if not wf_created:
                    print '\n'.join(full_log)
    print len(createst_wfs),"that can be submitted"
    for wf in createst_wfs:
        com = './assign.py %s --w %s'%( options.assignoptions, wf)
        print com
        y = (raw_input('go ?') if not options.dry else 'dry') if not options.go else 'y'
        if y.lower() in ['y','yes','go','ok']:
            go = os.popen(com)
            full_log = go.read().split('\n')
            for line in full_log:
                if line.startswith("Assigned workflow"):
                    print line





