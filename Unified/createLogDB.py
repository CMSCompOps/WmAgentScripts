#!/usr/bin/env python
import os
import json
import sys

from assignSession import *
import random
import optparse
from utils import moduleLock

parser = optparse.OptionParser()
parser.add_option('--workflow', help='Which workflow logs', default=None)
parser.add_option('--years',help='What year to parse', default='2017,2018')
parser.add_option('--months',help='What month to parse', default=None)
parser.add_option('--max',help='Limit the number of indexion', default=0, type=int)
(options,args) = parser.parse_args()


specific = options.workflow.split(',') if options.workflow else None
check_months = options.months.split(',') if options.months else None
check_years= options.years.split(',') if options.years else None

ml = moduleLock( component='createLogDB_%s'%options.workflow, wait=True, silent=True)
if ml():
    print "existing createLogDB",options.workflow
    sys.exit(1)

years = filter(None,os.popen('ls /eos/cms/store/logs/prod/').read().split('\n'))


vetoes = ['Express_Run','PromptReco_Run','Repack_Run','Validation','test','Test']
print years
n_index=0
for year in years:
    if options.max and n_index>options.max: break
    if check_years and not year in check_years : continue
    months = filter(None,os.popen('ls /eos/cms/store/logs/prod/%s/'%(year)).read().split('\n'))
    print year,months
    for month in months:
        if options.max and n_index>options.max: break
        if check_months and not month in check_months : continue
        workflows = filter(None,os.popen('ls /eos/cms/store/logs/prod/%s/%s/WMAgent/'%(year,month)).read().split('\n'))
        random.shuffle( workflows )
        print year,month,len(workflows)
        ## start reading
        for workflow in workflows:
            if options.max and n_index>options.max: break
            if specific and not any(s in workflow or workflow in s for s in specific): continue

            if any(v in workflow or workflow in v for v in vetoes): continue

            tars = filter(None,os.popen('ls /eos/cms/store/logs/prod/%s/%s/WMAgent/%s/'%(year,month,workflow)).read().split('\n'))
            print workflow,len(tars)
            logs_alread_in_db = list(set([l.logfile  for l in session.query(LogRecord).filter(LogRecord.workflow == workflow).all()]))
            already_in_db = list(set([l.path for l in session.query(LogRecord).filter(LogRecord.workflow == workflow).all()]))
            print len(already_in_db),"already in db"
            print len(tars),"tarball in eos"
            if len(tars) == len(already_in_db):
                print "Chances are that it is useless to go on"
                continue

            what = [filter(lambda b : b.startswith('LogCollect') or b.endswith('LogCollect'), tar.split('-')) for tar in tars]

            tasks = set(w[0] for w in what if w)
            print tasks
            for task in tasks:
                tars = filter(None,os.popen('ls /eos/cms/store/logs/prod/%s/%s/WMAgent/%s/*%s*.tar'%(year,month,workflow,task)).read().split('\n'))

                print task,len(tars)
                for tar in tars:
                    local='/tmp/vlimant'
                    tar = tar.split('/')[-1]
                    ## this is heavy, how can we avoid this ?
                    path = '/eos/cms/store/logs/prod/%s/%s/WMAgent/%s/%s'%(year,month,workflow,tar)
                    if path in already_in_db:
                        print path,"already in DB"
                        continue
                    
                    ## a full copy is a bit too much
                    tarloc = '/eos/cms/store/logs/prod/%s/%s/WMAgent/%s/%s'%( year,month,workflow,tar)
                    tarlloc = '%s/%s'%( local, tar)

                    logs = filter(None,map(lambda b : b.split('/')[-1], os.popen('tar tf %s'%(tarloc)).read().split('\n')))
                    for log in logs:
                        record = LogRecord(
                            workflow= workflow,
                            logfile = log,
                            path = path,
                            task = task[:40],
                            year = int(year),
                            month = int(month)
                            )
                        print "add",log
                        session.add( record )
                        n_index+=1
                        if (n_index%20==0):
                            print "\t committing",n_index
                            session.commit()
                    session.commit()

