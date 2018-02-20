#!/usr/bin/env python
import os
import json
import sys

from assignSession import *
import random
import optparse
from utils import moduleLock, duplicateLock

parser = optparse.OptionParser()
parser.add_option('--workflow', help='Which workflow logs', default=None)
parser.add_option('--years',help='What year to parse', default='2017,2018')
parser.add_option('--months',help='What month to parse', default=None)
parser.add_option('--max',help='Limit the number of indexion', default=0, type=int)
(options,args) = parser.parse_args()


eos='/usr/bin/eos'

specific = options.workflow.split(',') if options.workflow else None
check_months = options.months.split(',') if options.months else None
check_years= options.years.split(',') if options.years else None

years = filter(None,os.popen('%s ls /eos/cms/store/logs/prod/'%eos).read().split('\n'))

### make sure that we run this only on one instance
if duplicateLock('createLogDB', wait=True):
    print "existing createlog"
    sys.exit(1)

vetoes = ['Express_Run','PromptReco_Run','Repack_Run','Validation','test','Test']
print years
n_index=0
for year in years:
    if options.max and n_index>options.max: break
    if check_years and not year in check_years : continue
    months = filter(None,os.popen('%s ls /eos/cms/store/logs/prod/%s/'%(eos,year)).read().split('\n'))
    print year,months
    for month in months:
        if options.max and n_index>options.max: break
        if check_months and not month in check_months : continue
        workflows = filter(None,os.popen('%s ls /eos/cms/store/logs/prod/%s/%s/WMAgent/'%(eos,year,month)).read().split('\n'))
        random.shuffle( workflows )
        print year,month,len(workflows)
        ## start reading
        for workflow in workflows:
            if options.max and n_index>options.max: break
            if specific and not any(s in workflow or workflow in s for s in specific): continue

            #ml = moduleLock( component = 'createLogDB_%s'% workflow, wait=True )
            #if ml(): 
            #    print "the workflow is already been handled, skipping"
            #    continue

            if any(v in workflow or workflow in v for v in vetoes): continue
            tars = filter(None,os.popen('%s ls /eos/cms/store/logs/prod/%s/%s/WMAgent/%s/'%(eos,year,month,workflow)).read().split('\n'))
            print workflow,len(tars)
            logs_alread_in_db = list(set([l.logfile  for l in session.query(LogRecord).filter(LogRecord.workflow == workflow).all()]))
            already_in_db = list(set([l.path for l in session.query(LogRecord).filter(LogRecord.workflow == workflow).all()]))
            print len(already_in_db),"already in db"
            print len(tars),"tarball in eos"
            if len(tars) == len(already_in_db):
                print "Chances are that it is useless to go on"
                continue

            #print tars
            #tasks = set(map(lambda b : b.replace('LogCollect',''), [filter(lambda b : b.endswith('LogCollect'), tar.split('-'))[0] for tar in tars]))
            what = [filter(lambda b : b.startswith('LogCollect') or b.endswith('LogCollect'), tar.split('-')) for tar in tars]
            #what = [filter(lambda b : b.endswith('LogCollect'), tar.split('-')) for tar in tars]
            tasks = set(w[0] for w in what if w)
            print tasks
            for task in tasks:
                ## eos does not support * anywmore. how nice !!!
                tars = filter(None,os.popen('ls /eos/cms/store/logs/prod/%s/%s/WMAgent/%s/*%s*.tar'%(year,month,workflow,task)).read().split('\n'))
                #tars = filter(None,os.popen('%s ls /eos/cms/store/logs/prod/%s/%s/WMAgent/%s/*%s*.tar'%(eos,year,month,workflow,task)).read().split('\n'))
                print task,len(tars)
                for tar in tars:
                    local='/tmp/vlimant'
                    tar = tar.split('/')[-1]
                    ## this is heavy, how can we avoid this ?
                    path = '/eos/cms/store/logs/prod/%s/%s/WMAgent/%s/%s'%(year,month,workflow,tar)
                    if path in already_in_db:
                        print path,"already in DB"
                        continue
                    #if session.query(LogRecord).filter(LogRecord.path == path).first():
                    #    print path,"already in DB"
                    #    continue ## already parsed
                    
                    ## a full copy is a bit too much
                    tarloc = '/eos/cms/store/logs/prod/%s/%s/WMAgent/%s/%s'%( year,month,workflow,tar)
                    tarlloc = '%s/%s'%( local, tar)
                    ####os.system('%s cp /eos/cms/store/logs/prod/%s/%s/WMAgent/%s/%s %s/%s'%(eos,year,month,workflow,tar,  local,tar))
                    #os.system('%s cp %s %s'%(eos, tarloc, tarlloc))
                    #logs = filter(None,map(lambda b : b.split('/')[-1], os.popen('tar tf %s/%s'%(local,tar)).read().split('\n')))
                    #os.system('rm -f %s/%s'%(local,tar))
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

