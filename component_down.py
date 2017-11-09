import os
import time
import json
from collections import defaultdict

agent= os.getenv('HOSTNAME')
now = time.mktime(time.gmtime())
since = 7 ## in days

base_dir = '/data/srv/wmagent/current/install/wmagent/'
restarts = { 
    "timestamp" : now,
    "since" : since,
    "agent" : os.getenv('HOSTNAME'),
    "data" : defaultdict(list)
    }
   
for component in os.listdir(base_dir):
    if os.path.isfile(component):continue
    print component
    #for grep in os.popen('grep Harness %s/%s/ComponentLog | grep Starting | grep %s'%( base_dir, component,component)):
    for grep in os.popen('grep terminated %s/%s/ComponentLog | grep %s'%( base_dir, component,component)):
        timestamp = grep.split('INFO')[0][:-1].split(',')[0]
        #2016-08-17 16:10:26,753:140679060952832
        restart_date = time.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
        restart_time = time.mktime(restart_date)
        if (now - restart_time) > (since*24*60*60): continue
        print timestamp,time.asctime( restart_date )
        restarts['data'][component].append( restart_time )

open('/afs/cern.ch/user/c/cmst1/www/Automatic_Agent_json/%s.restart.json'% agent,'w').write( json.dumps( restarts, indent=2) )
#want to add to eos /eos/project/c/cms-unified-logs/www/

