import os
import glob
import time
import json
from collections import defaultdict
from utils import base_eos_dir

agent= os.getenv('HOSTNAME')
now = time.mktime(time.gmtime())
since = 7 ## in days

agent_log_base_dir = '/data/srv/wmagent/current/install/wmagent/'
restarts = { 
    "timestamp" : now,
    "since" : since,
    "agent" : os.getenv('HOSTNAME'),
    "data" : defaultdict(list)
    }
   
for logpath in glob.glob('%s/*/ComponentLog'% agent_log_base_dir):
    _,component,_ = logpath.rsplit('/',2)
    print component
    #for grep in os.popen('grep Harness %s/%s/ComponentLog | grep Starting | grep %s'%( base_dir, component,component)):
    for grep in os.popen('grep terminated %s | grep %s'%( logpath ,component )):
        timestamp = grep.split('INFO')[0][:-1].split(',')[0]
        #2016-08-17 16:10:26,753:140679060952832
        restart_date = time.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
        restart_time = time.mktime(restart_date)
        if (now - restart_time) > (since*24*60*60): continue
        print timestamp,time.asctime( restart_date )
        restarts['data'][component].append( restart_time )

open('/data/srv/wmagent/current/bin/%s.restart.json'% agent,'w').write( json.dumps( restarts, indent=2) )
os.system("xrdcp -f /data/srv/wmagent/current/bin/*.restart.json root://eoscms.cern.ch/%s"%(base_eos_dir))


