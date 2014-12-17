import MySQLdb
import sys
import os
import httplib
import json
import optparse
import datetime
import time
import calendar

parser = optparse.OptionParser()
parser.add_option('--correct_env',action="store_true",dest='correct_env')
parser.add_option('--sent_to_hn',action="store_true",dest='send_to_hn')
(options,args) = parser.parse_args()

command=""
for arg in sys.argv:
    command=command+arg+" "

if not options.correct_env:
    os.system("source /afs/cern.ch/project/gd/LCG-share/current_3.2/etc/profile.d/grid-env.sh; python2.6 "+command + "--correct_env")
    sys.exit(0)
    
url='cmsweb.cern.ch'


dbname = "relval"

while True:

    os.system("if [ -f relval_monitor.txt ]; then rm relval_monitor.txt; fi")
    
    sys.stdout = open('relval_monitor.txt','a')

    print "last update = " + str(datetime.datetime.now())

    conn = MySQLdb.connect(host='localhost', user='relval', passwd='relval')

    curs = conn.cursor()
    
    curs.execute("use "+dbname+";")
    
    #workflow = line.rstrip('\n')
    #curs.execute("insert into workflows set hn_req=\""+hnrequest+"\", workflow_name=\""+workflow+"\";")

    curs.execute("select * from batches order by batch_id")
    batches=curs.fetchall()


    for batch in batches:
        print "    batch "+str(batch[0])
        print "        hypernews request: "+str(batch[1])
        print "        processing version: "+str(batch[6])
        #print batch[1]
        #print batch[2]
        
        curs.execute("select workflow_name from workflows where batch_id = "+ str(batch[0])+";")
        wfs=curs.fetchall()

        n_workflows=0
        n_completed=0
        max_completion_time=0
        for wf in wfs:
            n_workflows=n_workflows+1
            conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
            r1=conn.request('GET','/couchdb/wmstats/_all_docs?keys=["'+wf[0]+'"]&include_docs=true')
            r2=conn.getresponse()
            data = r2.read()
            s = json.loads(data)
            if r2.status != 200:
                print "problem retrieving information from couchdb about "+str(wf[0])+", exiting"
                os.system('echo '+wf[0]+' | mail -s \"monitorying.py error 1\" andrew.m.levin@vanderbilt.edu -- -f amlevin@mit.edu')
                sys.exit(1)
                


            #print s['rows'][0]['doc']['request_status']
            #print len(s['rows'][0]['doc']['request_status'])

            if 'error' in s['rows'][0] and s['rows'][0]['error'] == 'not_found':
                os.system('echo '+wf[0]+' | mail -s \"monitoring.py error 2\" andrew.m.levin@vanderbilt.edu -- -f amlevin@mit.edu')
                continue    
            
            for status in s['rows'][0]['doc']['request_status']:
                if status['status'] == "completed":
                    n_completed=n_completed+1
                    if status['update_time'] > max_completion_time:
                        max_completion_time = status['update_time']
                    break    
                        
        #print "(calendar.timegm(datetime.datetime.utcnow().utctimetuple()) - max_completion_time)/60.0/60.0 = " + str((calendar.timegm(datetime.datetime.utcnow().utctimetuple()) - max_completion_time)/60.0/60.0)
                        

        print "        n_workflows = " + str(n_workflows)
        print "        n_completed = " + str(n_completed)

    sys.stdout.flush()    
    os.system("cp relval_monitor.txt /afs/cern.ch/user/r/relval/webpage/relval_monitor.txt")

    sys.exit(0)
    time.sleep(60)

        
