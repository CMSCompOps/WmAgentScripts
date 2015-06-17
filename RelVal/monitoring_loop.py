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
    os.system("source /cvmfs/grid.cern.ch/emi-ui-3.7.3-1_sl6v2/etc/profile.d/setup-emi3-ui-example.sh; export X509_USER_PROXY=/tmp/x509up_u13536; python2.6 "+command + "--correct_env")
    sys.exit(0)
    
url='cmsweb.cern.ch'


dbname = "relval"

while True:

    os.system("if [ -f relval_monitor_most_recent_50_batches.txt ]; then rm relval_monitor_most_recent_50_batches.txt; fi")
    
    sys.stdout = open('relval_monitor_most_recent_50_batches.txt','a')

    print "last update: " + str(datetime.datetime.now())
    print ""

    conn = MySQLdb.connect(host='dbod-cmsrv1.cern.ch', user='relval', passwd="relval", port=5506)

    curs = conn.cursor()
    
    curs.execute("use "+dbname+";")
    
    #workflow = line.rstrip('\n')
    #curs.execute("insert into workflows set hn_req=\""+hnrequest+"\", workflow_name=\""+workflow+"\";")

    curs.execute("select * from batches order by batch_id")
    batches=curs.fetchall()
    curs.execute("select * from batches_archive order by batch_id")
    batches_archive=curs.fetchall()
    colnames = [desc[0] for desc in curs.description]

    for batch in batches_archive:
        if "max_batch_id" not in vars():
            max_batch_id = batch[0]
        if batch[0] > max_batch_id:
            max_batch_id = batch[0]

    for batch in batches:
        if "max_batch_id" not in vars():
            max_batch_id = batch[0]
        if batch[0] > max_batch_id:
            max_batch_id = batch[0]

    assert("max_batch_id" in vars() and max_batch_id > 50)        

    for batch_id in range(max_batch_id - 50, max_batch_id+1):

        batch_id = max_batch_id - 50 + (max_batch_id - batch_id)

        curs.execute("select * from batches where batch_id = "+str(batch_id)+";")
        batches_with_batch_id=curs.fetchall()
        curs.execute("select * from batches_archive where batch_id = "+str(batch_id)+";")
        batches_archive_with_batch_id=curs.fetchall()
        curs.execute("select workflow_name from workflows_archive where batch_id = "+ str(batch_id)+";")
        wfs_archive_with_batch_id=curs.fetchall()
        curs.execute("select workflow_name from workflows where batch_id = "+ str(batch_id)+";")
        wfs=curs.fetchall()


        if len(batches_archive_with_batch_id) == 1 and len(batches_with_batch_id) == 0:
            batches_with_batch_id=batches_archive_with_batch_id
            wfs=wfs_archive_with_batch_id
        elif len(batches_archive_with_batch_id) == 0 and len(batches_with_batch_id) == 0:
            continue
        elif not (len(batches_archive_with_batch_id) == 0 and len(batches_with_batch_id) == 1):
            os.system('echo '+wf[0]+' | mail -s \"monitoring.py error 3\" andrew.m.levin@vanderbilt.edu --')
            sys.exit(0)
        
        for name, value in zip(colnames, batches_with_batch_id[0]):
            if name == "useridday":
                useridday=value
            elif name == "useridmonth":
                useridmonth=value
            elif name == "useridyear":    
                useridyear=value
            elif name == "useridnum":
                useridnum=value

        print "id: "+useridyear+"_"+useridmonth+"_"+useridday+"_"+str(useridnum)
        for name, value in zip(colnames, batch):
            if name == "batch_id" or name == "useridday" or name == "useridmonth" or name == "useridyear" or name == "useridnum":
                continue
            else:
                print '    '+name.rstrip(' ')+': '+str(value)

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
                os.system('echo '+wf[0]+' | mail -s \"monitorying.py error 1\" andrew.m.levin@vanderbilt.edu --')
                sys.exit(1)
                


            #print s['rows'][0]['doc']['request_status']
            #print len(s['rows'][0]['doc']['request_status'])

            if 'error' in s['rows'][0] and s['rows'][0]['error'] == 'not_found':
                os.system('echo '+wf[0]+' | mail -s \"monitoring.py error 2\" andrew.m.levin@vanderbilt.edu --')
                continue    

            for status in s['rows'][0]['doc']['request_status']:
                if status['status'] == "completed":
                    n_completed=n_completed+1
                    if status['update_time'] > max_completion_time:
                        max_completion_time = status['update_time']
                    break    
                        
        #print "(calendar.timegm(datetime.datetime.utcnow().utctimetuple()) - max_completion_time)/60.0/60.0 = " + str((calendar.timegm(datetime.datetime.utcnow().utctimetuple()) - max_completion_time)/60.0/60.0)
                        

        print "    n_workflows = " + str(n_workflows)
        print "    n_completed = " + str(n_completed)
        print ""
        print ""
        
    sys.stdout.flush()    
    os.system("cp relval_monitor_most_recent_50_batches.txt /afs/cern.ch/user/r/relval/webpage/relval_monitor_most_recent_50_batches.txt")

    sys.exit(0)
    time.sleep(60)

        
