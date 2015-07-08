import reqMgrClient
import MySQLdb
import sys
import datetime
import optparse
import json
import urllib2,urllib, httplib, sys, re, os

parser = optparse.OptionParser()
#parser.add_option('--correct_env',action="store_true",dest='correct_env')
(options,args) = parser.parse_args()

#command=""
#for arg in sys.argv:
#    command=command+arg+" "

#if not options.correct_env:
#    os.system("source /cvmfs/grid.cern.ch/emi-ui-3.7.3-1_sl6v2/etc/profile.d/setup-emi3-ui-example.sh; export X509_USER_PROXY=/tmp/x509up_u13536; python2.6 "+command + "--correct_env")
#    sys.exit(0)

dbname = "relval"

mysqlconn = MySQLdb.connect(host='dbod-cmsrv1.cern.ch', user='relval', passwd="relval", port=5506)
#conn = MySQLdb.connect(host='localhost', user='relval', passwd="relval")

curs = mysqlconn.cursor()

curs.execute("use "+dbname+";")



curs.execute("select * from batches where status = \"reject_abort_requested\";")
batches_rows=curs.fetchall()

colnames = [desc[0] for desc in curs.description]

for batches_row in batches_rows:

    for name, value in zip(colnames, batches_row):
        if name == "useridday":
            useridday=value
        elif name == "useridmonth":    
            useridmonth=value
        elif name == "useridyear":    
            useridyear=value
        elif name == "useridnum":    
            useridnum=value
        elif name == "batch_version_num":    
            batch_version_num=value

    print batches_row
    batchid=batches_row[0]
    curs.execute("select * from workflows where useridyear = \""+useridyear+"\" and useridmonth = \""+useridmonth+"\" and useridday = \""+useridday+"\" and useridnum = "+str(useridnum)+" and batch_version_num = "+str(batch_version_num)+";")
    workflows_rows=curs.fetchall()

    for workflow_row in workflows_rows:
        print workflow_row[5]
        workflow=workflow_row[5]
        url="cmsweb.cern.ch" 
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request('GET','/reqmgr/reqMgr/request?requestName=' + workflow)
        r2=conn.getresponse()
        j1 = json.loads(r2.read())
        status= j1['RequestStatus']
        
        print status

        if status == "completed" or status == "assignment-approved" or status == "assigned" or status == "running-open" or status == "running-closed" or status == "acquired":        
            if status == "completed" or status == "assignment-approved":
                reqMgrClient.rejectWorkflow(url,workflow)
            if status == "assigned" or status == "running-open" or status == "running-closed" or status == "acquired":
                reqMgrClient.abortWorkflow(url,workflow)
        elif status != "aborted-archived" and status != "aborted" and status != "rejected" and status != "rejected-archived":
            os.system('echo '+workflow+' | mail -s \"batch_rejecter_aborter.py error 1\" andrew.m.levin@vanderbilt.edu --')
            sys.exit(1)
        else:
            continue

        #headers={"Content-type": "application/x-www-form-urlencoded","Accept": "text/plain"}
        #params = {"requestName" : workflow,"status" : newstatus}
        #encodedParams = urllib.urlencode(params)
        #conn.request("PUT", "/reqmgr/reqMgr/request", encodedParams, headers)
        #response = conn.getresponse()
        #print response.status, response.reason
    #    #curs.execute("insert into workflows_archive VALUES "+str(workflow_row)+";")

    print "copying the workflows and the batch to the archive databases"    

    curs.execute("update batches set status=\"killed\", current_status_start_time=\""+datetime.datetime.now().strftime("%y:%m:%d %H:%M:%S")+"\" where useridyear = \""+useridyear+"\" and useridmonth = \""+useridmonth+"\" and useridday = \""+useridday+"\" and useridnum = "+str(useridnum)+" and batch_version_num = "+str(batch_version_num)+";")

    curs.execute("delete from datasets where useridyear = \""+useridyear+"\" and useridmonth = \""+useridmonth+"\" and useridday = \""+useridday+"\" and useridnum = "+str(useridnum)+" and batch_version_num = "+str(batch_version_num)+";")

    mysqlconn.commit()

