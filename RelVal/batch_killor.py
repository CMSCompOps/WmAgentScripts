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

batches_colnames = [desc[0] for desc in curs.description]

for batches_row in batches_rows:

    batch_dict=dict(zip(batches_colnames, batches_row))

    print batches_row
    curs.execute("select * from workflows where useridyear = \""+batch_dict["useridyear"]+"\" and useridmonth = \""+batch_dict["useridmonth"]+"\" and useridday = \""+batch_dict["useridday"]+"\" and useridnum = "+str(batch_dict["useridnum"])+" and batch_version_num = "+str(batch_dict["batch_version_num"])+";")
    workflows_rows=curs.fetchall()

    workflows_colnames = [desc[0] for desc in curs.description]

    for workflow_row in workflows_rows:

        wf_dict=dict(zip(workflows_colnames, workflow_row))

        print wf_dict["workflow_name"]
        workflow=wf_dict["workflow_name"]

        url="cmsweb.cern.ch" 

        headers = {"Content-type": "application/json", "Accept": "application/json"}

        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request('GET','/reqmgr2/data/request/' + workflow, headers = headers)
        r2=conn.getresponse()
        j1 = json.loads(r2.read())

        if r2.status != 200:
            os.system('echo '+workflow+' | mail -s \"batch_killor.py error 2\" andrew.m.levin@vanderbilt.edu')
            sys.exit(1)

        j1 = j1['result']

        if len(j1) != 1:
            os.system('echo '+wf[0]+' | mail -s \"batch_killor.py error 3\" andrew.m.levin@vanderbilt.edu')
            sys.exit(1)

        j1 = j1[0]

        j1 = j1[workflow]

        status= j1['RequestStatus']

        if status == "completed" or status == "assignment-approved" or status == "assigned" or status == "running-open" or status == "running-closed" or status == "acquired":        
            if status == "completed" or status == "assignment-approved":
                reqMgrClient.rejectWorkflow(url,workflow)
            if status == "assigned" or status == "running-open" or status == "running-closed" or status == "acquired":
                reqMgrClient.abortWorkflow(url,workflow)
        elif status != "aborted-archived" and status != "aborted" and status != "rejected" and status != "rejected-archived":
            os.system('echo '+workflow+' | mail -s \"batch_killor.py error 1\" andrew.m.levin@vanderbilt.edu --')
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

    curs.execute("update batches set status=\"killed\", current_status_start_time=\""+datetime.datetime.now().strftime("%y:%m:%d %H:%M:%S")+"\" where useridyear = \""+batch_dict["useridyear"]+"\" and useridmonth = \""+batch_dict["useridmonth"]+"\" and useridday = \""+batch_dict["useridday"]+"\" and useridnum = "+str(batch_dict["useridnum"])+" and batch_version_num = "+str(batch_dict["batch_version_num"])+";")

    curs.execute("delete from datasets where useridyear = \""+batch_dict["useridyear"]+"\" and useridmonth = \""+batch_dict["useridmonth"]+"\" and useridday = \""+batch_dict["useridday"]+"\" and useridnum = "+str(batch_dict["useridnum"])+" and batch_version_num = "+str(batch_dict["batch_version_num"])+";")

    mysqlconn.commit()

