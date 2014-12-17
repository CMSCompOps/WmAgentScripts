import MySQLdb
import sys
import datetime
import optparse
import json
import urllib2,urllib, httplib, sys, re, os

parser = optparse.OptionParser()
parser.add_option('--correct_env',action="store_true",dest='correct_env')
(options,args) = parser.parse_args()

command=""
for arg in sys.argv:
    command=command+arg+" "

if not options.correct_env:
    os.system("source /afs/cern.ch/project/gd/LCG-share/current_3.2/etc/profile.d/grid-env.sh; python2.6 "+command + "--correct_env")
    sys.exit(0)

dbname = "relval"

conn = MySQLdb.connect(host='localhost', user='relval', passwd="relval")

curs = conn.cursor()

curs.execute("use "+dbname+";")

curs.execute("select * from batches where status = \"reject_abort_requested\";")
batches_rows=curs.fetchall()

for batches_row in batches_rows:
    print batches_row
    batchid=batches_row[0]
    curs.execute("select * from workflows where batch_id = \""+ str(batchid)+"\";")
    workflows_rows=curs.fetchall()

    print "changing the statuses of the workflows in batch "+str(batchid)+" in the request manager"

    #for workflow_row in workflows_rows:
    #    print workflow_row[1]
    #    workflow=workflow_row[1]
    #    url="cmsweb.cern.ch" 
    #    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    #    r1=conn.request('GET','/reqmgr/reqMgr/request?requestName=' + workflow)
    #    r2=conn.getresponse()
    #    j1 = json.loads(r2.read())
    #    status= s['RequestStatus']

    #    if status != "completed" and status != "assignment_approved" and status != "assigned" and status != "running-open" and status != "running-closed" and status != "acquired":
    #        os.system('echo '+workflow+' | mail -s \"batch_rejecter_aborter.py error\" andrew.m.levin@vanderbilt.edu -- -f amlevin@mit.edu')
    #        sys.exit(1)
    #    
    #    if status == "completed" or status == "assignment_approved":
    #        newstatus="rejected"
    #    if status == "assigned" or status == "running-open" or status == "running-closed" or status == "acquired":
    #        newstatus="aborted"            
    #        
    #    headers={"Content-type": "application/x-www-form-urlencoded","Accept": "text/plain"}
    #    params = {"requestName" : workflowname,"status" : newstatus}
    #    encodedParams = urllib.urlencode(params)
    #    conn.request("PUT", "/reqmgr/reqMgr/request", encodedParams, headers)
    #    response = conn.getresponse()
    #    print response.status, response.reason
    #    #curs.execute("insert into workflows_archive VALUES "+str(workflow_row)+";")

    print "copying the workflows and the batch to the archive databases"    

    print batchid    
    curs.execute("update batches set status=\"not_announced\", current_status_start_time=\""+datetime.datetime.now().strftime("%y:%m:%d %H:%M:%S")+"\" where batch_id = "+str(batchid) +";")
    curs.execute("select * from batches where batch_id = \""+ str(batchid)+"\";")
    updated_batches_rows=curs.fetchall()
    if len(updated_batches_rows) != 1:
        print "number of batches with this batch id is not equal to 1, exiting"
        sys.exit(1)
        
    curs.execute("insert into batches_archive VALUES "+str(tuple(str(entry) for entry in updated_batches_rows[0]))+";")

    curs.execute("select * from workflows where batch_id = \""+ str(batchid)+"\";")
    workflows_rows=curs.fetchall()

    for workflow_row in workflows_rows:
        curs.execute("insert into workflows_archive VALUES "+str(workflow_row)+";")

    print "deleting the workflows and the batch from the original databases"    

    curs.execute("delete from workflows where batch_id = \""+ str(batchid)+"\";")
    curs.execute("delete from batches where batch_id = \""+ str(batchid)+"\";")
