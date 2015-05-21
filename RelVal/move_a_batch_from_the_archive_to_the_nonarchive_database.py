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
    os.system("source /cvmfs/grid.cern.ch/emi-ui-3.7.3-1_sl6v2/etc/profile.d/setup-emi3-ui-example.sh; export X509_USER_PROXY=/tmp/x509up_u13536; python2.6 "+command + "--correct_env")
    sys.exit(0)

dbname = "relval"

mysqlconn = MySQLdb.connect(host='dbod-altest1.cern.ch', user='relval', passwd="relval", port=5505)
#conn = MySQLdb.connect(host='localhost', user='relval', passwd="relval")

curs = mysqlconn.cursor()

curs.execute("use "+dbname+";")

curs.execute("select * from batches_archive;")
batches_rows=curs.fetchall()

for batches_row in batches_rows:

    batchid=batches_row[0]

    if batchid!=93:
        continue

    print batches_row

    curs.execute("select * from workflows_archive where batch_id = \""+ str(batchid)+"\";")
    workflows_rows=curs.fetchall()

    print "changing the statuses of the workflows in batch "+str(batchid)+" in the request manager"

    print "copying the workflows and the batch to the archive databases"    

    print batchid    
    curs.execute("update batches_archive set status=\"assigned\", current_status_start_time=\""+datetime.datetime.now().strftime("%y:%m:%d %H:%M:%S")+"\" where batch_id = "+str(batchid) +";")

    mysqlconn.commit()

    curs.execute("select * from batches_archive where batch_id = \""+ str(batchid)+"\";")
    updated_batches_rows=curs.fetchall()
    if len(updated_batches_rows) != 1:
        print "number of batches with this batch id is not equal to 1, exiting"
        sys.exit(1)
        
    curs.execute("insert into batches VALUES "+str(tuple(str(entry) for entry in updated_batches_rows[0]))+";")

    curs.execute("select * from workflows_archive where batch_id = \""+ str(batchid)+"\";")
    workflows_rows=curs.fetchall()

    for workflow_row in workflows_rows:
        curs.execute("insert into workflows VALUES "+str(workflow_row)+";")

    print "deleting the workflows and the batch from the original databases"    

    curs.execute("delete from workflows_archive where batch_id = \""+ str(batchid)+"\";")
    curs.execute("delete from batches_archive where batch_id = \""+ str(batchid)+"\";")

    mysqlconn.commit()
