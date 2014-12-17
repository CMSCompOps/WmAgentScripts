import MySQLdb
import sys
import datetime

batchid=sys.argv[1]

dbname = "relval"

conn = MySQLdb.connect(host='localhost', user='relval', passwd="relval")

curs = conn.cursor()

curs.execute("use "+dbname+";")

curs.execute("select * from workflows where batch_id = \""+ batchid+"\";")
workflows_rows=curs.fetchall()

curs.execute("select * from batches where batch_id = \""+ batchid+"\";")
batches_rows=curs.fetchall()

if len(batches_rows) != 1:
    print "number of batches with this batch id is not equal to 1, exiting"
    sys.exit(1)
if len(workflows_rows) == 0 :
    print "no workflows with this batch id, exiting"
    sys.exit(1)

print "setting the status of the batch to reject_abort_requested"

curs.execute("update batches set status=\"reject_abort_requested\", current_status_start_time=\""+datetime.datetime.now().strftime("%y:%m:%d %H:%M:%S")+"\" where batch_id = "+str(batchid) +";")
