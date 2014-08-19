import MySQLdb
import sys

batchid=sys.argv[1]

dbname = "relval3"

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

print "copying the workflows and the batch to the archive databases"

curs.execute("insert into batches_archive VALUES "+str(batches_rows[0])+";")

for workflow_row in workflows_rows:
    curs.execute("insert into workflows_archive VALUES "+str(workflow_row)+";")

print "deleting the workflows and the batch from the original databases"
curs.execute("delete from workflows where batch_id = \""+ batchid+"\";")
curs.execute("delete from batches where batch_id = \""+ batchid+"\";")
