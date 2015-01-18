import MySQLdb
import sys
import datetime

batchid=sys.argv[1]
newsite=sys.argv[2]
newprocessingversion=sys.argv[3]

dbname = "relval"

conn = MySQLdb.connect(host='localhost', user='relval', passwd="relval")

curs = conn.cursor()

curs.execute("use "+dbname+";")

curs.execute("select * from batches where batch_id = \""+ batchid+"\";")
batches_rows=curs.fetchall()
curs.execute("select * from batches_archive where batch_id = \""+ batchid+"\";")
batches_archive_rows=curs.fetchall()

if len(batches_rows)+len(batches_archive_rows) != 1:
    print "number of batches with this batch id is not equal to 1, exiting"
    sys.exit(1)

print "inserting the request in the clone_reinsert_requets table"

curs.execute("insert into clone_reinsert_requests set batch_id="+str(batchid)+", new_site=\""+newsite+"\", new_processing_version="+str(newprocessingversion)+";")
