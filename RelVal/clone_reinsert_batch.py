import MySQLdb
import sys
import datetime

userid=sys.argv[1]
newsite=sys.argv[2]
newprocessingversion=sys.argv[3]

dbname = "relval"

conn = MySQLdb.connect(host='dbod-cmsrv1.cern.ch', user='relval', passwd="relval", port=5506)
#conn = MySQLdb.connect(host='localhost', user='relval', passwd="relval")

curs = conn.cursor()

curs.execute("use "+dbname+";")

useridnum=userid.split('_')[3]
useridday=userid.split('_')[2]
useridmonth=userid.split('_')[1]
useridyear=userid.split('_')[0]

print useridnum
print useridday
print useridmonth
print useridyear

curs.execute("select batch_id from batches_archive where useridnum = "+ useridnum+" and useridday = \""+ useridday + "\" and useridmonth = \"" + useridmonth + "\" and useridyear = \""+ useridyear +"\";")
batches_archive_rows=curs.fetchall()
curs.execute("select batch_id from batches where useridnum = "+ useridnum+" and useridday = \""+ useridday + "\" and useridmonth = \"" + useridmonth + "\" and useridyear = \""+ useridyear +"\";")
batches_rows=curs.fetchall()

print len(batches_archive_rows)
print len(batches_rows)

if len(batches_archive_rows) == 1 and len(batches_rows) == 0:
    batchid = str(batches_archive_rows[0][0])
elif len(batches_rows) == 1 and len(batches_archive_rows) == 0:
    batchid = str(batches_rows[0][0])
else:
    print "could not find a batch with this userid or found multiple batches with this userid"
    sys.exit(0)

print batchid

curs.execute("select * from batches where batch_id = \""+ batchid+"\";")
batches_rows=curs.fetchall()
curs.execute("select * from batches_archive where batch_id = \""+ batchid+"\";")
batches_archive_rows=curs.fetchall()

if len(batches_rows)+len(batches_archive_rows) != 1:
    print "number of batches with this batch id is not equal to 1, exiting"
    sys.exit(1)

print "inserting the request in the clone_reinsert_requets table"

curs.execute("insert into clone_reinsert_requests set batch_id="+str(batchid)+", new_site=\""+newsite+"\", new_processing_version="+str(newprocessingversion)+";")

conn.commit()
