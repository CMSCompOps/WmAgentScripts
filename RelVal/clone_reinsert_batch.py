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

batch_version_num=userid.split('_')[4]
useridnum=userid.split('_')[3]
useridday=userid.split('_')[2]
useridmonth=userid.split('_')[1]
useridyear=userid.split('_')[0]

print batch_version_num
print useridnum
print useridday
print useridmonth
print useridyear

assert(batch_version_num == "0")

curs.execute("select * from batches where batch_version_num = "+batch_version_num+" and useridnum = "+ useridnum+" and useridday = \""+ useridday + "\" and useridmonth = \"" + useridmonth + "\" and useridyear = \""+ useridyear +"\";")
batches_rows=curs.fetchall()

if len(batches_rows) != 1:
    print "number of batches with this batch id is not equal to 1, exiting"
    sys.exit(1)

print "inserting the request in the clone_reinsert_requets table"

curs.execute("insert into clone_reinsert_requests set batch_version_num = "+batch_version_num+", useridnum = "+ useridnum+", useridday = \""+ useridday + "\", useridmonth = \"" + useridmonth + "\", useridyear = \""+ useridyear +"\", new_site=\""+newsite+"\", new_processing_version="+str(newprocessingversion)+";")

conn.commit()
