import MySQLdb
import sys
import datetime

userid=sys.argv[1]

dbname = "relval"

conn = MySQLdb.connect(host='dbod-cmsrv1.cern.ch', user='relval', passwd="relval", port=5506)
#conn = MySQLdb.connect(host='localhost', user='relval', passwd="relval")

curs = conn.cursor()

curs.execute("use "+dbname+";")

#colnames = [desc[0] for desc in curs.description]

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

curs.execute("select * from workflows where batch_version_num = "+str(batch_version_num)+" and useridnum = "+ str(useridnum)+" and useridday = \""+ useridday + "\" and useridmonth = \"" + useridmonth + "\" and useridyear = \""+ useridyear +"\";")
workflows_rows=curs.fetchall()

curs.execute("select * from batches where batch_version_num = "+str(batch_version_num)+" and useridnum = "+ str(useridnum)+" and useridday = \""+ useridday + "\" and useridmonth = \"" + useridmonth + "\" and useridyear = \""+ useridyear +"\";")
batches_rows=curs.fetchall()

if len(batches_rows) != 1:
    print "number of batches with this batch id is not equal to 1, exiting"
    sys.exit(1)
if len(workflows_rows) == 0 :
    print "no workflows with this batch id, exiting"
    sys.exit(1)

print "setting the status of the batch to reject_abort_requested"

curs.execute("update batches set status=\"reject_abort_requested\", current_status_start_time=\""+datetime.datetime.now().strftime("%y:%m:%d %H:%M:%S")+"\" where batch_version_num = "+str(batch_version_num)+" and useridnum = "+ str(useridnum)+" and useridday = \""+ useridday + "\" and useridmonth = \"" + useridmonth + "\" and useridyear = \""+ useridyear +"\";")

conn.commit()
