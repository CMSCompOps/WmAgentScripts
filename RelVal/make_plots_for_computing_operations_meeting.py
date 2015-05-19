import MySQLdb
import sys
import datetime
import calendar
import os

fname_stats = "computing_operations_meeting/23February2015_computing_meeting_report.txt"
fname_wfs = "computing_operations_meeting/23February2015_computing_meeting_report_wfs.txt"

dbname = "relval"

conn = MySQLdb.connect(host='dbod-altest1.cern.ch', user='relval', passwd="relval", port=5505)
#conn = MySQLdb.connect(host='localhost', user='relval', passwd="relval")

curs = conn.cursor()

curs.execute("use "+dbname+";")

curs.execute("select * from batches_archive;")
batches=curs.fetchall()

colnames = [desc[0] for desc in curs.description]

exitcode=os.system("ls "+fname_stats+" >& /dev/null")

if exitcode == 0:
    print fname_stats+" already exists, exiting"
    sys.exit(1)

exitcode=os.system("ls "+fname_wfs+" >& /dev/null")

if exitcode == 0:
    print fname_wfs+" already exists, exiting"
    sys.exit(1)

os.system("touch "+fname_wfs)
os.system("touch "+fname_stats)



for batch in batches:

    for name, value in zip(colnames, batch):
        if name=="status":
            status=value
        elif name == "batch_id":
            batchid=value
        elif name == "current_status_start_time":
            current_status_start_time=value
        elif name == "useridyear":
            useridyear=value
        elif name == "useridmonth":
            useridmonth=value
        elif name == "useridday":
            useridday=value
        elif name == "useridnum":
            useridnum=value

    if status!="announced":
        if status!="not_announced":
            print "batch has unexpected status \""+batch[7] +"\", exiting"
        continue

    userid=useridyear+"_"+useridmonth+"_"+useridday+"_"+str(useridnum)

    if calendar.timegm(current_status_start_time.utctimetuple()) < calendar.timegm(datetime.datetime(2015,2,16,14,0,0).timetuple()):
        continue

    if calendar.timegm(current_status_start_time.utctimetuple()) > calendar.timegm(datetime.datetime(2015,2,23,14,0,0).timetuple()):
        continue    

    curs.execute("select * from workflows_archive where batch_id = \""+ str(batchid)+"\";")
    workflows=curs.fetchall()

    for workflow in workflows:
        os.system("echo "+ workflow[1]+" >> "+fname_wfs)
    

    os.system("echo /afs/cern.ch/user/r/relval/webpage/relval_stats/"+userid+ ".txt >> "+fname_stats)

    #print (calendar.timegm(datetime.datetime.now().timetuple())-calendar.timegm(batch[8].utctimetuple()))/60/60/24


os.system("echo \"|number of workflows announced | \" `cat "+fname_wfs+" | wc -l` \"|\"")

os.system("for file in `cat "+fname_stats+"`; do cat $file | grep \"|/\" | grep \"/RECO[^-]\|/GEN-SIM-RECO[^-]\|/GEN-SIM-DIGI-RECO[^-]\" | awk '{EVTS+=$3}END{printf(\"total events: %d\\n\",EVTS)}'; done | awk '{EVTS+=$3}END{printf(\"|number of !RECO, GEN-SIM-RECO, and GEN-SIM-DIGI-RECO events announced | %d |\\n\",EVTS)}'")

os.system("for file in `cat "+fname_stats+"`; do cat $file | grep \"|/\" | grep \"/GEN-SIM[^-]\" | awk '{EVTS+=$3}END{printf(\"total events: %d\\n\",EVTS)}'; done | awk '{EVTS+=$3}END{printf(\"|number of GEN-SIM events announced | %d |\\n\",EVTS)}'")

