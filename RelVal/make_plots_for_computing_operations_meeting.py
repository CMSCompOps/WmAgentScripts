import MySQLdb
import sys
import datetime
import calendar
import os

fname_stats = "computing_operations_meeting/15December2014_computing_meeting_report.txt"
fname_wfs = "computing_operations_meeting/15December2014_computing_meeting_report_wfs.txt"

dbname = "relval"

conn = MySQLdb.connect(host='localhost', user='relval', passwd="relval")

curs = conn.cursor()

curs.execute("use "+dbname+";")

curs.execute("select * from batches_archive;")
batches=curs.fetchall()

exitcode=os.system("ls "+fname_stats+" >& /dev/null")

if exitcode == 0:
    print fname_stats+" already exists, exiting"
    sys.exit(1)

exitcode=os.system("ls "+fname_wfs+" >& /dev/null")

if exitcode == 0:
    print fname_wfs+" already exists, exiting"
    sys.exit(1)

for batch in batches:
    if batch[7]!="announced":
        if batch[7]!="not_announced":
            print "batch has unexpected status \""+batch[7] +"\", exiting"
        continue

    if calendar.timegm(batch[8].utctimetuple()) < calendar.timegm(datetime.datetime(2014,12,8,14,0,0).timetuple()):
        continue

    if calendar.timegm(batch[8].utctimetuple()) > calendar.timegm(datetime.datetime(2014,12,15,14,0,0).timetuple()):
        continue    

    curs.execute("select * from workflows_archive where batch_id = \""+ str(batch[0])+"\";")
    workflows=curs.fetchall()

    for workflow in workflows:
        os.system("echo "+ workflow[1]+" >> "+fname_wfs)
    

    os.system("echo /afs/cern.ch/user/r/relval/webpage/relval_stats/"+batch[4]+ " >> "+fname_stats)

    #print (calendar.timegm(datetime.datetime.now().timetuple())-calendar.timegm(batch[8].utctimetuple()))/60/60/24


os.system("echo \"|number of workflows announced | \" `cat "+fname_wfs+" | wc -l` \"|\"")

os.system("for file in `cat "+fname_stats+"`; do cat $file | grep \"|/\" | grep \"/RECO[^-]\|/GEN-SIM-RECO[^-]\|/GEN-SIM-DIGI-RECO[^-]\" | awk '{EVTS+=$3}END{printf(\"total events: %d\\n\",EVTS)}'; done | awk '{EVTS+=$3}END{printf(\"|number of !RECO, GEN-SIM-RECO, and GEN-SIM-DIGI-RECO events announced | %d |\\n\",EVTS)}'")

os.system("for file in `cat "+fname_stats+"`; do cat $file | grep \"|/\" | grep \"/GEN-SIM[^-]\" | awk '{EVTS+=$3}END{printf(\"total events: %d\\n\",EVTS)}'; done | awk '{EVTS+=$3}END{printf(\"|number of GEN-SIM events announced | %d |\\n\",EVTS)}'")

