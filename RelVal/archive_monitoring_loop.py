import MySQLdb
import sys
import os
import httplib
import json
import optparse
import datetime
import time
import calendar

parser = optparse.OptionParser()
parser.add_option('--correct_env',action="store_true",dest='correct_env')
parser.add_option('--sent_to_hn',action="store_true",dest='send_to_hn')
(options,args) = parser.parse_args()

command=""
for arg in sys.argv:
    command=command+arg+" "

if not options.correct_env:
    os.system("source /afs/cern.ch/project/gd/LCG-share/current_3.2/etc/profile.d/grid-env.sh; python2.6 "+command + "--correct_env")
    sys.exit(0)
    
url='cmsweb.cern.ch'


dbname = "relval"

while True:

    os.system("if [ -f relval_archive_monitor.txt ]; then rm relval_archive_monitor.txt; fi")
    
    sys.stdout = open('relval_archive_monitor.txt','a')

    print "last update = " + str(datetime.datetime.now())

    conn = MySQLdb.connect(host='localhost', user='relval', passwd='relval')

    curs = conn.cursor()
    
    curs.execute("use "+dbname+";")
    
    #workflow = line.rstrip('\n')
    #curs.execute("insert into workflows set hn_req=\""+hnrequest+"\", workflow_name=\""+workflow+"\";")

    curs.execute("select * from batches_archive order by batch_id")
    batches=curs.fetchall()


    for batch in batches:
        print "    batch "+str(batch[0])
        print "        hypernews request: "+str(batch[1])
        print "        processing version: "+str(batch[6])
        print "        status = " + str(batch[7])
        print "        announcement e-mail title = " + str(batch[3])

    sys.stdout.flush()    
    os.system("scp relval_archive_monitor.txt relval@vocms174.cern.ch:~/webpage/")

    sys.exit(0)
    time.sleep(60)

        
