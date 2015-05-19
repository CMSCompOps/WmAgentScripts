import MySQLdb
import sys
import os
import httplib
import json
import optparse
import datetime
import time
import calendar

url='cmsweb.cern.ch'

dbname = "relval"

while True:

    os.system("if [ -f batches.html ]; then rm batches.html; fi")
    
    sys.stdout = open('batches.html','a')

    print "<html>"
    print "<head><title>batches</title>"
    print "</head>"
    print "<body>"



    print "last update = " + str(datetime.datetime.now())
    print "<br>"

    print "<form action=\"http://localhost:50000/cgi-bin/handle_POST_2.py\" method=\"post\">"

    conn = MySQLdb.connect(host='dbod-altest1.cern.ch', user='relval', passwd="relval", port=5505)

    curs = conn.cursor()
    
    curs.execute("use "+dbname+";")
    
    #workflow = line.rstrip('\n')
    #curs.execute("insert into workflows set hn_req=\""+hnrequest+"\", workflow_name=\""+workflow+"\";")

    curs.execute("select * from batches order by batch_id")
    batches=curs.fetchall()

    for batch in batches:

        colnames = [desc[0] for desc in curs.description]
        
        for name, value in zip(colnames, batch):
            if name=="status":
                status=value
            elif name == "batch_id":
                batchid=value;
            elif name == "announcement_title":
                title=value;
            



        if status != "inserted":
            continue
        print "<br>"        
        print "batch "+str(batchid)+": "+str(title)+"<br>"
        print "<input  name='batch"+str(batchid)+"' value='approve' type='radio'/> approve <input  name='batch"+str(batchid)+"' value='disapprove' type='radio'/> disapprove <input checked='checked' name='batch"+str(batchid)+"' value='null' type='radio'/> do nothing<br/>"

    print "<br>"    
    print "<input type=\"submit\" value=\"Submit\"/>"

    print "</form>"
    print "</body>"
    print "</html>"
        
    sys.stdout.flush()
    
    #os.system("scp relval_monitor.txt relval@vocms174.cern.ch:~/webpage/")

    sys.exit(0)
    time.sleep(60)


