import MySQLdb
import sys
import os
import time
import datetime

dbname = "relval"

while True:

    conn = MySQLdb.connect(host='localhost', user='relval', passwd='relval')
    
    curs = conn.cursor()
    
    curs.execute("use "+dbname+";")
    
    #workflow = line.rstrip('\n')  
    #curs.execute("insert into workflows set hn_req=\""+hnrequest+"\", workflow_name=\""+workflow+"\";")

    curs.execute("select * from batches")
    batches=curs.fetchall()
    
    for batch in batches:

        #print batch
        print batch[0]
        print batch[1]
        print batch[2]
        print batch[3]
        print batch[4]
        print batch[5]
        print batch[6]
        print batch[7]
        print batch[8]
        print ""

        if batch[7] == "approved":
            
            print "assigning workflows in batch "+str(batch[0])
            
            curs.execute("select workflow_name from workflows where batch_id = "+ str(batch[0])+";")
            wfs=curs.fetchall()
            
            for wf in wfs:
                print wf[0]
                os.system("python2.6 assignRelValWorkflow.py -w "+wf[0] +" -s "+batch[5]+" -p "+str(batch[6]))
                time.sleep(30)


            curs.execute("update batches set status=\"assigned\", current_status_start_time=\""+datetime.datetime.now().strftime("%y:%m:%d %H:%M:%S")+"\" where batch_id = "+str(batch[0]) +";")    

    time.sleep(100)
