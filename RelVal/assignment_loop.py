import smtplib
import email

from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.Utils import COMMASPACE, formatdate


import MySQLdb
import sys
import os
import time
import datetime

dbname = "relval"

while True:
    
    conn = MySQLdb.connect(host='dbod-altest1.cern.ch', user='relval', passwd="relval", port=5505)
    #conn = MySQLdb.connect(host='localhost', user='relval', passwd='relval')
    
    curs = conn.cursor()
    
    curs.execute("use "+dbname+";")
    
    #workflow = line.rstrip('\n')  
    #curs.execute("insert into workflows set hn_req=\""+hnrequest+"\", workflow_name=\""+workflow+"\";")

    curs.execute("select * from batches")
    batches=curs.fetchall()

    colnames = [desc[0] for desc in curs.description]
    
    for batch in batches:

        for name, value in zip(colnames, batch):
            print name+" => "+str(value)
            if name=="status":
                status=value
            elif name == "batch_id":
                batchid=value
            elif name == "site":
                site=value
            elif name == "processing_version":
                processing_version=value
            elif name == "hn_message_id":
                hn_message_id=value
            elif name == "announcement_title":
                title=value


        #print batch
        print ""

        if status == "approved":
            
            print "assigning workflows in batch "+str(batchid)
            
            curs.execute("select workflow_name from workflows where batch_id = "+ str(batchid)+";")
            wfs=curs.fetchall()
            
            for wf in wfs:
                print wf[0]
                os.system("python2.6 assignRelValWorkflow.py -w "+wf[0] +" -s "+site+" -p "+str(processing_version))
                time.sleep(30)


            curs.execute("update batches set status=\"assigned\", current_status_start_time=\""+datetime.datetime.now().strftime("%y:%m:%d %H:%M:%S")+"\" where batch_id = "+str(batchid) +";")    

            conn.commit()

            msg = MIMEMultipart()
            reply_to = []
            #send_to = ["andrew.m.levin@vanderbilt.edu"]
            send_to = ["hn-cms-dataopsrequests@cern.ch","andrew.m.levin@vanderbilt.edu"]
            #send_to = ["hn-cms-hnTest@cern.ch"]

            msg['In-Reply-To'] = hn_message_id
            msg['References'] = hn_message_id

            msg['From'] = "amlevin@mit.edu"
            msg['reply-to'] = COMMASPACE.join(reply_to)
            msg['To'] = COMMASPACE.join(send_to)
            msg['Date'] = formatdate(localtime=True)
            msg['Subject'] = title
            msg['Message-ID'] = email.Utils.make_msgid()
    

            messageText="Dear all,\n"
            messageText=messageText+"\n"
            messageText=messageText+"This batch has been assigned.\n"
            messageText=messageText+"\n"
            messageText=messageText+"RelVal Batch Manager"

            try:
                msg.attach(MIMEText(messageText))
                smtpObj = smtplib.SMTP()
                smtpObj.connect()
                smtpObj.sendmail("amlevin@mit.edu", send_to, msg.as_string())
                smtpObj.close()
            except Exception as e:
                print "Error: unable to send email: %s" %(str(e))


    time.sleep(100)
