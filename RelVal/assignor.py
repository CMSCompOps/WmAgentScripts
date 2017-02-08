import reqMgrClient

import httplib
import json

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

import utils

import assignment

def main():

    mysqlconn = MySQLdb.connect(host='dbod-cmsrv1.cern.ch', user='relval', passwd="relval", port=5506)
    
    curs = mysqlconn.cursor()
    
    curs.execute("use "+dbname+";")

    #curs.execute("lock tables batches write, batches_archive write, workflows write, workflows_archive write, datasets write, clone_reinsert_requests write")

    curs.execute("select * from batches")
    batches=curs.fetchall()

    batches_colnames = [desc[0] for desc in curs.description]
    
    for batch in batches:
        #for name, value in zip(batches_colnames, batch):
        #    print name+" => "+str(value)

        batch_dict=dict(zip(batches_colnames,batch))    

        userid = batch_dict["useridyear"]+"_"+batch_dict["useridmonth"]+"_"+batch_dict["useridday"]+"_"+str(batch_dict["useridnum"])+"_"+str(batch_dict["batch_version_num"])

        if batch_dict["status"] == "input_dsets_ready":

            print "    userid => "+userid

            curs.execute("select workflow_name from workflows where useridyear = \""+batch_dict["useridyear"]+"\" and useridmonth = \""+batch_dict["useridmonth"]+"\" and useridday = \""+batch_dict["useridday"]+"\" and useridnum = "+str(batch_dict["useridnum"])+" and batch_version_num = "+str(batch_dict["batch_version_num"])+";")
            wfs=curs.fetchall()

            #first do checks to make sure the workflows do not write into an existing dataset
            for wf in wfs:

                conn  =  httplib.HTTPSConnection('cmsweb.cern.ch', cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))

                headers = {"Content-type": "application/json", "Accept": "application/json"}

                r1=conn.request("GET",'/reqmgr2/data/request/'+wf[0], headers = headers)
                r2=conn.getresponse()

                schema = (json.loads(r2.read()))

                schema = schema['result']

                if len(schema) != 1:
                    os.system('echo '+wf[0]+' | mail -s \"assignor.py error 9\" andrew.m.levin@vanderbilt.edu')
                    sys.exit(1)

                schema = schema[0]

                schema = schema[wf[0]]

                #if schema['RequestTransition'][len(schema['RequestTransition'])-1]['Status'] != "assignment-approved":
                #    continue

                for key, value in schema.items():
                    if key == "ProcessingString":
                        procstring_main = value
                        

                for key, value in schema.items():
                    if type(value) is dict and key.startswith("Task"):
                        if ('KeepOutput' in value and value['KeepOutput']) or 'KeepOutput' not in value:
                            if 'InputDataset' in value:

                                if 'AcquisitionEra' not in value:
                                    os.system('echo \"'+wf[0]+'\" | mail -s \"assignor.py error 10\" andrew.m.levin@vanderbilt.edu')
                                    sys.exit(1)

                                if 'ProcessingString' in value:
                                    procstring = value['ProcessingString']
                                elif "procstring_main" in vars():
                                    procstring = procstring_main
                                else:
                                    os.system('echo \"'+wf[0]+'\" | mail -s \"assignor.py error 11\" andrew.m.levin@vanderbilt.edu')
                                    sys.exit(1)
                                    
                                dset="/" + value['InputDataset'].split('/')[1] + "/" + value['AcquisitionEra'] + "-" + procstring + "-v" + str(batch_dict["processing_version"])+"/*"

                                curs.execute("select * from datasets where dset_name = \""+ dset.rstrip("*")+"\";")

                                dbs_dset_check=utils.getDatasets(dset)
                                
                                curs_fetchall = curs.fetchall()

                                if len(curs_fetchall) != 0:
                                    dsets_colnames = [desc[0] for desc in curs.description]
                                    dset_dict=dict(zip(dsets_colnames,curs_fetchall[0]))    
                                    userid_previously_inserted_dset=dset_dict["useridyear"]+"_"+dset_dict["useridmonth"]+"_"+dset_dict["useridday"]+"_"+str(dset_dict["useridnum"])+"_"+str(dset_dict["batch_version_num"])
                                    os.system('echo \"'+userid+"\n"+wf[0]+"\n"+userid_previously_inserted_dset+"\n"+dset_dict["workflow_name"]+"\n"+dset+'\" | mail -s \"assignor.py error 1\" andrew.m.levin@vanderbilt.edu')
                                    sys.exit(1)
                                elif len(dbs_dset_check) != 0:    
                                    os.system('echo \"'+userid+"\n"+wf[0]+"\n"+dset+'\" | mail -s \"assignor.py error 5\" andrew.m.levin@vanderbilt.edu')
                                    sys.exit(1)
                                else:   

                                    curs.execute("insert into datasets set dset_name=\""+dset.rstrip("*")+"\", workflow_name=\""+wf[0]+"\", useridyear = \""+batch_dict["useridyear"]+"\", useridmonth = \""+batch_dict["useridmonth"]+"\", useridday = \""+batch_dict["useridday"]+"\", useridnum = "+str(batch_dict["useridnum"])+", batch_version_num = "+str(batch_dict["batch_version_num"])+";")

                                    
                            elif 'PrimaryDataset' in value:

                                dset="/" + value['PrimaryDataset'] + "/" + value['AcquisitionEra'] + "-" + value['ProcessingString'] + "-v" + str(batch_dict["processing_version"])+"/*"
                                curs.execute("select * from datasets where dset_name = \""+ dset.rstrip("*")+"\";")

                                curs_fetchall = curs.fetchall()

                                dbs_dset_check=utils.getDatasets(dset)

                                if len(curs_fetchall) != 0:
                                    dsets_colnames = [desc[0] for desc in curs.description]
                                    dset_dict=dict(zip(dsets_colnames,curs_fetchall[0]))    
                                    userid_previously_inserted_dset=dset_dict["useridyear"]+"_"+dset_dict["useridmonth"]+"_"+dset_dict["useridday"]+"_"+str(dset_dict["useridnum"])+"_"+str(dset_dict["batch_version_num"])
                                    os.system('echo \"'+userid+"\n"+wf[0]+"\n"+userid_previously_inserted_dset+"\n"+dset_dict["workflow_name"]+"\n"+dset+'\" | mail -s \"assignor.py error 2\" andrew.m.levin@vanderbilt.edu')
                                    sys.exit(1)
                                elif len(dbs_dset_check) != 0:    
                                    os.system('echo \"'+userid+"\n"+wf[0]+'\" | mail -s \"assignor.py error 7\" andrew.m.levin@vanderbilt.edu')
                                    sys.exit(1)
                                else:
                                    curs.execute("insert into datasets set dset_name=\""+dset.rstrip("*")+"\", workflow_name=\""+wf[0]+"\", useridyear = "+batch_dict["useridyear"]+", useridmonth = "+batch_dict["useridmonth"]+", useridday = "+batch_dict["useridday"]+", useridnum = "+str(batch_dict["useridnum"])+", batch_version_num = "+str(batch_dict["batch_version_num"])+";")

            #only assign the workflows after all of the checks are done                        
            for wf in wfs:

                conn  =  httplib.HTTPSConnection('cmsweb.cern.ch', cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))

                headers = {"Content-type": "application/json", "Accept": "application/json"}

                r1=conn.request("GET",'/reqmgr2/data/request/'+wf[0], headers = headers)
                r2=conn.getresponse()

                if r2.status != 200:
                    time.sleep(30)
                    conn  =  httplib.HTTPSConnection('cmsweb.cern.ch', cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
                    r1=conn.request("GET",'/reqmgr2/data/request/'+wf[0], headers = headers)
                    r2=conn.getresponse()
                    if r2.status != 200:
                        os.system('echo '+wf[0]+' | mail -s \"assignor.py error 8\" andrew.m.levin@vanderbilt.edu')
                        sys.exit(0)

                schema = json.loads(r2.read())

                schema = schema['result']

                if len(schema) != 1:
                    os.system('echo '+wf[0]+' | mail -s \"assignor.py error 9\" andrew.m.levin@vanderbilt.edu')
                    sys.exit(1)

                schema = schema[0]

                schema = schema[wf[0]]

                if schema['RequestTransition'][len(schema['RequestTransition'])-1]['Status'] != "assignment-approved":
                    continue

                #hack because workflows assigned to only T2_CH_CERN_T0 never get acquired
                site = batch_dict["site"]
                #if site == "T2_CH_CERN_T0":
                #    site = ["T2_CH_CERN","T2_CH_CERN_T0"]

                params = assignment.make_assignment_params(schema,site,batch_dict["processing_version"])                    

                result=reqMgrClient.assignWorkflow("cmsweb.cern.ch", wf[0], "relval", params)

                if result != True:
                    os.system('echo '+wf[0]+' | mail -s \"assignor.py error 4\" andrew.m.levin@vanderbilt.edu')
                    sys.exit(0)

                time.sleep(30)

            curs.execute("update batches set status=\"assigned\", current_status_start_time=\""+datetime.datetime.now().strftime("%y:%m:%d %H:%M:%S")+"\" where useridyear = \""+batch_dict["useridyear"]+"\" and useridmonth = \""+batch_dict["useridmonth"]+"\" and useridday = \""+batch_dict["useridday"]+"\" and useridnum = "+str(batch_dict["useridnum"])+" and batch_version_num = "+str(batch_dict["batch_version_num"])+";")    

            mysqlconn.commit()

            if batch_dict["hn_message_id"] != "do_not_send_an_acknowledgement_email":

                msg = MIMEMultipart()
                reply_to = []
                send_to = ["andrew.m.levin@vanderbilt.edu","andrew.m.levin.filter1@gmail.com"]
                #send_to = ["hn-cms-dataopsrequests@cern.ch","andrew.m.levin@vanderbilt.edu"]
                #send_to = ["hn-cms-hnTest@cern.ch"]

                msg['In-Reply-To'] = batch_dict["hn_message_id"]
                msg['References'] = batch_dict["hn_message_id"]

                msg['From'] = "amlevin@mit.edu"
                msg['reply-to'] = COMMASPACE.join(reply_to)
                msg['To'] = COMMASPACE.join(send_to)
                msg['Date'] = formatdate(localtime=True)
                msg['Subject'] = batch_dict["announcement_title"]
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


    #curs.execute("unlock tables")

    #time.sleep(100)
    #sys.exit(0)


if __name__ == "__main__":
    main()
