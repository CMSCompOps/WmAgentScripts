import MySQLdb
import sys
import datetime
import optparse
import json
import urllib2,urllib, httplib, sys, re, os

parser = optparse.OptionParser()
#parser.add_option('--correct_env',action="store_true",dest='correct_env')
(options,args) = parser.parse_args()

#command=""
#for arg in sys.argv:
#    command=command+arg+" "

#if not options.correct_env:
#    os.system("source /cvmfs/grid.cern.ch/emi-ui-3.7.3-1_sl6v2/etc/profile.d/setup-emi3-ui-example.sh; export X509_USER_PROXY=/tmp/x509up_u13536; python2.6 "+command + "--correct_env")
#    sys.exit(0)

dbname = "relval"

conn = MySQLdb.connect(host='dbod-cmsrv1.cern.ch', user='relval', passwd="relval", port=5506)
#conn = MySQLdb.connect(host='localhost', user='relval', passwd="relval")

curs = conn.cursor()

curs.execute("use "+dbname+";")

curs.execute("select * from clone_reinsert_requests;")

clone_reinsert_requests_colnames = [desc[0] for desc in curs.description]

requests_rows=curs.fetchall()

colnames = [desc[0] for desc in curs.description]

for requests_row in requests_rows:
    print requests_row

    request_dict = dict(zip(clone_reinsert_requests_colnames, requests_row))

    assert(request_dict["batch_version_num"] == 0)

    #the batch id of the new batch should be 1 more than any existing batch id
    curs.execute("select MAX(batch_version_num) from batches where useridyear=\""+request_dict["useridyear"]+"\" and useridmonth=\""+request_dict["useridmonth"]+"\" and useridday=\""+request_dict["useridday"]+"\" and useridnum = "+str(request_dict["useridnum"])+";")
    max_batch_version_num=curs.fetchall()[0][0]

    assert(max_batch_version_num != None)

    new_batch_version_num=max_batch_version_num+1

    curs.execute("select * from workflows where useridyear = \""+request_dict["useridyear"]+"\" and useridmonth = \""+request_dict["useridmonth"]+"\" and useridday = \""+request_dict["useridday"]+"\" and useridnum = "+str(request_dict["useridnum"])+" and batch_version_num = "+str(request_dict["batch_version_num"])+";")
    workflows_rows=curs.fetchall()

    workflows_colnames = [desc[0] for desc in curs.description]

    assert(len(workflows_rows) > 0)

    workflows=[]
    
    for workflow_row in workflows_rows:

        workflow_dict = dict(zip(workflows_colnames,workflow_row))

        #print workflow_row
        workflow=workflow_dict["workflow_name"]
        return_string=os.popen("python2.6 /home/relval/WmAgentScripts/RelVal/resubmit.py "+workflow+" anlevin DATAOPS").read()
        if len(return_string) == 0:
            os.system('echo '+workflow+' | mail -s \"batch_clonor.py error 1\" andrew.m.levin@vanderbilt.edu')
            sys.exit(1)
        print return_string
        workflows.append(return_string.split(' ')[len(return_string.split(' ')) - 1].rstrip('\n'))

    assert(len(workflows) == len(workflows_rows))

    curs.execute("select * from batches where useridyear = \""+request_dict["useridyear"]+"\" and useridmonth = \""+request_dict["useridmonth"]+"\" and useridday = \""+request_dict["useridday"]+"\" and useridnum = "+str(request_dict["useridnum"])+" and batch_version_num = "+str(request_dict["batch_version_num"])+";")
    batches_rows=curs.fetchall()

    batches_colnames = [desc[0] for desc in curs.description]

    assert(len(batches_rows) == 1)



    batch_dict=dict(zip(batches_colnames, batches_rows[0]))

    for line in workflows:
        workflow = line.rstrip('\n')
        if workflow == "":
            print "empty line in the file, exiting"
            sys.exit(1)
        curs.execute("select workflow_name from workflows where workflow_name=\""+ workflow +"\";")
        if len(curs.fetchall()) > 0:
            print "workflow "+workflow+" was already inserted into the database, exiting"
            sys.exit(1)

    f_index=0
    g_index=0

    #check that the workflow name contains only letters, numbers, '-' and '_' 
    for workflow in workflows:
        workflow=workflow.rstrip('\n')
        for c in workflow:
            if c != 'a' and c != 'b' and c != 'c' and c != 'd' and c != 'e' and c != 'f' and c != 'g' and c != 'h' and c != 'i' and c != 'j' and c != 'k' and c != 'l' and c != 'm' and c != 'n' and c != 'o' and c != 'p' and c != 'q' and c != 'r' and c != 's' and c != 't' and c != 'u' and c != 'v' and c != 'w' and c != 'x' and c != 'y' and c != 'z' and c != 'A' and c != 'B' and c != 'C' and c != 'D' and c != 'E' and c != 'F' and c != 'G' and c != 'H' and c != 'I' and c != 'J' and c != 'K' and c != 'L' and c != 'M' and c != 'N' and c != 'O' and c != 'P' and c != 'Q' and c != 'R' and c != 'S' and c != 'T' and c != 'U' and c != 'V' and c != 'W' and c != 'X' and c != 'Y' and c != 'Z' and c != '0' and c != '1' and c != '2' and c != '3' and c != '4' and c != '5' and c != '6' and c != '7' and c != '8' and c != '9' and c != '_' and c != '-':
                print "workflow "+workflow+" contains the character "+str(c)+" which is not allowed, exiting"
                sys.exit(0)          

    #check that no workflows are repeated in the file
    for line1 in workflows:
        workflow1 = line1.rstrip('\n')
        for line2 in workflows:
            if f_index == g_index:
                continue
                g_index=g_index+1
            workflow2 = line2.rstrip('\n')
            if workflow1 == workflow2:
                print "workflow "+ workflow1+" is repeated twice in the input file, exiting"
                sys.exit(1)
            g_index=g_index+1
        f_index=f_index+1
                
    #description = description.rstrip('\n')+"\n\n(clone of batch "+olduserid+")"

    curs.execute("insert into batches set DN=\""+batch_dict["DN"]+"\", announcement_title=\""+batch_dict["announcement_title"]+"\", processing_version="+str(request_dict["new_processing_version"])+", site=\""+request_dict["new_site"]+"\", description=\""+batch_dict["description"]+"\", status=\"approved\", useridyear=\""+request_dict["useridyear"]+"\", useridmonth=\""+request_dict["useridmonth"]+"\", useridday=\""+request_dict["useridday"]+"\", useridnum="+str(request_dict["useridnum"])+", batch_version_num = "+str(new_batch_version_num)+", hn_message_id=\"do_not_send_an_acknowledgement_email\", current_status_start_time=\""+datetime.datetime.now().strftime("%y:%m:%d %H:%M:%S")+"\", batch_creation_time = \""+datetime.datetime.now().strftime("%y:%m:%d %H:%M:%S")+"\"")

    conn.commit()

    for i in range(0,len(workflows)):
        #workflow = line.rstrip('\n')
        curs.execute("insert into workflows set useridyear = \""+request_dict["useridyear"]+"\", useridmonth = \""+request_dict["useridmonth"]+"\", useridday = \""+request_dict["useridday"]+"\", useridnum = "+str(request_dict["useridnum"])+", batch_version_num = "+str(new_batch_version_num)+", workflow_name=\""+workflows[i]+"\", original_workflow_name = \""+dict(zip(workflows_colnames,workflows_rows[i]))['workflow_name']+"\";")

    #batchid is assigned the new batch id now    
    curs.execute("delete from clone_reinsert_requests where useridyear=\""+request_dict["useridyear"]+"\" and useridmonth=\""+request_dict["useridmonth"]+"\" and useridday=\""+request_dict["useridday"]+"\" and useridnum = "+str(request_dict["useridnum"])+" and batch_version_num = "+str(request_dict["batch_version_num"])+";")
    conn.commit()
