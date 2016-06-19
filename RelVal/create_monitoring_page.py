import MySQLdb
import os
import sys
import httplib
import json
import optparse
import datetime
import time
import calendar

url='cmsweb.cern.ch'

dbname = "relval"

def main():

    os.system("if [ -f relval_monitor_most_recent_50_batches.txt ]; then rm relval_monitor_most_recent_50_batches.txt; fi")
    
    sys.stdout = open('relval_monitor_most_recent_50_batches.txt','a')

    print "last update: " + str(datetime.datetime.now())
    print ""

    conn = MySQLdb.connect(host='dbod-cmsrv1.cern.ch', user='relval', passwd="relval", port=5506)

    curs = conn.cursor()
    
    curs.execute("use "+dbname+";")
    
    #workflow = line.rstrip('\n')
    #curs.execute("insert into workflows set hn_req=\""+hnrequest+"\", workflow_name=\""+workflow+"\";")

    curs.execute("select * from batches order by batch_creation_time")
    batches=curs.fetchall()
    colnames = [desc[0] for desc in curs.description]

    for i in range(max(len(batches)-50,0), len(batches)):

        batch=batches[len(batches)-1-i+max(len(batches) - 50,0)]

        batch_dict=dict(zip(colnames, batch))

        #curs.execute("select * from batches where useridyear = "+batch_dict["useridyear"]+" and useridmonth = "+batch_dict["useridmonth"]+" and useridday = "+batch_dict["useridday"]+" and useridnum = "+str(batch_dict["useridnum"])+" and batch_version_num = "+str(batch_dict["batch_version_num"])+";")
        #batches_with_batch_id=curs.fetchall()
        curs.execute("select workflow_name from workflows where useridyear = "+batch_dict["useridyear"]+" and useridmonth = "+batch_dict["useridmonth"]+" and useridday = "+batch_dict["useridday"]+" and useridnum = "+str(batch_dict["useridnum"])+" and batch_version_num = "+str(batch_dict["batch_version_num"])+";")
        wfs=curs.fetchall()

        formatted_description=""

        for line in batch_dict["description"].split('\n'):
            formatted_description=formatted_description+'        '+line+'\n'

        print "id: "+batch_dict["useridyear"]+"_"+batch_dict["useridmonth"]+"_"+batch_dict["useridday"]+"_"+str(batch_dict["useridnum"])+"_"+str(batch_dict["batch_version_num"])
        #for name, value in zip(colnames, batches_with_batch_id[0]):
        #    if name == "useridday" or name == "useridmonth" or name == "useridyear" or name == "useridnum" or name == "hn_message_id" or name == "batch_version_num":
        #        continue
        #    else:
        #        print '    '+name.rstrip(' ')+': '+str(value)
        print '    '+"DN: "+batch_dict["DN"]
        print '    '+"description:\n"+formatted_description.rstrip('\n')
        print '    '+"announcement_title: "+batch_dict["announcement_title"]
        print '    '+"site: "+batch_dict["site"]
        print '    '+"status: "+batch_dict["status"]
        print '    '+"current_status_start_time: "+str(batch_dict["current_status_start_time"])
        print '    '+"batch_creation_time: "+str(batch_dict["batch_creation_time"])
        

        n_workflows=0
        n_completed=0
        for wf in wfs:

            n_workflows=n_workflows+1

            conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))

            urn = '/reqmgr2/data/request?name=' + wf[0]
            getHeaders = {"Accept": "application/json"}
            for retry in range(3):

                conn.request('GET', urn, headers=getHeaders)

                r2=conn.getresponse()

                if r2.status != 200:
                    #try it again
                    time.sleep(10)
                    if retry == 2:
                        os.system('echo '+wf[0]+' | mail -s \"create_monitoring_page.py error 1\" andrew.m.levin@vanderbilt.edu')
                        sys.exit(1)
                else:
                    data = r2.read()
                    s = json.loads(data)

                    if len(s['result']) == 0:
                        os.system('echo '+wf[0]+' | mail -s \"create_monitoring_page.py error 6\" andrew.m.levin@vanderbilt.edu')
                        sys.exit(1)
                    elif wf[0] not in s['result'][0]:
                        #try it again
                        time.sleep(10)
                        if retry == 2:
                            os.system('echo '+wf[0]+' | mail -s \"create_monitoring_page.py error 7\" andrew.m.levin@vanderbilt.edu')
                            sys.exit(1)
                    else:
                        # we got our result
                        break



            #print "andrew debug 3"

            #print s['rows'][0]['doc']['request_status']
            #print len(s['rows'][0]['doc']['request_status'])
            
            if s['result'] == []:
                os.system('echo '+wf[0]+' | mail -s \"create_monitoring_page.py error 4\" andrew.m.levin@vanderbilt.edu')
                sys.exit(1)

            if wf[0] not in s['result'][0]:
                #os.system('echo \"'+str(datetime.datetime.now())+"\n\n"+wf[0]+"\n\n"+str(s['result'][0])+'\" | mail -s \"create_monitoring_page.py error 5\" andrew.m.levin@vanderbilt.edu')
                os.system('echo \"'+str(datetime.datetime.now())+"\n\n"+urn+"\n\n"+str(s['result'])+'\" | mail -s \"create_monitoring_page.py error 5\" andrew.m.levin@vanderbilt.edu')
                sys.exit(1)                

            #if len(s['result'][0][wf[0]]['RequestTransition']) == 2:
            #    print wf[0]




            for status in s['result'][0][wf[0]]['RequestTransition']:

                if status['Status'] == "failed":
                    os.system('echo '+wf[0]+' | mail -s \"create_monitoring_page.py error 3\" andrew.m.levin@vanderbilt.edu --')


                if status['Status'] == "completed":

                    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
                    #r1=conn.request('GET',"/wmstatsserver/data/isfinished/"+wf[0],headers={"Accept": "application/json"})
                    #r2=conn.getresponse()
                    #data = r2.read()
                    #s = json.loads(data)
                    #if s['result'][0] == True:
                    #    n_completed=n_completed+1
                    n_completed=n_completed+1
                    break    

        print "    n_workflows = " + str(n_workflows)
        print "    n_completed = " + str(n_completed)
        print ""
        print ""

    sys.stdout.flush()    
    ret=os.system("cp relval_monitor_most_recent_50_batches.txt /afs/cern.ch/user/r/relval/webpage/relval_monitor_most_recent_50_batches.txt")

    if ret != 0:
        os.system('echo \"'+userid+'\" | mail -s \"create_monitoring_page.py error 2\" andrew.m.levin@vanderbilt.edu')

if __name__ == "__main__":
    main()
