import httplib
import json
import phedexClient

import MySQLdb
import sys
import os
import time
import datetime

import utils

dbname = "relval"

#it is probably too expensive to retrieve the information from phedex every 100 seconds
#count= 0

def main():

    mysqlconn = MySQLdb.connect(host='dbod-cmsrv1.cern.ch', user='relval', passwd="relval", port=5506)
    #conn = MySQLdb.connect(host='localhost', user='relval', passwd='relval')
    
    curs = mysqlconn.cursor()
    
    curs.execute("use "+dbname+";")

    #curs.execute("lock tables batches write, batches_archive write, workflows write, workflows_archive write, datasets write, clone_reinsert_requests write")

    #workflow = line.rstrip('\n')  
    #curs.execute("insert into workflows set hn_req=\""+hnrequest+"\", workflow_name=\""+workflow+"\";")

    curs.execute("select * from batches")
    batches=curs.fetchall()

    batches_colnames = [desc[0] for desc in curs.description]
    
    for batch in batches:

        #if batch[0] != 21:
        #    continue

        blocks_dsets_to_transfer=[]
        blocks_not_at_site=[]

        batch_dict = dict(zip(batches_colnames, batch))

        site = batch_dict["site"]

        if "T2" in site:
            site_disk = site
        elif "T1" in site:
            site_disk = site + "_Disk"
        else:
            os.system('echo '+site+' | mail -s \"input_dset_checkor.py error 1\" andrew.m.levin@vanderbilt.edu')
            print "Neither T1 nor T2 is in site name, exiting"
            sys.exit(1)

        if site == "T2_CH_CERN_T0":
            site_disk = "T2_CH_CERN"

        if site == "T2_CH_CERN_AI":
            site_disk = "T2_CH_CERN"

        #print batch
        #print ""

        userid = batch_dict["useridyear"]+"_"+batch_dict["useridmonth"]+"_"+batch_dict["useridday"]+"_"+str(batch_dict["useridnum"])+"_"+str(batch_dict["batch_version_num"])    

        #if status == "waiting_for_transfer" and count % 10 == 0:        

        if batch_dict["status"] == "waiting_for_transfer":        

            print "    userid ==> "+str(userid)

            #count = 0

            all_dsets_blocks_at_site=True

            curs.execute("select workflow_name from workflows where useridyear = \""+ batch_dict["useridyear"]+"\" and useridmonth = \""+batch_dict["useridmonth"]+"\" and useridday = \""+batch_dict["useridday"]+"\" and useridnum = "+str(batch_dict["useridnum"])+" and batch_version_num = "+str(batch_dict["batch_version_num"])+";")
            wfs=curs.fetchall()

            for wf in wfs:

                print wf[0]

                headers = {"Content-type": "application/json", "Accept": "application/json"}

                conn  =  httplib.HTTPSConnection('cmsweb.cern.ch', cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
                r1=conn.request("GET",'/reqmgr2/data/request/'+wf[0],headers=headers)
                r2=conn.getresponse()

                schema = json.loads(r2.read())

                schema = schema['result'][0][wf[0]]

                isthereanmcpileupdataset=False

                for key, value in schema.items():
                    if type(value) is dict and key.startswith("Task"):

                        if 'MCPileup' in value:
                            isthereanmcpileupdataset=True
                            ismcpileupdatasetatsite=utils.checkIfDatasetIsSubscribedToASite("cmsweb.cern.ch",value["MCPileup"],site_disk)
                            

                        if 'InputDataset' in value:

                            inputdset=value['InputDataset']
                             
                            if 'RunWhitelist' in value:

                                runwhitelist=value['RunWhitelist']
                                blocks_fname=os.popen("mktemp").read().rstrip("\n")
                                 
                                list_of_blocks=utils.getListOfBlocks(inputdset,str(runwhitelist))

                                for block in list_of_blocks:

                                    #this block (/DoubleMu/...) is not registered in phedex, so it cannot be subscribed to any site
                                    if block ==  "/DoubleMu/Run2011A-ZMu-08Nov2011-v1/RAW-RECO#93c53d22-25b2-11e1-8c62-003048f02c8a":
                                        continue

                                    isblockatsite = utils.checkIfBlockIsAtASite("cmsweb.cern.ch",block,site_disk)

                                    if not isblockatsite:
                                        all_dsets_blocks_at_site=False

                            else:   

                                isdatasetatsite=utils.checkIfDatasetIsSubscribedToASite("cmsweb.cern.ch",inputdset,site_disk)

                                if not isdatasetatsite:
                                    all_dsets_blocks_at_site=False

            if all_dsets_blocks_at_site and (not isthereanmcpileupdataset or ismcpileupdatasetatsite):
                curs.execute("update batches set status=\"input_dsets_ready\", current_status_start_time=\""+datetime.datetime.now().strftime("%y:%m:%d %H:%M:%S")+"\" where useridyear = \""+ batch_dict["useridyear"]+"\" and useridmonth = \""+batch_dict["useridmonth"]+"\" and useridday = \""+batch_dict["useridday"]+"\" and useridnum = "+str(batch_dict["useridnum"])+" and batch_version_num = "+str(batch_dict["batch_version_num"])+";")
                mysqlconn.commit()

        if batch_dict["status"] == "approved":

            print "    userid ==> "+str(userid)

            #print "checking input datasets for workflows in batch "+str(batchid)
            
            curs.execute("select workflow_name from workflows where useridyear = \""+ batch_dict["useridyear"]+"\" and useridmonth = \""+batch_dict["useridmonth"]+"\" and useridday = \""+batch_dict["useridday"]+"\" and useridnum = "+str(batch_dict["useridnum"])+" and batch_version_num = "+str(batch_dict["batch_version_num"])+";")
            wfs=curs.fetchall()
            
            for wf in wfs:

                print wf[0]

                headers = {"Content-type": "application/json", "Accept": "application/json"}

                conn  =  httplib.HTTPSConnection('cmsweb.cern.ch', cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
                r1=conn.request("GET",'/reqmgr2/data/request/'+wf[0],headers=headers)
                r2=conn.getresponse()

                schema = json.loads(r2.read())

                schema = schema['result'][0][wf[0]]

                for key, value in schema.items():
                    if type(value) is dict and key.startswith("Task"):

                        if 'MCPileup' in value:
                            isdatasetatsite=utils.checkIfDatasetIsSubscribedToASite("cmsweb.cern.ch",value['MCPileup'],site_disk)

                            if not isdatasetatsite:
                                blocks_dsets_to_transfer.append(value['MCPileup'])

                        if 'InputDataset' in value:

                             subscribed_to_disk=False
                             
                             inputdset=value['InputDataset']
                             
                             if 'RunWhitelist' in value:

                                 runwhitelist=value['RunWhitelist']

                                 list_of_blocks=utils.getListOfBlocks(inputdset,str(runwhitelist))

                                 for block in list_of_blocks:

                                     #this block (/DoubleMu/...) is not registered in phedex, so it cannot be subscribed to any site
                                     if block == "/DoubleMu/Run2011A-ZMu-08Nov2011-v1/RAW-RECO#93c53d22-25b2-11e1-8c62-003048f02c8a":
                                         continue

                                     isblocksubscribedtosite=utils.checkIfBlockIsSubscribedToASite("cmsweb.cern.ch",block,site_disk)
                                     isblockatsite=utils.checkIfBlockIsAtASite("cmsweb.cern.ch",block,site_disk)

                                     if not isblocksubscribedtosite:
                                         blocks_dsets_to_transfer.append(block)
                                     if not isblockatsite:
                                         blocks_not_at_site.append(block)

                             else:   
                                 isdatasetsubscribedtosite=utils.checkIfDatasetIsSubscribedToASite("cmsweb.cern.ch",inputdset,site_disk)
                                 isdatasetatsite=utils.checkIfDatasetIsSubscribedToASite("cmsweb.cern.ch",inputdset,site_disk)

                                 if not isdatasetsubscribedtosite:
                                     blocks_dsets_to_transfer.append(inputdset)
                                 if not isdatasetatsite:
                                     blocks_not_at_site.append(inputdset)
                                         

                                     
            
            if blocks_dsets_to_transfer != []:                    

                print "transfering the following blocks:"

                print blocks_dsets_to_transfer

                result=utils.makeReplicaRequest(url="cmsweb.cern.ch", site=site_disk, datasets=blocks_dsets_to_transfer, comments="relval datasets", group = "RelVal")

                phedexid = result['phedex']['request_created'][0]['id']

                utils.approveSubscription("cmsweb.cern.ch",phedexid)

                curs.execute("update batches set status=\"waiting_for_transfer\", current_status_start_time=\""+datetime.datetime.now().strftime("%y:%m:%d %H:%M:%S")+"\" where useridyear = \""+ batch_dict["useridyear"]+"\" and useridmonth = \""+batch_dict["useridmonth"]+"\" and useridday = \""+batch_dict["useridday"]+"\" and useridnum = "+str(batch_dict["useridnum"])+" and batch_version_num = "+str(batch_dict["batch_version_num"])+";")
                mysqlconn.commit()
            elif blocks_not_at_site != []:
                print blocks_not_at_site
                curs.execute("update batches set status=\"waiting_for_transfer\", current_status_start_time=\""+datetime.datetime.now().strftime("%y:%m:%d %H:%M:%S")+"\" where useridyear = \""+ batch_dict["useridyear"]+"\" and useridmonth = \""+batch_dict["useridmonth"]+"\" and useridday = \""+batch_dict["useridday"]+"\" and useridnum = "+str(batch_dict["useridnum"])+" and batch_version_num = "+str(batch_dict["batch_version_num"])+";")
            else:    
                curs.execute("update batches set status=\"input_dsets_ready\", current_status_start_time=\""+datetime.datetime.now().strftime("%y:%m:%d %H:%M:%S")+"\" where useridyear = \""+ batch_dict["useridyear"]+"\" and useridmonth = \""+batch_dict["useridmonth"]+"\" and useridday = \""+batch_dict["useridday"]+"\" and useridnum = "+str(batch_dict["useridnum"])+" and batch_version_num = "+str(batch_dict["batch_version_num"])+";")
                mysqlconn.commit()

    #count = count+1            

    #curs.execute("unlock tables")

    #time.sleep(100)
    #sys.exit(0)

if __name__ == "__main__":
    main()
