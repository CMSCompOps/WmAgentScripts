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

    colnames = [desc[0] for desc in curs.description]
    
    for batch in batches:

        #if batch[0] != 21:
        #    continue

        blocks_dsets_to_transfer=[]
        blocks_not_at_site=[]

        for name, value in zip(colnames, batch):
            if name=="status":
                status=value
            elif name == "useridyear":
                useridyear=value
            elif name == "useridmonth":
                useridmonth=value
            elif name == "useridday":
                useridday=value
            elif name == "useridnum":
                useridnum=value
            elif name == "batch_version_num":
                batch_version_num=value
            elif name == "site":
                site=value
            elif name == "processing_version":
                processing_version=value
            elif name == "hn_message_id":
                hn_message_id=value
            elif name == "announcement_title":
                title=value
            elif name == "site":
                site=value

        if "T2" in site:
            site_disk = site
        elif "T1" in site:
            site_disk = site + "_Disk"
        else:
            os.system('echo '+site+' | mail -s \"input_dset_checker error 1\" andrew.m.levin@vanderbilt.edu')
            print "Neither T1 nor T2 is in site name, exiting"
            sys.exit(1)

        if site == "T2_CH_CERN_T0":
            site_disk = "T2_CH_CERN"

        #print batch
        #print ""

        userid = useridyear+"_"+useridmonth+"_"+useridday+"_"+str(useridnum)+"_"+str(batch_version_num)    

        #if status == "waiting_for_transfer" and count % 10 == 0:        

        if status == "waiting_for_transfer":        

            print "    userid ==> "+str(userid)

            #count = 0

            all_dsets_blocks_at_site=True

            curs.execute("select workflow_name from workflows where useridyear = \""+ useridyear+"\" and useridmonth = \""+useridmonth+"\" and useridday = \""+useridday+"\" and useridnum = "+str(useridnum)+" and batch_version_num = "+str(batch_version_num)+";")
            wfs=curs.fetchall()

            for wf in wfs:
                print wf[0]

                conn  =  httplib.HTTPSConnection('cmsweb.cern.ch', cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
                r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+wf[0])
                r2=conn.getresponse()

                schema = json.loads(r2.read())

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
                            
                                    block = line.rstrip('\n')

                                    isblockatsite = utils.checkIfBlockIsAtASite("cmsweb.cern.ch",block,site_disk)

                                    #this block (/DoubleMu/...) is not registered in phedex, so it cannot be subscribed to any site
                                    if not isblockatsite and block != "/DoubleMu/Run2011A-ZMu-08Nov2011-v1/RAW-RECO#93c53d22-25b2-11e1-8c62-003048f02c8a":
                                        all_dsets_blocks_at_site=False

                            else:   

                                isdatasetatsite=utils.checkIfDatasetIsSubscribedToASite("cmsweb.cern.ch",inputdset,site_disk)

                                if not isdatasetatsite:
                                    all_dsets_blocks_at_site=False

            if all_dsets_blocks_at_site and (not isthereanmcpileupdataset or ismcpileupdatasetatsite):
                curs.execute("update batches set status=\"input_dsets_ready\", current_status_start_time=\""+datetime.datetime.now().strftime("%y:%m:%d %H:%M:%S")+"\" where useridyear = \""+ useridyear+"\" and useridmonth = \""+useridmonth+"\" and useridday = \""+useridday+"\" and useridnum = "+str(useridnum)+" and batch_version_num = "+str(batch_version_num)+";")
                mysqlconn.commit()

        if status == "approved":

            print "    userid ==> "+str(userid)

            #print "checking input datasets for workflows in batch "+str(batchid)
            
            curs.execute("select workflow_name from workflows where useridyear = \""+ useridyear+"\" and useridmonth = \""+useridmonth+"\" and useridday = \""+useridday+"\" and useridnum = "+str(useridnum)+" and batch_version_num = "+str(batch_version_num)+";")
            wfs=curs.fetchall()
            
            for wf in wfs:

                print wf[0]

                conn  =  httplib.HTTPSConnection('cmsweb.cern.ch', cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
                r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+wf[0])
                r2=conn.getresponse()

                schema = json.loads(r2.read())

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

                result=utils.makeReplicaRequest("cmsweb.cern.ch", site_disk, blocks_dsets_to_transfer, "relval datasets")

                phedexid = result['phedex']['request_created'][0]['id']

                utils.approveSubscription("cmsweb.cern.ch",phedexid)

                curs.execute("update batches set status=\"waiting_for_transfer\", current_status_start_time=\""+datetime.datetime.now().strftime("%y:%m:%d %H:%M:%S")+"\" where useridyear = \""+ useridyear+"\" and useridmonth = \""+useridmonth+"\" and useridday = \""+useridday+"\" and useridnum = "+str(useridnum)+" and batch_version_num = "+str(batch_version_num)+";")
                mysqlconn.commit()
            elif blocks_not_at_site != []:
                print blocks_not_at_site
                curs.execute("update batches set status=\"waiting_for_transfer\", current_status_start_time=\""+datetime.datetime.now().strftime("%y:%m:%d %H:%M:%S")+"\" where useridyear = \""+ useridyear+"\" and useridmonth = \""+useridmonth+"\" and useridday = \""+useridday+"\" and useridnum = "+str(useridnum)+" and batch_version_num = "+str(batch_version_num)+";")
            else:    
                curs.execute("update batches set status=\"input_dsets_ready\", current_status_start_time=\""+datetime.datetime.now().strftime("%y:%m:%d %H:%M:%S")+"\" where useridyear = \""+ useridyear+"\" and useridmonth = \""+useridmonth+"\" and useridday = \""+useridday+"\" and useridnum = "+str(useridnum)+" and batch_version_num = "+str(batch_version_num)+";")
                mysqlconn.commit()

    #count = count+1            

    #curs.execute("unlock tables")

    #time.sleep(100)
    #sys.exit(0)

if __name__ == "__main__":
    main()
