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
count= 0

while True:

    mysqlconn = MySQLdb.connect(host='dbod-cmsrv1.cern.ch', user='relval', passwd="relval", port=5506)
    #conn = MySQLdb.connect(host='localhost', user='relval', passwd='relval')
    
    curs = mysqlconn.cursor()
    
    curs.execute("use "+dbname+";")
    
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

        #print batch
        #print ""

        if status == "waiting_for_transfer" and count % 10 == 0:        

            count = 0

            all_dsets_blocks_at_site=True

            curs.execute("select workflow_name from workflows where batch_id = "+ str(batchid)+";")
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
                            old_ld_library_path=os.environ['LD_LIBRARY_PATH']
                            os.environ['LD_LIBRARY_PATH']=''
                            ismcpileupdatasetatsite=os.system("python2.6 check_if_dataset_is_at_a_site.py --dataset "+value['MCPileup']+" --site "+site_disk)
                            os.environ['LD_LIBRARY_PATH']=old_ld_library_path
                            
                        if 'InputDataset' in value:

                            inputdset=value['InputDataset']
                             
                            if 'RunWhitelist' in value:
                                runwhitelist=value['RunWhitelist']
                                blocks_fname=os.popen("mktemp").read().rstrip("\n")
                                 
                                os.system("python2.6 get_list_of_blocks.py --dataset "+ inputdset  +" --runwhitelist \""+str(runwhitelist) + "\" --output_fname " + blocks_fname)
                                 
                                blocks_file = open(blocks_fname,'r')

                                #blocks=dbsApi.listBlocks(dataset = inputdset, run_num = runwhitelist)
                                for line in blocks_file:
                            
                                    block = line.rstrip('\n')
                                    old_ld_library_path=os.environ['LD_LIBRARY_PATH']
                                    os.environ['LD_LIBRARY_PATH']=''
                                    isblockatsite=os.system("python2.6 check_if_block_is_at_a_site.py --block "+block+" --site "+site_disk)
                                    os.environ['LD_LIBRARY_PATH']=old_ld_library_path


                                    #this block (/DoubleMu/...) is not registered in phedex, so it cannot be subscribed to any site
                                    if not isblockatsite and block != "/DoubleMu/Run2011A-ZMu-08Nov2011-v1/RAW-RECO#93c53d22-25b2-11e1-8c62-003048f02c8a":
                                        all_dsets_blocks_at_site=False

                            else:   

                                old_ld_library_path=os.environ['LD_LIBRARY_PATH']
                                os.environ['LD_LIBRARY_PATH']=''
                                isdatasetatsite=os.system("python2.6 check_if_dataset_is_at_a_site.py --dataset "+inputdset+" --site "+site_disk)
                                os.environ['LD_LIBRARY_PATH']=old_ld_library_path                                 
            
                                if not isdatasetatsite:
                                    all_dsets_blocks_at_site=False

            if all_dsets_blocks_at_site and (not isthereanmcpileupdataset or ismcpileupdatasetatsite):
                curs.execute("update batches set status=\"input_dsets_ready\", current_status_start_time=\""+datetime.datetime.now().strftime("%y:%m:%d %H:%M:%S")+"\" where batch_id = "+str(batchid) +";")                    
                mysqlconn.commit()

        if status == "approved":

            print "checking input datasets for workflows in batch "+str(batchid)
            
            curs.execute("select workflow_name from workflows where batch_id = "+ str(batchid)+";")
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
                            old_ld_library_path=os.environ['LD_LIBRARY_PATH']
                            os.environ['LD_LIBRARY_PATH']=''
                            isdatasetatsite=os.system("python2.6 check_if_dataset_is_at_a_site.py --dataset "+value['MCPileup']+" --site "+site_disk)
                            os.environ['LD_LIBRARY_PATH']=old_ld_library_path

                            if not isdatasetatsite:
                                blocks_dsets_to_transfer.append(value['MCPileup'])

                        if 'InputDataset' in value:

                             subscribed_to_disk=False
                             
                             inputdset=value['InputDataset']
                             
                             if 'RunWhitelist' in value:
                                 runwhitelist=value['RunWhitelist']

                                 blocks_fname=os.popen("mktemp").read().rstrip("\n")
                                 
                                 os.system("python2.6 get_list_of_blocks.py --dataset "+ inputdset  +" --runwhitelist \""+str(runwhitelist) + "\" --output_fname " + blocks_fname)
                                 
                                 blocks_file = open(blocks_fname,'r')

                                 #blocks=dbsApi.listBlocks(dataset = inputdset, run_num = runwhitelist)
                                 for line in blocks_file:
                            
                                     block = line.rstrip('\n')

                                     old_ld_library_path=os.environ['LD_LIBRARY_PATH']
                                     os.environ['LD_LIBRARY_PATH']=''
                                     isblocksubscribedtosite=os.system("python2.6 check_if_block_is_subscribed_to_a_site.py --block "+block+" --site "+site_disk)
                                     isblockatsite=os.system("python2.6 check_if_block_is_at_a_site.py --block "+block+" --site "+site_disk)
                                     os.environ['LD_LIBRARY_PATH']=old_ld_library_path
                                     
                                     #this block (/DoubleMu/...) is not registered in phedex, so it cannot be subscribed to any site
                                     if not isblocksubscribedtosite and block != "/DoubleMu/Run2011A-ZMu-08Nov2011-v1/RAW-RECO#93c53d22-25b2-11e1-8c62-003048f02c8a":
                                         blocks_dsets_to_transfer.append(block)
                                     if not isblockatsite and block != "/DoubleMu/Run2011A-ZMu-08Nov2011-v1/RAW-RECO#93c53d22-25b2-11e1-8c62-003048f02c8a":
                                         blocks_not_at_site.append(block)

                             else:   
                                 old_ld_library_path=os.environ['LD_LIBRARY_PATH']
                                 os.environ['LD_LIBRARY_PATH']=''
                                 isdatasetsubscribedtosite=os.system("python2.6 check_if_dataset_is_subscribed_to_a_site.py --dataset "+inputdset+" --site "+site_disk)
                                 isdatasetatsite=os.system("python2.6 check_if_dataset_is_at_a_site.py --dataset "+inputdset+" --site "+site_disk)
                                 os.environ['LD_LIBRARY_PATH']=old_ld_library_path                                 
            
                                 if not isdatasetsubscribedtosite:
                                     blocks_dsets_to_transfer.append(inputdset)
                                 if not isdatasetatsite:
                                     blocks_not_at_site.append(inputdset)
                                         
            
            if blocks_dsets_to_transfer != []:                    
                tmp_fname=os.popen("mktemp").read().rstrip("\n")
                tmp_file=open(tmp_fname,'w')

                result=utils.makeReplicaRequest("cmsweb.cern.ch", site_disk, blocks_dsets_to_transfer, "relval datasets")

                phedexid = result['phedex']['request_created'][0]['id']

                utils.approveSubscription("cmsweb.cern.ch",phedexid)

                #for block in blocks_dsets_to_transfer:
                #    print >> tmp_file,  block

                #tmp_file.close()



                #os.system("python2.6 phedexSubscription.py "+site_disk+" "+tmp_fname+" \\\"relval datasets\\\" --autoapprove")

                curs.execute("update batches set status=\"waiting_for_transfer\", current_status_start_time=\""+datetime.datetime.now().strftime("%y:%m:%d %H:%M:%S")+"\" where batch_id = "+str(batchid) +";")    
                mysqlconn.commit()

            elif blocks_not_at_site != []:
                print blocks_not_at_site
                curs.execute("update batches set status=\"waiting_for_transfer\", current_status_start_time=\""+datetime.datetime.now().strftime("%y:%m:%d %H:%M:%S")+"\" where batch_id = "+str(batchid) +";")    
            else:    
                curs.execute("update batches set status=\"input_dsets_ready\", current_status_start_time=\""+datetime.datetime.now().strftime("%y:%m:%d %H:%M:%S")+"\" where batch_id = "+str(batchid) +";")    
                mysqlconn.commit()

    count = count+1            
    time.sleep(100)
