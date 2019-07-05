import sys
import os
import socket
import json
import optparse
from json import JSONEncoder


parser = optparse.OptionParser()
parser.add_option('-n','--name',help='campaign name to be removed')
(options,args) = parser.parse_args()

if not options.name:
	print "enter a campaign name as parameter -n <campaign name>"
	sys.exit(1)
p=0
with open('campaigns.json','r') as campaigns:
	campaign_db = json.load(campaigns)
        for key,value in campaign_db.items():
		if key == options.name:
                        # remove the campaign from the mongoDB
	            	remove_from_campaign_config = 'python campaignsConfiguration.py --name ' + options.name	+ ' --remove'
	            	os.system(remove_from_campaign_config)
			with open('archive_campaigns.json','r') as outfile:
				destination = json.load(outfile)	
                        	destination.update({key : value})
                        	with open('archive_campaigns.json', 'w') as out:
                            		json.dump(destination, out, indent=2) #putting the campaign into archive_campaigns.json file
                            		with open('campaigns.json','w') as Origin:
                                		del campaign_db[key] #deleting the campaign from campaigns.json file
			        		Origin.write(json.dumps(campaign_db, indent=2))
                                                sys.exit(1)
                else:
			p=1

if p is 1:
	print "campaign name doesn't exists."

