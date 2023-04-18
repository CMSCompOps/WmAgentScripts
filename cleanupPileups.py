from RucioClient import RucioClient

import json
from collections import OrderedDict

# Opening JSON file
f = open('campaigns.json')

campaigns = json.load(f)

for campaignName, v in list(campaigns.items()):
    print (campaignName)
    if "secondaries" in v:
        for secondaryName, locationsDict in list(v["secondaries"].items()):
            print (secondaryName)
            print (locationsDict)





#rucioClient = RucioClient()


#pileup_locations = rucioClient.getDatasetLocationsByAccount(sec, "wmcore_transferor")