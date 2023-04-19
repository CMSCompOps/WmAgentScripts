from RucioClient import RucioClient

import json
from collections import OrderedDict

# Opening JSON file
f1 = open('justin_campaigns.json')
f2 = open('hasan_campaigns.json')

justin_campaigns = json.load(f1)
hasan_campaigns = json.load(f2)

rucioClient = RucioClient()

inconsistency_count = 0

justin_map = {}
hasan_map = {}


for campaignName, v in list(justin_campaigns.items()):
    if "secondaries" in v:
        for secondaryName, locationsDict in list(v["secondaries"].items()):


            secondaryLocations = []
            if "SecondaryLocation" in locationsDict:
                secondaryLocations = locationsDict["SecondaryLocation"]
            elif "SiteWhitelist" in locationsDict:
                secondaryLocations = locationsDict["SiteWhitelist"]
            else:
                print ("No location defined for the secondary, exiting")

            justin_map[secondaryName] = {
                "SecondaryLocations": secondaryLocations
            }

for campaignName, v in list(hasan_campaigns.items()):
    if "secondaries" in v:
        for secondaryName, locationsDict in list(v["secondaries"].items()):


            secondaryLocations = []
            if "SecondaryLocation" in locationsDict:
                secondaryLocations = locationsDict["SecondaryLocation"]
            elif "SiteWhitelist" in locationsDict:
                secondaryLocations = locationsDict["SiteWhitelist"]
            else:
                print ("No location defined for the secondary, exiting")

            hasan_map[secondaryName] = {
                "SecondaryLocations": secondaryLocations
            }


set1 = set(justin_map.items())
set2 = set(hasan_map.items())
print(set1 ^ set2)







#


#