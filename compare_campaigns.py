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


print ("Difference justin - hasan")
for pileupName, locationsDict in list(justin_map.items()):
    if pileupName not in hasan_map:
        print ("Justin has it, Hasan doesn't", pileupName)
    elif hasan_map[pileupName] != locationsDict:
        print("Both has it, but locations are different", pileupName)
        print("Justin:", str(locationsDict["SecondaryLocations"]))
        print("Hasan:", str(hasan_map[pileupName]["SecondaryLocations"]))

print("")
print ("Difference hasan - justin")
for pileupName, locationsDict in list(hasan_map.items()):
    if pileupName not in justin_map:
        print ("Hasan has it, Justin doesn't", pileupName)
    elif justin_map[pileupName] != locationsDict:
        print("Both has it, but locations are different", pileupName)
        print("Hasan:", str(locationsDict["SecondaryLocations"]))
        print("Justin:", str(justin_map[pileupName]["SecondaryLocations"]))







#


#