from RucioClient import RucioClient

import json
from collections import OrderedDict

# Opening JSON file
f = open('campaigns.json')

campaigns = json.load(f)

rucioClient = RucioClient()

for campaignName, v in list(campaigns.items()):
    #print (campaignName)
    if "secondaries" in v:
        for secondaryName, locationsDict in list(v["secondaries"].items()):
            #print (secondaryName)
            #print (locationsDict)

            secondaryLocations = []
            if "SecondaryLocation" in locationsDict:
                secondaryLocations = locationsDict["SecondaryLocation"]
            elif "SiteWhitelist" in locationsDict:
                secondaryLocations = locationsDict["SiteWhitelist"]
            else:
                print ("No location defined for the secondary, exiting")

            pileup_locations_on_rucio = rucioClient.getDatasetLocationsByAccount(secondaryName, "wmcore_transferor")

            if set(pileup_locations_on_rucio) != set(secondaryLocations):
                print("Inconsistency of pileup between Rucio and campaign config")
                print(campaignName)
                print(secondaryName)
                print("On campaign config:", str(secondaryLocations) )
                print("On Rucio:", str(pileup_locations_on_rucio))








#


#