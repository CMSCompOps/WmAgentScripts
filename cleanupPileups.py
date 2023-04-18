from RucioClient import RucioClient

import json
from collections import OrderedDict

# Opening JSON file
f = open('campaigns.json')

campaigns = json.load(f)

rucioClient = RucioClient()

inconsistency_count = 0

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

            pileup_locations_on_rucio_wmcore_transferor = rucioClient.getDatasetLocationsByAccountAsRSEs(secondaryName, "wmcore_transferor")
            pileup_locations_on_rucio_transfer_ops = rucioClient.getDatasetLocationsByAccountAsRSEs(secondaryName,
                                                                                       "transfer_ops")

            if set(pileup_locations_on_rucio_wmcore_transferor) != set(secondaryLocations):
                inconsistency_count += 1
                print("Inconsistency of pileup between Rucio and campaign config")
                print(campaignName)
                print(secondaryName)
                print("On campaign config:", str(secondaryLocations) )
                print("On Rucio by wmcore_transferor:", str(pileup_locations_on_rucio_wmcore_transferor))
                print("On Rucio by transfer_ops:", str(pileup_locations_on_rucio_transfer_ops))
                print("")








#


#