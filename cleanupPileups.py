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
                print(str(inconsistency_count) + ". Inconsistency between campaigns.json and wmcore_transferor rules for campaign " + campaignName)
                print(secondaryName)
                print("On campaign config:", str(secondaryLocations) )
                print("On Rucio by wmcore_transferor:", str(pileup_locations_on_rucio_wmcore_transferor))

                if pileup_locations_on_rucio_wmcore_transferor == []:
                    print("ACTION: The pileup is not protected by any wmcore_transferor rules, removing it")
                    campaigns[campaignName]["secondaries"].pop()
                else:
                    print("ACTION: The pileup is protected by at least onE wmcore_transferor rule, but it doesn't match that of the campaign config. Updating the campaign config")
                    campaigns[campaignName]["secondaries"].pop()
                    campaigns[campaignName]["secondaries"] = {
                        "SecondaryLocation": pileup_locations_on_rucio_wmcore_transferor
                    }
                #if set(pileup_locations_on_rucio_wmcore_transferor) != set(pileup_locations_on_rucio_transfer_ops):
                #    print("There is also inconsistency between wmcore_transferor and transfer_ops rules")
                #    print("On Rucio by transfer_ops:", str(pileup_locations_on_rucio_transfer_ops))
            #elif set(pileup_locations_on_rucio_wmcore_transferor) != set(pileup_locations_on_rucio_transfer_ops):
            #    inconsistency_count += 1
            #    print(str(inconsistency_count) + ". campaigns.json and wmcore_transferor rules are consistent, but there is inconsistency between wmcore_transferor and transfer_ops rules for campaign " + campaignName)
            #    print(secondaryName)
            #    print("On campaign config:", str(secondaryLocations) )
            #    print("On Rucio by wmcore_transferor:", str(pileup_locations_on_rucio_wmcore_transferor))
            #    print("On Rucio by transfer_ops:", str(pileup_locations_on_rucio_transfer_ops))
            print("")


campaigns_str = json.dumps(campaigns, indent = 2, separators=(',', ': '))

with open("campaigns_mspileup.json", "w") as outfile:
    outfile.write(campaigns_str)

print ("Total number of inconsistencies: ", str(inconsistency_count+1))






#


#