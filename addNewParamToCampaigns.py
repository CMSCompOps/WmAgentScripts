
import json
# Opening JSON file
f1 = open('campaigns.json')

campaigns = json.load(f1)

for campaignName, v in list(campaigns.items()):
    if "secondaries" in v:

        # If there is no secondary, pop the key
        if campaigns[campaignName]["secondaries"] == {}:
            campaigns[campaignName].pop(("secondaries", None))
        else:
            for secondaryName, locationsDict in list(v["secondaries"].items()):
                campaigns[campaignName]["secondaries"][secondaryName]["keepOnDisk"] = True
                campaigns[campaignName]["secondaries"][secondaryName]["fractionOnDisk"] = 1.0

campaigns_str = json.dumps(campaigns, indent = 2, separators=(',', ': '))

with open("campaigns_mspileup.json", "w") as outfile:
    outfile.write(campaigns_str)

