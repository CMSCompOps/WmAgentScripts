import json
import http.client
import os
import traceback
import urllib
from utils import make_x509_conn, reqmgr_url
from MSPileupClient import MSPileupClient

class PileupConfiguration():


    def __init__(self):

        #self.url = reqmgr_url
        self.url = "cmsweb-testbed.cern.ch"
        self.mspileupClient = MSPileupClient(url=self.url)

    def uploadPileups(self, content):
        pileupMap = self.arePileupsConsistent(content)
        if not pileupMap:
            print("ERROR: Pileups are not consistent")
            return False
        self.updatePileupDocuments(pileupMap)
        self.checkOrphanPileups(pileupMap)
        return True

    def arePileupsConsistent(self, content):
        """
        Parse all the campaigns. If the locations of a given pileup are defined differently in different campaings,
        it exits.
        TODO: Make this function modular, there is too many duplicate code
        :param campaign content
        """
        pileupMap = {}
        for campaignName, v in list(content.items()):
            if "secondaries" in v:
                for secondaryName, pileupDetails in list(v["secondaries"].items()):

                    if "SecondaryLocation" in pileupDetails:
                        secondaryLocations = pileupDetails["SecondaryLocation"]
                    elif "SiteWhitelist" in pileupDetails:
                        secondaryLocations = pileupDetails["SiteWhitelist"]
                    else:
                        print ("No location defined for the secondary, exiting")
                        return False

                    if "keepOnDisk" in pileupDetails:
                        keepOnDisk = pileupDetails["keepOnDisk"]
                    else:
                        print ("No keepOnDisk defined for the secondary, exiting")
                        return False

                    if "fractionOnDisk" in pileupDetails:
                        fractionOnDisk = pileupDetails["fractionOnDisk"]
                    else:
                        print ("No keepOnDisk defined for the secondary, exiting")
                        return False

                    if secondaryName in pileupMap:
                        # Check location consistency
                        if set(pileupMap[secondaryName]["secondaryLocations"]) != set(secondaryLocations):
                            print ("Inconsistent pileup location setting for ", secondaryName)
                            return False

                        # Check keepOnDisk consistency
                        if pileupMap[secondaryName]["keepOnDisk"] != keepOnDisk:
                            print ("Inconsistent keepOnDisk setting for ", secondaryName)
                            return False

                        # Check fractionOnDisk consistency
                        if pileupMap[secondaryName]["fractionOnDisk"] != fractionOnDisk:
                            print ("Inconsistent fractionOnDisk setting for ", secondaryName)
                            return False

                        # Add the campaign
                        pileupMap[secondaryName]["campaigns"].append(campaignName)

                    else:
                        # TODO: Add other attributes: pileup_type, active
                        pileupMap[secondaryName] = {
                            "secondaryLocations": secondaryLocations,
                            "campaigns": [campaignName],
                            "keepOnDisk": keepOnDisk,
                            "fractionOnDisk": fractionOnDisk,
                            "pileupType": self.getPileupType(secondaryName)
                        }

        return pileupMap

    def updatePileupDocuments(self, pileupMap):
        """
        Parse all the campaigns. If pileup is new, insert it into MSPileup. If not, update it.
        :param campaign content
        """
        for pileupName, pileupDetails in pileupMap.items():
            print ("Starting checking the following pileup ", pileupName)
            try:
                responseToGET = self.mspileupClient.getByPileupName(pileupName)["result"]
                pileupDocument = {
                    "pileupName": pileupName,
                    "pileupType": pileupDetails["pileupType"],
                    "expectedRSEs": pileupDetails["secondaryLocations"],
                    "campaigns": pileupDetails["campaigns"],
                    "active": pileupDetails["keepOnDisk"],
                    "containerFraction": pileupDetails["fractionOnDisk"]
                }
                if not responseToGET:
                    print ("PILEUP CREATION NEEDED: Will make a POST call to MSPileup with the following data:")
                    print (pileupDocument)
                    responseToPOST = self.mspileupClient.createPileupDocument(pileupDocument)
                    if responseToPOST:
                        print ("Response for the create POST call:")
                        print (responseToPOST)
                    else:
                        print ("ERROR: Pileup creation failed")
                else:
                    print ("PILEUP UPDATE NEEDED: Will make a PUT call to MSPileup with the following data:")
                    print (pileupDocument)
                    responseToPUT = self.mspileupClient.updatePileupDocument(pileupDocument)
                    print ("Response for the update PUT call:")
                    print(responseToPUT)

            except Exception as e:
                print ("ERROR: updatePileupDocuments failed")
                print (str(e))

    def checkOrphanPileups(self, pileupMap):
        allPileups = self.mspileupClient.getAllPileups()["result"]
        if not allPileups:
            print("ERROR: Couldn't get all the pileups. Cannot perform orphan pileup check")
        else:
            for pileupObj in allPileups:
                if pileupObj["pileupName"] not in pileupMap:
                    print (
                    "ORPHAN PILEUP: This pileup exists in MSPileup, but not in WmAgentScripts/campaigns.json. Please check and consider deleting it:",
                    pileupObj["pileupName"])

    def getPileupType(self, pileupName):
        if any(['minbias' in c.lower() for c in pileupName]):
            return "classic"
        else:
            return "premix"