#!/usr/bin/env python
import json

from utils import make_x509_conn, reqmgr_url


def getCampaignConfig(docName, url=reqmgr_url):
    """
    Retrieve campaign documents from central CouchDB
    :param url: API URL
    :param docName: string with the doc name (use "ALL_DOCS" to retrieve all docs)
    :return: a list of dictionaries
    """
    headers = {"Content-type": "application/json", "Accept": "application/json"}
    conn = make_x509_conn(url)
    url = '/reqmgr2/data/campaignconfig/%s' % docName
    conn.request("GET", url, headers=headers)
    r2 = conn.getresponse()
    data = json.loads(r2.read())
    return data['result']


def createCampaignConfig(docContent, url=reqmgr_url):
    """
    Create a new campaign document in central CouchDB
    :param url: API URL
    :param docContent: dictionary with the campaign content
    :return: a boolean whether it succeeded (True) or not (False)
    """
    if isinstance(docContent, list) and len(docContent) > 1:
        print("ERROR: createCampaignConfig expects a single campaign configuration, not a list of them!")
        return False
    elif isinstance(docContent, list):
        docContent = docContent[0]
    outcome = True
    headers = {"Content-type": "application/json", "Accept": "application/json"}
    conn = make_x509_conn(url)
    url = '/reqmgr2/data/campaignconfig/%s' % docContent['CampaignName']
    json_args = json.dumps(docContent)
    conn.request("POST", url, json_args, headers=headers)
    resp = conn.getresponse()
    if resp.status >= 400:
        print("FAILED to create campaign: %s. Response status: %s, response reason: %s"
              % (docContent['CampaignName'], resp.status, resp.reason))
        outcome = False
    conn.close()
    return outcome


def updateCampaignConfig(docContent, url=reqmgr_url):
    """
    Update an existent campaign document in central CouchDB
    :param url: API URL
    :param docContent: full dictionary with the campaign content
    :return: a boolean whether it succeeded (True) or not (False)
    """
    outcome = True
    headers = {"Content-type": "application/json", "Accept": "application/json"}
    conn = make_x509_conn(url)
    url = '/reqmgr2/data/campaignconfig/%s' % docContent['CampaignName']
    json_args = json.dumps(docContent)
    conn.request("PUT", url, json_args, headers=headers)
    resp = conn.getresponse()
    if resp.status >= 400:
        print("FAILED to update campaign: %s. Response status: %s, response reason: %s"
              % (docContent['CampaignName'], resp.status, resp.reason))
        outcome = False
    conn.close()
    return outcome


def deleteCampaignConfig(docName, url=reqmgr_url):
    """
    Delete an existent campaign document in central CouchDB
    :param url: API URL
    :param docName: string with the document name
    :return: a boolean whether it succeeded (True) or not (False)
    """
    outcome = True
    headers = {"Content-type": "application/json", "Accept": "application/json",
               "Content-Length": 0}  # this is required for DELETE calls
    conn = make_x509_conn(url)
    url = '/reqmgr2/data/campaignconfig/%s' % docName
    conn.request("DELETE", url, headers=headers)
    resp = conn.getresponse()
    if resp.status >= 400:
        print("FAILED to delete campaign: %s. Response status: %s, response reason: %s"
              % (docName, resp.status, resp.reason))
        outcome = False
    conn.close()
    return outcome


def parseMongoCampaigns(campaigns, verbose=False):
    """
    Given a set of campaign records, parse it and convert them
    to a WMCore supported format
    :param campaigns: list of campaign configurations
    :param verbose: flag to enable verbosity or not
    :return: a list of dictionaries, each dict corresponds to a campaign config
    """
    wmCampaigns = []

    # re-map Unified keys into campaign schema ones
    remap = {
        'name': 'CampaignName',
        'SiteWhitelist': 'SiteWhiteList',
        'SiteBlacklist': 'SiteBlackList',
        'primary_AAA': 'PrimaryAAA',
        'secondary_AAA': 'SecondaryAAA',
        'SecondaryLocation': 'SecondaryLocation',
        'secondaries': 'Secondaries',
        'partial_copy': 'PartialCopy',
        'toDDM': 'TiersToDM',
        'maxcopies': 'MaxCopies'}
    # campaign schema dict
    confRec = {
        'CampaignName': None,
        'SiteWhiteList': [],
        'SiteBlackList': [],
        'PrimaryAAA': False,
        'SecondaryAAA': False,
        'SecondaryLocation': [],
        'Secondaries': {},
        'PartialCopy': 1,
        'TiersToDM': [],
        'MaxCopies': 1}

    if not isinstance(campaigns, list):
        campaigns = [campaigns]
    for rec in campaigns:
        if verbose:
            print("read record: %s (type=%s)" % (rec, type(rec)))

        conf = dict(confRec)
        # Set default value from top level campaign configuration
        # or use the default values defined above
        for uniKey, wmKey in remap.items():
            conf[wmKey] = rec.get(uniKey, conf[wmKey])

        conf['SiteWhiteList'] = _getSiteList("SiteWhitelist", conf['SiteWhiteList'], rec)
        conf['SiteBlackList'] = _getSiteList("SiteBlacklist", conf['SiteBlackList'], rec)
        conf['SecondaryAAA'] = _getSecondaryAAA(conf['SecondaryAAA'], rec)
        conf['SecondaryLocation'] = _getSecondaryLocation(conf['SecondaryLocation'], rec)
        conf['Secondaries'] = _getSecondaries(conf['Secondaries'], rec)
        if verbose:
            print("Final WMCore Campaign configuration: %s" % conf)
        wmCampaigns.append(conf)
    return wmCampaigns


def _intersect(slist1, slist2):
    "Helper function to intersect values from two non-empty lists"
    if slist1 and slist2:
        return list(set(slist1) & set(slist2))
    if slist1 and not slist2:
        return slist1
    if not slist1 and slist2:
        return slist2
    return []


def _getSiteList(keyName, initialValue, uniRecord):
    """
    Parse information related to the SiteWhiteList and SiteBlackList, which corresponds
    to a list of sites where the workflow gets (or doesn't) assigned to and where the
    primary and secondary CLASSIC MIX dataset is placed (likely in chunks of data);
    mapped from the SiteWhitelist/SiteBlacklist key which can be AFAIK in multiple places, like:
      * top level dict,
      * under the parameters key and
      * under the secondaries dictionary.
    If it appears multiple times, we make an intersection of the values
    """
    if keyName in uniRecord.get("parameters", {}):
        print("Found internal %s for campaign: %s" % (keyName, uniRecord['name']))
        initialValue = _intersect(initialValue, uniRecord["parameters"][keyName])

    return initialValue


def _getSecondaryAAA(initialValue, uniRecord):
    """
    SecondaryAAA: boolean to flag whether to use AAA for the secondary dataset;
    mapped from the secondary_AAA key, which can be either:
      * at top level or
      * under the secondaries dictionary.
    If it appears multiple times, we make an OR of the values.
    """
    for _, innerDict in uniRecord.get("secondaries", {}).items():
        if "secondary_AAA" in innerDict:
            print("Found internal secondary_AAA for campaign: %s" % uniRecord['name'])
            initialValue = initialValue or innerDict["secondary_AAA"]
    return initialValue


def _getSecondaryLocation(initialValue, uniRecord):
    """
    SecondaryLocation: list of sites where the secondary PREMIX dataset has to be placed
    as a whole (not necessarily also assigned to those).
    mapped from the SecondaryLocation key, which can be either:
      * at top level or
      * under the secondaries dictionary.
    If it appears multiple times, we make an intersection of the values.
    """
    for _, innerDict in uniRecord.get("secondaries", {}).items():
        if "SecondaryLocation" in innerDict:
            print("Found internal SecondaryLocation for campaign: %s" % uniRecord['name'])
            initialValue = _intersect(initialValue, innerDict["SecondaryLocation"])
    return initialValue


def _getSecondaries(initialValue, uniRecord):
    """
    Secondaries: dictionary with a map of allowed secondary datasets and where they are
    supposed to be placed (in conjunction with the top level location parameters;
    mapped from the secondaries top level key

    Each dataset will have a list value type, and the content is either:
      * taken from the SiteWhitelist key or
      * taken from the SecondaryLocation one
    """
    for dset, innerDict in uniRecord.get("secondaries", {}).items():
        print("Found secondaries for campaign: %s" % uniRecord['name'])
        initialValue[dset] = _intersect(innerDict.get("SiteWhitelist", []),
                                        innerDict.get("SecondaryLocation", []))

    return initialValue
