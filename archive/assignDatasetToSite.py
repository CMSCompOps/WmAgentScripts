#!/usr/bin/env python
#---------------------------------------------------------------------------------------------------
#
# This script will find a requested number of Tier-2 sites appropriate to serve as the initial
# location(s) for the specifified dataset. It will also make sure that the sample copies on all
# Tier-1 disk spaces owned by 'DataOps' phedex group will be signed over to the 'AnalysisOps' group.
#
# Injection of so called open datasets (datasets that are not yet completed and will be growing) is
# problematic as the size of the dataset is not correct in the database. To solve this problem an
# expected dataset size can be specified to overwrite this information (ex. --expectedSizeGb=1000). 
# 
# Failures of any essential part of this assignment will lead to a non-zero return code. For now the
# failure return code is always 1.
#
# Implementation: by design this should be a standalone script that will work when you copy it into
# your directory. It is important so that it virtually runs anywhere and anyone can easily use it
# without having to checkout anything from github.
#
# Set up dbs3 client (of course you have to install it first):
#   VO_CMS_SW_DIR=$HOME/cms/cmssoft
#   SCRAM_ARCH=slc6_amd64_gcc481
#   DBS3_CLIENT_VERSION=3.2.11d
#   source $VO_CMS_SW_DIR/$SCRAM_ARCH/cms/dbs3-client/3.2.11d/etc/profile.d/init.sh
#
# Unit test:
#   ./assignDatasetToSite.py --nCopies=2 --dataset=/DoubleElectron/Run2012A-22Jan2013-v1/AOD
#---------------------------------------------------------------------------------------------------
import os, sys, subprocess, getopt, re, random, urllib, urllib2, httplib, json, time
from dbs.apis.dbsClient import DbsApi

#===================================================================================================
#  C L A S S E S
#===================================================================================================

#---------------------------------------------------------------------------------------------------
class phedexApi:
#---------------------------------------------------------------------------------------------------
    """
    _phedexApi_
    Interface to submit queries to the PhEDEx API For specifications of calls see
    https://cmsweb.cern.ch/phedex/datasvc/doc
    Class variables:
    phedexBase -- Base URL to the PhEDEx web API (https://cmsweb.cern.ch/phedex/datasvc/)
    """
    # Useful variables
    #-----------------
    # phedexInstance = "prod" or "dev"
    # dataType = "json" or "xml"
    # site = "T2_US_Nebraska"
    # dataset = "/Muplus_Pt1_PositiveEta-gun/Muon2023Upg14-DES23_62_V1-v1/GEN-SIM"
    # group = 'AnalysisOps' (or 'local')

    def __init__(self):
        """
        __init__
        Set up class constants
        """
        self.phedexBase = "https://cmsweb.cern.ch/phedex/datasvc/"

    def phedexCall(self, url, values):
        """
        _phedexCall_
        Make http post call to PhEDEx API. Function only gaurantees that something is returned, the
        caller need to check the response for correctness.
        url    - URL to make API call
        values - arguments to pass to the call
        Return values:
        1 -- Status, 0 = everything went well, 1 = something went wrong
        2 -- IF status == 0 : HTTP response ELSE : Error message
        """
        #print "call",time.asctime()
        data = urllib.urlencode(values)
        #print "encode",time.asctime()
        opener = urllib2.build_opener(HTTPSGridAuthHandler())
        #print "auth",time.asctime()
        request = urllib2.Request(url, data)
        #print "request",time.asctime()
        try:
            response = opener.open(request)
        except urllib2.HTTPError, e:
            return 1, " Error - urllib2.HTTPError %s \n  URL: %s\n  VALUES: %s"%\
                   (e.read(),str(url),str(values))
        except urllib2.URLError, e:
            return 1, " Error - urllib2.URLError %s \n  URL: %s\n  VALUES: %s"%\
                   (e.args,str(url),str(values))
        return 0, response

    def data(self, dataset='', block='', fileName='', level='block',
             createSince='', format='json', instance='prod'):
        """
        _data_
        PhEDEx data call. At least one of the arguments dataset, block, file have to be passed.  No
        checking is made for xml data. Even if JSON data is returned no gaurantees are made for the
        structure of it
        Keyword arguments:
        dataset     -- Name of dataset to look up
        block       -- Name of block to look up
        file        -- Name of file to look up
        block       -- Only return data for this block
        fileName    -- Data for file fileName returned
        level       -- Which granularity of dataset information to show
        createSince -- Files/blocks/datasets created since this date/time
        format      -- Which format to return data as, XML or JSON
        instance    -- Which instance of PhEDEx to query, dev or prod
        Return values:
        check -- 0 if all went well, 1 if error occured
        data  -- json structure if json format, xml structure if xml format
        """
        if not (dataset or block or fileName):
            return 1, " Error - Need to pass at least one of dataset/block/fileName"
        values = { 'dataset' : dataset, 'block' : block, 'file' : fileName,
                   'level' : level, 'create_since' : createSince }
        dataURL = urllib.basejoin(self.phedexBase, "%s/%s/data"%(format, instance))
        check, response = self.phedexCall(dataURL, values)
        if check:
            return 1, " Error - Data call failed"
        if format == "json":
            try:
                data = json.load(response)
            except ValueError, e:
                # This usually means that PhEDEx didn't like the URL
                return 1, " ERROR - ValueError in call to url %s : %s"%(dataURL, str(e))
            if not data:
                return 1, " Error - no json data available"
        else:
            data = response.read()
        return 0, data

    def parse(self, data, xml):
        """
        _parse_
        Take data output from PhEDEx and parse it into xml syntax corresponding to subscribe and
        delete calls.
        """
        for k, v in data.iteritems():
            k = k.replace("_", "-")
            if type(v) is list:
                xml = "%s>" % (xml,)
                for v1 in v:
                    xml = "%s<%s" % (xml, k)
                    xml = self.parse(v1, xml)
                    if (k == "file"):
                        xml = "%s/>" % (xml,)
                    else:
                        xml = "%s</%s>" % (xml, k)
            else:
                if k == "lfn":
                    k = "name"
                elif k == "size":
                    k = "bytes"
                if (k == "name" or k == "is-open" or k == "is-transient" or \
                    k == "bytes" or k== "checksum"):
                    xml = '%s %s="%s"' % (xml, k, v)
        return xml

    def xmlData(self, datasets=[], instance='prod'):
        """
        _xmlData_
        Get json data from PhEDEx for all datasets and convert it to a xml structure complient with
        the PhEDEx delete/subscribe call.
        datasets - list of dataset names
        instance - the instance on which the datasets resides, prod/dev
        Return values:
        error -- 1 if an error occurred, 0 if everything went as expected
        xml   -- the converted data now represented as an xml structure
        """
        if not datasets:
            return 1, " Error - need to pass at least one of dataset."
        xml = '<data version="2">'
        xml = '%s<%s name="https://cmsweb.cern.ch/dbs/%s/global/DBSReader">'\
              % (xml, 'dbs', instance)
        for dataset in datasets:
            check, response = self.data(dataset=dataset, level='file', instance=instance)
            if check:
                return 1, " Error"
            data = response.get('phedex').get('dbs')
            if not data:
                return 1, " Error"
            xml = "%s<%s" % (xml, 'dataset')
            data = data[0].get('dataset')
            xml = self.parse(data[0], xml)
            xml = "%s</%s>" % (xml, 'dataset')
        xml = "%s</%s>" % (xml, 'dbs')
        xml_data = "%s</data>" % (xml,)

        return 0, xml_data

    def subscribe(self, node='', data='', level='dataset', priority='low', move='n', static='n',
                  custodial='n', group='AnalysisOps', timeStart='', requestOnly='n', noMail='y',
                  comments='', format='json', instance='prod'):
        """
        _subscribe_
        Set up subscription call to PhEDEx API.
        """
        if not (node and data):
            return 1, "Error - subscription: node and data needed."
        values = { 'node' : node, 'data' : data, 'level' : level, 'priority' : priority,
                   'move' : move, 'static' : static, 'custodial' : custodial, 'group' : group,
                   'time_start' : timeStart, 'request_only' : requestOnly, 'no_mail' : noMail,
                   'comments' : comments }
        subscriptionURL = urllib.basejoin(self.phedexBase, "%s/%s/subscribe" % (format, instance))
        check, response = self.phedexCall(subscriptionURL, values)
        if check:
            return 1, "Error - subscription: check not zero"
        return 0, response

    def delete(self, node='', data='', level='dataset', rmSubscriptions='y',
               comments='', format='json', instance='prod'):
        """
        _delete_
        Set up subscription call to PhEDEx API.
        """
        if not (node and data):
            return 1, " Error - need to pass both node and data"
        values = { 'node' : node, 'data' : data, 'level' : level,
                   'rm_subscriptions' : rmSubscriptions, 'comments' : comments }
        deleteURL = urllib.basejoin(self.phedexBase, "%s/%s/delete" % (format, instance))
        check, response = self.phedexCall(deleteURL, values)
        if check:
            return 1, " Error - self.phedexCall with response: " + response
        return 0, response
    
    def updateSubscription(self, node='', dataset='', group='AnalysisOps',
                           format='json', instance='prod'):
        """
        _updateSubscription_
        Update an existing subscription through a call to PhEDEx API.
        """
        name = "updatesubscription"
        if not (node and dataset):
            return 1, "Error - %s: node and dataset are needed."%(name)
        values = {'node' : node, 'dataset' : dataset, 'group' : group}
        url = urllib.basejoin(self.phedexBase, "%s/%s/%s" % (format,instance,name))
        check, response = self.phedexCall(url, values)
        if check:
            return 1, "ERROR - self.phedexCall with response: " + response
        return 0, response

#---------------------------------------------------------------------------------------------------
class HTTPSGridAuthHandler(urllib2.HTTPSHandler):
    """
    _HTTPSGridAuthHandler_
    Get proxy to acces PhEDEx API. Needed for subscribe and delete calls.
    Class variables:
      key  -- user key to CERN with access to PhEDEx
      cert -- user certificate connected to key
    """

    def __init__(self):
        urllib2.HTTPSHandler.__init__(self)
        self.key = self.getProxy()
        self.cert = self.key

    def https_open(self, req):
        return self.do_open(self.getConnection, req)

    def getProxy(self):
        #proxy = os.environ['X509_USER_PROXY']
        cmd = 'voms-proxy-info -path'
        for line in subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE).stdout.readlines():
	    proxy = line[:-1]
        
        return proxy

    def getConnection(self, host, timeout=300):
        return httplib.HTTPSConnection(host, key_file=self.key, cert_file=self.cert)

#===================================================================================================
#  H E L P E R S
#===================================================================================================
def testLocalSetup(dataset,debug=0):
    # check the input parameters
    if dataset == '':
        print ' Error - no dataset specified. EXIT!\n'
        print usage
        sys.exit(1)

    # check the user proxy
    validProxy = False
    cmd = 'voms-proxy-info -path'
    for line in subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE).stdout.readlines():
        proxy = line[:-1]
    if proxy != "":
        if debug>0:
	    print " User proxy in: " + proxy
        cmd = 'voms-proxy-info -timeleft'
        for line in subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE).stdout.readlines():
	    timeleft = int(line[:-1])
        if timeleft > 3600:
	    validProxy = True

    if not validProxy:
        print ' Error - no X509_USER_PROXY, please check. EXIT!'
        sys.exit(1)

def convertSizeToGb(sizeTxt):

    # first make sure string has proper basic format
    if len(sizeTxt) < 3:
        print ' ERROR - string for sample size (%s) not compliant. EXIT.'%(sizeTxt)
        sys.exit(1)

    if sizeTxt.isdigit(): # DAS decides to give back size in bytes
        sizeGb = int(sizeTxt)/1000/1000/1000        
    else:              # DAS gives human readable size with unit integrated
        # this is the text including the size units, that need to be converted
        sizeGb  = float(sizeTxt[0:-2])
        units   = sizeTxt[-2:]
        # decide what to do for the given unit
        if units == 'UB':
            sizeGb = sizeGb/(1024.)**3
        elif   units == 'MB':
            sizeGb = sizeGb/1000.
        elif units == 'GB':
            pass
        elif units == 'TB':
            sizeGb = sizeGb*1000.
        else:
            print ' ERROR - Could not identify size. EXIT!'
            sys.exit(1)

    # return the size in GB as a float
    return sizeGb

def findExistingSubscriptions(dataset,group='AnalysisOps',sitePattern='T2*',debug=0):
    conn  =  httplib.HTTPSConnection('cmsweb.cern.ch', cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",'/phedex/datasvc/json/prod/subscriptions?group=%s&node=%s&block=%s%%23*&collapse=y'%(group,sitePattern,dataset))
    r2=conn.getresponse()
    result = json.loads(r2.read())['phedex']
    siteNames = []
    #print result['dataset']
    for dataset in result['dataset']:
        if not 'subscription' in dataset: continue
        for sub in dataset['subscription']:
            if sub['level'] != "DATASET" : continue
            
            siteName = sub['node']
            if siteName in siteNames: 
                if debug>0:        
                    print ' Site already in list. Skip!'
            else:
                siteNames.append( sub['node'] )
    return siteNames
                   

"""    webServer = 'https://cmsweb.cern.ch/'
    phedexBlocks = 'phedex/datasvc/xml/prod/blockreplicas?subscribed=y&group=%s&node=%s&dataset=%s'\
               %(group,sitePattern,dataset)
    url = '"'+webServer+phedexBlocks + '"'
    cmd = 'curl -k -H "Accept: text/xml" ' + url + ' 2> /dev/null'

    #cert = os.environ.get('X509_USER_PROXY')
    #cmd = 'curl --cert ' + cert + ' -k -H "Accept: text/xml" ' + url + ' 2> /dev/null'

    if debug > 1:
        print ' Access phedexDb: ' + cmd

    # setup the shell command
    siteNames = []
    for line in subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE).stdout.readlines():
        if debug > 1:
            print ' LINE: ' + line
        # find the potential T2s
        try:
            sublines = re.split("<replica\ ",line)
            for subline in sublines[1:]:
                siteName = (re.findall(r"node='(\S+)'",subline))[0]
                if siteName in siteNames:
                    if debug>0:
                        print ' Site already in list. Skip!'
                else:
                    siteNames.append(siteName)
        except:
            siteName = ''

    return siteNames
"""

def getActiveSites(debug=0):
    # hardcoded fallback
    tier2Base = [ 'T2_AT_Vienna','T2_BR_SPRACE','T2_CH_CSCS','T2_DE_DESY','T2_DE_RWTH',
                  'T2_ES_CIEMAT','T2_ES_IFCA',
                  'T2_FR_IPHC','T2_FR_GRIF_LLR',
                  'T2_IT_Pisa','T2_IT_Bari','T2_IT_Rome',
                  'T2_RU_JINR',
                  'T2_UK_London_IC',
                  'T2_US_Caltech','T2_US_Florida','T2_US_MIT','T2_US_Nebraska','T2_US_Purdue',
                  'T2_US_Wisconsin'
                  ]

    # download list of active sites
    sites = []

    # get the active site list
    #cmd  = 'wget http://t3serv001.mit.edu/~cmsprod/IntelROCCS/Detox/ActiveSites.txt'
    cmd  = 'wget http://t3serv001.mit.edu/~cmsprod/IntelROCCS/Detox/SitesInfo.txt'
    cmd += ' -O - 2> /dev/null | grep -v "#" | grep T2_ | tr -s " "'
    for line in subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE).stdout.readlines():
        site = line[:-1]
        f = site.split(' ')
        if debug>2:
            print " Length: %d"%(len(f))
        if len(f) != 7:
            continue

        # decode
        if debug>2:
            print f
        site = f[-2]
        lastCopy = int(f[-3])
        quota = int(f[-5])
        valid = int(f[-6])

        # sanity check
        if quota == 0:
            continue

        # debug output
        if debug > 1:
            print ' Trying to add: "' + site + '"  lastCp: %d  Quota: %d  -->  %f'\
                %(lastCopy,quota,float(lastCopy)/quota)

        # check whether site is appropriate
        if valid != 1:
            continue

        # is the site large enough
        if float(lastCopy)/quota > 0.7:
            if debug > 0:
                print '  -> skip %s as Last Copy too large or not valid.\n'%(site)
            continue


        if debug > 0:
            print '  -> adding %s\n'%(site)
                
        # add this site
        sites.append(site)

    # something went wrong
    if len(sites) < 10:
        print ' WARNINIG - too few sites found, reverting to hardcoded list'
        sites = tier2Base

    # return the size in GB as a float
    return sites

def chooseMatchingSite(tier2Sites,nSites,sizeGb,debug):
    # Given a list of Tier-2 centers, a requested number of copies and the size of the sample to
    # assign we choose a list of sites

    iRan = -1
    quotas = []
    lastCps = []
    sites = []

    nTrials = 0

    fraction_usable_quota = 0.1
    while len(sites) < nSites:
        iRan = random.randint(0,len(tier2Sites)-1)
        site = tier2Sites[iRan]
        # not elegant or reliable (should use database directly)
        cmd  = 'wget http://t3serv001.mit.edu/~cmsprod/IntelROCCS/Detox/result/'+site+'/Summary.txt'
        cmd += ' -O - 2> /dev/null | grep ^Total | head -1'
        quota = 0
        for line in subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE).stdout.readlines():
            line = line[:-1]
            f = line.split(' ')
            quota = float(f[-1]) * 1000.   # make sure it is GB

        cmd  = 'wget http://t3serv001.mit.edu/~cmsprod/IntelROCCS/Detox/result/'+site+'/Summary.txt'
        cmd += ' -O - 2> /dev/null | grep ^\"Space last CP\"  | head -1'

        lastCp = 0
        for line in subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE).stdout.readlines():
            line = line[:-1]
            f = line.split(' ')
            lastCp = float(f[-1]) * 1000.  # make sure it is GB

        if sizeGb < fraction_usable_quota*quota:
            sites.append(site)
            quotas.append(quota)
            lastCps.append(lastCp)
            tier2Sites.remove(site)
        else:
            fraction_usable_quota += 0.05

        if debug > 0:
            print ' Trying to fit %.1f GB into Tier-2 [%d]: %s with quota of %.1f GB (use %.3f max)'%\
                  (sizeGb,iRan,site,quota,fraction_usable_quota)

        if nTrials > 20:
            print ' Error - not enough matching sites could be found. Dataset too big? EXIT!'
            sys.exit(1)

        nTrials += 1
    return sites,quotas,lastCps

def submitSubscriptionRequests(sites,datasets=[],debug=0, group='AnalysisOps'):
    # submit the subscription requests

    # keep track of the return code
    rc = 0

    # make sure we have datasets to subscribe
    if len(datasets) < 1:
        rc = 1
        print " ERROR - Trying to submit empty request for "
        print sites
        return rc

    phedex = phedexApi()

    # compose data for subscription request
    check,data = phedex.xmlData(datasets=datasets,instance='prod')

    if check:
        rc = 1
        print " ERROR - phedexApi.xmlData failed"
        return rc
    message = 'IntelROCCS -- Automatic Dataset Subscription by Computing Operations.'

    # here the request is really sent to each requested site
    for site in sites:
        if debug>-1:
            print " --> phedex.subscribe(node=%s,data=....,comments=%s', \ "%(site,message)
            print "                      group=%s,instance='prod')"%group

        check,response = phedex.subscribe(node=site,data=data,comments=message,group=group,
                          instance='prod')
        if check:
            rc = 1
            print " ERROR - phedexApi.subscribe failed for Tier2: " + site
            print response
            continue

    return rc

def submitUpdateSubscriptionRequest(sites,datasets=[],debug=0,group='AnalysisOps'):
    # submit the request for an update of the subscription

    # keep track of potential failures
    rc = 0

    # check our paramters for phedex call
    # make sure we have datasets to subscribe
    dataset = 'EMPTY'
    if len(datasets) < 1:
        rc = 1
        print " ERROR - Trying to submit empty update subscription request for " + site
        return rc
    else:
        dataset = datasets[0]
        
    # setup phedex api
    phedex = phedexApi()

    # loop through all identified sites
    for site in sites:
        if debug>-1:
            print " --> phedex.updateSubscription(node=%s, \ "%(site)
            print "                               data=%s, \ "%(dataset)
            print "                               group=%s ) "%(group)

        check,response = phedex.updateSubscription(node=site,dataset=dataset,group=group,
                               instance='prod')
        if check:
            rc = 1
            print " ERROR - phedexApi.updateSubscription failed for site: " + site
            print response
            print time.asctime()
            continue

    return rc

#===================================================================================================
#  M A I N
#===================================================================================================
# Define string to explain usage of the script
usage =  " Usage: assignDatasetToSite.py   --dataset=<name of a CMS dataset>\n"
usage += "                 [ --nCopies=1 ]           <-- number of desired copies \n"
usage += "                 [ --expectedSizeGb=-1 ]   <-- open subscription to avoid small sites \n"
usage += "                 [ --destination=... ]     <-- coma separated list of destination sites \n"
usage += "                 [ --debug=0 ]             <-- see various levels of debug output\n"
usage += "                 [ --exec ]                <-- add this to execute all actions\n"
usage += "                 [ --help ]\n\n"

# Define the valid options which can be specified and check out the command line
valid = ['dataset=','debug=','nCopies=','expectedSizeGb=','destination=', 'exec','help']
try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)

# --------------------------------------------------------------------------------------------------
# Get all parameters for the production
# --------------------------------------------------------------------------------------------------
# Set defaults for each command line parameter/option
debug = 0
dataset = ''
nCopies = 1
destination = []
exe = False
expectedSizeGb = -1
isMiniAod = False
group='AnalysisOps'

# Read new values from the command line
for opt, arg in opts:
    if opt == "--help":
        print usage
        sys.exit(0)
    if opt == "--dataset":
        dataset = arg
    if opt == "--nCopies":
        nCopies = int(arg)
    if opt == "--expectedSizeGb":
        expectedSizeGb = int(arg)
    if opt == "--destination":
        destination = arg.split(",")
    if opt == "--debug":
        debug = int(arg)
    if opt == "--exec":
        exe = True
    if opt == "--group":
        group = arg

## in the meantime in casablanca
#debug=1

# inspecting the local setup
#---------------------------

start_time = time.mktime(time.gmtime())
testLocalSetup(dataset,debug)

# Say what dataset we are looking at
#-----------------------------------

print '\n DATASET: ' + dataset
f = dataset.split("/")
if len(f) > 3:
    tier = f[3]
    if 'MINIAOD' in tier:
        print ' MINIAOD* identified, consider extra T2_CH_CERN copy.'
        isMiniAod = True

# size of provided dataset
#-------------------------

# instantiate an API
dbsapi = DbsApi(url='https://cmsweb.cern.ch/dbs/prod/global/DBSReader')

# first test whether dataset is valid
dbsList = dbsapi.listDatasets(dataset = dataset, dataset_access_type = 'VALID')
datasetInvalid = False
if dbsList == []:
    datasetInvalid = True
    print ' Dataset does not exist or is invalid. Exit now!\n'
    sys.exit(1)

# determine size and number of files
size = str(sum([block['file_size'] for block in dbsapi.listBlockSummaries(dataset = dataset)]))+'UB'
sizeGb = convertSizeToGb(size)

# in case this is an open subscription we need to adjust sizeGb to the expected size
if expectedSizeGb > 0:
    sizeGb = expectedSizeGb

print ' SIZE:    %.1f GB'%(sizeGb)

# prepare subscription list
datasets = []
datasets.append(dataset)


# first make sure this dataset is not owned by DataOps group anymore at the Tier-1 site(s)
#-----------------------------------------------------------------------------------------

tier1Sites = findExistingSubscriptions(dataset,'DataOps','T1_*_Disk',debug)
if debug>0:
    print ' Re-assign all Tier-1 copies from DataOps to AnalysisOps space.'
if len(tier1Sites) > 0:
    print '\n Resident in full under DataOps group on the following Tier-1 disks:'
    for tier1Site in tier1Sites:
        print ' --> ' + tier1Site
    print ''

    # update subscription at Tier-1 sites
    if exe:
        # make AnalysisOps the owner of all copies at Tier-1 site(s)
        rc = submitUpdateSubscriptionRequest(tier1Sites,datasets,debug)
        if rc != 0:
            sys.exit(1)
    else:
        print '\n -> WARNING: not doing anything .... please use  --exec  option.\n'
else:
    print '\n No Tier-1 full copies of this dataset in DataOps space.'
tier2Sites = findExistingSubscriptions(dataset,'DataOps','T2_*',debug)
if debug>0:
    print ' Re-assign all Tier-2 copies from DataOps to AnalysisOps space.'
if len(tier2Sites) > 0:
    print '\n Resident in full under DataOps group on the following Tier-2 disks:'
    for tier2Site in tier2Sites:
        print ' --> ' + tier2Site
    print ''

    # update subscription at Tier-1 sites
    if exe:
        # make AnalysisOps the owner of all copies at Tier-1 site(s)
        rc = submitUpdateSubscriptionRequest(tier2Sites,datasets,debug, group=group)
        if rc != 0:
            sys.exit(1)
    else:
        print '\n -> WARNING: not doing anything .... please use  --exec  option.\n'
else:
    print '\n No Tier-2 full copies of this dataset in DataOps space.'


# has the dataset already been subscribed?
#-----------------------------------------
# - no test that the complete dataset has been subscribed (could be just one block?)
# - we test all Tier2s and check there is at least one block subscribed no completed bit required
#
# --> need to verify this is sufficient

siteNames = findExistingSubscriptions(dataset,'AnalysisOps','T2_*',debug)
nAdditionalCopies = nCopies - len(siteNames)

if len(siteNames) >= nCopies:
    print '\n Already subscribed on Tier-2:'
    for siteName in siteNames:
        print ' --> ' + siteName

    if not isMiniAod:
        print '\n The job is done already: EXIT!\n'
        sys.exit(0)
else:
    print ' Requested %d copies at Tier-2. Only %d copies found.'%(nCopies,len(siteNames))
    print ' --> will find %d more sites for subscription.\n'%(nAdditionalCopies)


# find a sufficient matching site
#--------------------------------

# find all dynamically managed sites
tier2Sites = getActiveSites(debug)

# remove the already used sites
for siteName in siteNames:
    if debug>0:
        print ' Removing ' + siteName
    try:
        tier2Sites.remove(siteName)
    except:
        if debug>0:
            print ' Site is not in list: ' + siteName

# choose a site randomly and exclude sites that are too small

sites,quotas,lastCps = chooseMatchingSite(tier2Sites,nAdditionalCopies,sizeGb,debug)

if destination:
    print "overriding destination with",destination
    sites = destination

if not exe:
    print ''
    print ' SUCCESS - Found requested %d matching Tier-2 sites'%(len(sites))
    for i in range(len(sites)):
        print '           - %-20s (quota: %.1f TB lastCp: %.1f TB)'\
            %(sites[i],quotas[i]/1000.,lastCps[i]/1000.)

# make phedex subscription
#-------------------------

# subscribe them
if exe:
    # make subscriptions to Tier-2 site(s)
    rc = submitSubscriptionRequests(sites,datasets, group)
    if rc != 0:
        sys.exit(1)

    # make special subscription for /MINIAOD* to T2_CH_CERN
    if isMiniAod:
        cern = [ 'T2_CH_CERN' ]
        submitSubscriptionRequests(cern,datasets, group)    
        if rc != 0:
            sys.exit(1)

else:
    print '\n -> WARNING: not doing anything .... please use  --exec  option.\n'
    if isMiniAod:
        print ' INFO: extra copy to T2_CH_CERN activated.'

stop_time = time.mktime(time.gmtime())

print "total elapsed",stop_time-start_time
