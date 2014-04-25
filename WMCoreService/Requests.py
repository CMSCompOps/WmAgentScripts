#!/usr/bin/env python
#pylint: disable-msg=C0103,R0913,W0102
"""
_Requests_

A set of classes to handle making http and https requests to a remote server and
deserialising the response.

The response from the remote server is cached if expires/etags are set.
"""

import urllib
import os
import base64
from httplib import HTTPSConnection, HTTPConnection
import socket
import logging
import urlparse
from httplib import HTTPException
import tempfile
import shutil
import stat
import sys

from WMCoreService.JsonWrapper import JSONEncoder, JSONDecoder
from WMCoreService.JsonWrapper.JSONThunker import JSONThunker

def check_permissions(filehandle, permission, pass_stronger = False):
    info = os.stat(filehandle)
    filepermission = oct(info[stat.ST_MODE] & 0777)
    if pass_stronger:
        assert filepermission <= permission, "file's permissions are too weak"
    else:
        assert filepermission == permission, "file does not have the correct permissions"

def owner_readonly(file):
    check_permissions(file, oct(0400))

def owner_readwrite(file):
    check_permissions(file, oct(0600))

def owner_readwriteexec(file):
    check_permissions(file, oct(0700))
    
def check_server_url(srvurl):
    """Check given url for correctness"""
    good_name = srvurl.startswith('http://') or srvurl.startswith('https://')
    if not good_name:
        msg  = "You must include"
        msg += "http(s):// in your servers address, %s doesn't" % srvurl
        raise ValueError(msg)


class Requests(dict):
    """
    Generic class for sending different types of HTTP Request to a given URL
    """

    def __init__(self, url = 'http://localhost', idict=None):
        """
        url should really be host - TODO fix that when have sufficient code
        coverage and change _getURLOpener if needed
        """
        if  not idict:
            idict = {}
        dict.__init__(self, idict)
        
        #set up defaults
        self.setdefault("accept_type", 'text/html')
        self.setdefault("content_type", 'application/x-www-form-urlencoded')
        self.additionalHeaders = {}

        self.setdefault("host", url)

        # then update with the incoming dict
        self.update(idict)

        self['endpoint_components'] = urlparse.urlparse(self['host'])
        # If cachepath = None disable caching
        if 'cachepath' in idict and idict['cachepath'] is None:
            self["req_cache_path"] = None
        else:
            cache_dir = (self.cachePath(idict.get('cachepath'), \
                        idict.get('service_name')))
            self["cachepath"] = cache_dir
            self["req_cache_path"] = os.path.join(cache_dir, '.cache')
        self.setdefault("timeout", 600)
        self.setdefault("logger", logging)

        check_server_url(self['host'])
        # and then get the URL opener
        self.setdefault("conn", self._getURLOpener())


    def get(self, uri=None, data={}, incoming_headers={},
               encode = True, decode=True, contentType=None):
        """
        GET some data
        """
        return self.makeRequest(uri, data, 'GET', incoming_headers,
                                encode, decode, contentType)

    def post(self, uri=None, data={}, incoming_headers={},
               encode = True, decode=True, contentType=None):
        """
        POST some data
        """
        return self.makeRequest(uri, data, 'POST', incoming_headers,
                                encode, decode, contentType)

    def put(self, uri=None, data={}, incoming_headers={},
               encode = True, decode=True, contentType=None):
        """
        PUT some data
        """
        return self.makeRequest(uri, data, 'PUT', incoming_headers,
                                encode, decode, contentType)

    def delete(self, uri=None, data={}, incoming_headers={},
               encode = True, decode=True, contentType=None):
        """
        DELETE some data
        """
        return self.makeRequest(uri, data, 'DELETE', incoming_headers,
                                encode, decode, contentType)

    def makeRequest(self, uri=None, data={}, verb='GET',
            incoming_headers={}, encoder=True, decoder=True, contentType=None):
        """
        Make a request to the remote database. for a given URI. The type of
        request will determine the action take by the server (be careful with
        DELETE!). Data should be a dictionary of {dataname: datavalue}.

        Returns a tuple of the data from the server, decoded using the
        appropriate method the response status and the response reason, to be
        used in error handling.

        You can override the method to encode/decode your data by passing in an
        encoding/decoding function to this method. Your encoded data must end up
        as a string.

        """
        #TODO: User agent should be:
        # $client/$client_version (CMS)
        # $http_lib/$http_lib_version $os/$os_version ($arch)
        if  not contentType:
            contentType = self['content_type']
        headers = {"Content-type": contentType,
               "User-agent": "WMCore.Services.Requests/v001",
               "Accept": self['accept_type']}
        encoded_data = ''

        for key in self.additionalHeaders.keys():
            headers[key] = self.additionalHeaders[key]

        #And now overwrite any headers that have been passed into the call:
        #WARNING: doesn't work with deplate so only accept gzip 
        incoming_headers["accept-encoding"] = "gzip,identity"
        headers.update(incoming_headers)

        # If you're posting an attachment, the data might not be a dict
        #   please test against ConfigCache_t if you're unsure.
        #assert type(data) == type({}), \
        #        "makeRequest input data must be a dict (key/value pairs)"

        # There must be a better way to do this...
        def f():
            """Dummy function"""
            pass

        if verb != 'GET' and data:
            if type(encoder) == type(self.get) or type(encoder) == type(f):
                encoded_data = encoder(data)
            elif encoder == False:
                # Don't encode the data more than we have to
                #  we don't want to URL encode the data blindly,
                #  that breaks POSTing attachments... ConfigCache_t
                #encoded_data = urllib.urlencode(data)
                #  -- Andrew Melo 25/7/09
                encoded_data = data
            else:
                # Either the encoder is set to True or it's junk, so use
                # self.encode
                encoded_data = self.encode(data)
            headers["Content-length"] = len(encoded_data)
        elif verb == 'GET' and data:
            #encode the data as a get string
            uri = "%s?%s" % (uri, urllib.urlencode(data, doseq=True))

        headers["Content-length"] = str(len(encoded_data))

        assert type(encoded_data) == type('string'), \
            "Data in makeRequest is %s and not encoded to a string" \
                % type(encoded_data)

        self['conn'].request(method = verb, url = uri,
                                    body = encoded_data, headers = headers)
        response = self['conn'].getresponse()
        result = response.read()
        
        if response.status >= 400:
            e = HTTPException()
            setattr(e, 'req_data', encoded_data)
            setattr(e, 'req_headers', headers)
            setattr(e, 'url', uri)
            setattr(e, 'result', result)
            setattr(e, 'status', response.status)
            setattr(e, 'reason', response.reason)
            setattr(e, 'headers', response)
            raise e

        if type(decoder) == type(self.makeRequest) or type(decoder) == type(f):
            result = decoder(result)
        elif decoder != False:
            result = self.decode(result)
        #TODO: maybe just return result and response...
        return result, response.status, response.reason, False

    def encode(self, data):
        """
        encode data into some appropriate format, for now make it a string...
        """
        return urllib.urlencode(data, doseq=1)

    def decode(self, data):
        """
        decode data to some appropriate format, for now make it a string...
        """
        return data.__str__()

    def cachePath(self, given_path, service_name):
        """Return cache location"""
        if not service_name:
            service_name = 'REQUESTS'
        top = self.cacheTopPath(given_path, service_name)

        # deal with multiple Services that have the same service running and
        # with multiple users for a given Service
        if self.getUserName() is None:
            cachepath = os.path.join(top, self['endpoint_components'].netloc)
        else:
            cachepath = os.path.join(top, '%s-%s' \
                % (self.getUserName(), self.getDomainName()))

        try:
            # only we should be able to write to this dir
            os.makedirs(cachepath, stat.S_IRWXU)
        except OSError:
            if not os.path.isdir(cachepath):
                raise
            owner_readwriteexec(cachepath)

        return cachepath

    def cacheTopPath(self, given_path, service_name):
        """Where to cache results?

        Logic:
          o If passed in take that
          o Is the environment variable "SERVICE_NAME"_CACHE_DIR defined?
          o Is WMCORE_CACHE_DIR set
          o Generate a temporary directory
          """
        if given_path:
            return given_path
        user = str(os.getuid())
        # append user id so users don't clobber each other
        lastbit = os.path.join('.wmcore_cache_%s' % user, service_name.lower())
        for var in ('%s_CACHE_DIR' % service_name.upper(),
                    'WMCORE_CACHE_DIR'):
            if os.environ.get(var):
                firstbit = os.environ[var]
                break
        else:
            idir = tempfile.mkdtemp(prefix='.wmcore_cache_')
            # object to store temporary directory - cleaned up on destruction
            self['deleteCacheOnExit'] = TempDirectory(idir)
            return idir

        return os.path.join(firstbit, lastbit)

    def getDomainName(self):
        """Parse netloc info to get hostname"""
        return self['endpoint_components'].hostname

    def getUserName(self):
        """Parse netloc to get user"""
        return self['endpoint_components'].username

    def _getURLOpener(self):
        """
        method getting a secure (HTTPS) connection
        """
        key, cert = None, None
        if self['endpoint_components'].scheme == 'https':
            # only add certs to https requests
            # if we have a key/cert add to request,
            # if not proceed as not all https connections require them
            try:
                key, cert = self.getKeyCert()
            except Exception, ex:
                msg = 'No certificate or key found, authentication may fail'
                self['logger'].info(msg)
                self['logger'].debug(str(ex))
            
            uri = self['endpoint_components'].netloc
            http = HTTPSConnection(uri, key_file=key, cert_file=cert, 
                                   timeout = self["timeout"])
        elif self['endpoint_components'].scheme == 'http':
            uri = self['endpoint_components'].netloc
            http = HTTPConnection(uri, timeout = self["timeout"])
            
        return http

    def addBasicAuth(self, username, password):
        """Add basic auth headers to request"""
        auth_string = "Basic %s" % base64.encodestring('%s:%s' % (
                                            username, password)).strip()
        self.additionalHeaders["Authorization"] = auth_string

    def getKeyCert(self):
        """
       _getKeyCert_

       Get the user credentials if they exist, otherwise throw an exception.
       This code was modified from DBSAPI/dbsHttpService.py
        """
        cert = None
        key = None
        # Zeroth case is if the class has over ridden the key/cert and has it
        # stored in self
        if self.has_key('cert') and self.has_key('key' ) \
             and self['cert'] and self['key']:
            key = self['key']
            cert = self['cert']

        # Now we're trying to guess what the right cert/key combo is...
        # First preference to HOST Certificate, This is how it set in Tier0
        elif os.environ.has_key('X509_HOST_CERT'):
            cert = os.environ['X509_HOST_CERT']
            key = os.environ['X509_HOST_KEY']
        # Second preference to User Proxy, very common
        elif (os.environ.has_key('X509_USER_PROXY')) and \
                (os.path.exists( os.environ['X509_USER_PROXY'])):
            cert = os.environ['X509_USER_PROXY']
            key = cert

        # Third preference to User Cert/Proxy combinition
        elif os.environ.has_key('X509_USER_CERT'):
            cert = os.environ['X509_USER_CERT']
            key = os.environ['X509_USER_KEY']

        # TODO: only in linux, unix case, add other os case
        # look for proxy at default location /tmp/x509up_u$uid
        elif os.path.exists('/tmp/x509up_u'+str(os.getuid())):
            cert = '/tmp/x509up_u'+str(os.getuid())
            key = cert

        # if interactive we can use an encrypted certificate
        elif sys.stdin.isatty():
            if os.path.exists(os.environ['HOME'] + '/.globus/usercert.pem'):
                cert = os.environ['HOME'] + '/.globus/usercert.pem'
                if os.path.exists(os.environ['HOME'] + '/.globus/userkey.pem'):
                    key = os.environ['HOME'] + '/.globus/userkey.pem'
                else:
                    key = cert

        #Set but not found
        if  key and cert:
            if not os.path.exists(cert) or not os.path.exists(key):
                raise Exception('Request requires a host certificate and key',
                                  "WMCORE-11")

        # All looks OK, still doesn't guarantee proxy's validity etc.
        return key, cert

    def getCAPath(self):
        """
        _getCAPath_

        Return the path of the CA certificates. The check is loose in the pycurl_manager:
        is capath == None then the server identity is not verified. To enable this check
        you need to set either the X509_CERT_DIR variable or the cacert key of the request.
        """
        cacert = None
        if self.has_key('capath'):
            cacert = self['capath']
        elif os.environ.has_key("X509_CERT_DIR"):
            cacert = os.environ["X509_CERT_DIR"]
        return cacert

    
class JSONRequests(Requests):
    """
    Example implementation of Requests that encodes data to/from JSON.
    """
    def __init__(self, url = 'http://localhost:8080', idict={}):
        Requests.__init__(self, url, idict)
        self['accept_type'] = "application/json"
        self['content_type'] = "application/json"

    def encode(self, data):
        """
        encode data as json
        """
        encoder = JSONEncoder()
        thunker = JSONThunker()
        thunked = thunker.thunk(data)
        return encoder.encode(thunked)


    def decode(self, data):
        """
        decode the data to python from json
        """
        if data:
            decoder = JSONDecoder()
            thunker = JSONThunker()
            data =  decoder.decode(data)
            unthunked = thunker.unthunk(data)
            return unthunked
        else:
            return {}


class TempDirectory():
    """Directory that cleans up after itself"""
    def __init__(self, idir):
        self.dir = idir

    def __del__(self):
        shutil.rmtree(self.dir, ignore_errors = True)
