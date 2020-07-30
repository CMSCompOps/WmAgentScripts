import sys

import unittest
import json
import mock
from mock import MagicMock, patch, mock_open

from StringIO import StringIO


class ContextualStringIO(StringIO):
    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False


class TestDeepUpdate(unittest.TestCase):

    def setUp(self):

        self.u = {
            "first": {"a": "A"},
            "second": "B"
        }
        self.d = {
            "first": {"a": "B"},
            "second": "C"
        }
        sys.modules['dbs'] = MagicMock()
        sys.modules['dbs.apis'] = MagicMock()
        sys.modules['dbs.apis.dbsClient'] = MagicMock()
        from WmAgentScripts.utils import deep_update
        self.deep_update = deep_update

    def test_deep_update(self):

        self.assertDictEqual(self.d, self.deep_update(self.d, self.u))


class TestUnifiedConfiguration(unittest.TestCase):

    def setUp(self):
        self.test_json = json.dumps({
            "email": {
                "value": ["test@cern.ch", "test@testmail.com"],
                "description": "The list of people that get the emails notifications"
            }
        })

    def test_get_json(self):

        from WmAgentScripts.utils import unifiedConfiguration, open_json_file
        mock_open_json = mock_open(read_data=self.test_json)
        with patch('__builtin__.open', mock_open_json):
            open_json_file('filename')
            uc = unifiedConfiguration(configFile='fake_file_path')
            self.assertEqual(
                uc.get("email"), ["test@cern.ch", "test@testmail.com"])

        with self.assertRaises(SystemExit):
            self.assertEqual(
                uc.get("cernmails"), [
                    "test@cern.ch", "test@testmail.com"])

    def test_get_mongodb(self):

        # TODO: mock db and write tests
        pass


class TestEsHeader(unittest.TestCase):

    def test_es_header(self):
        from WmAgentScripts.utils import es_header
        mock_date = mock_open(read_data=u"fake_entrypointname:fake_password")
        with patch('__builtin__.open', mock_date):
            result = es_header()
            fake_result = {
                'Content-Type': 'application/json',
                'Authorization': 'Basic ZmFrZV9lbnRyeXBvaW50bmFtZTpmYWtlX3Bhc3N3b3Jk'}
            self.assertDictEqual(result, fake_result)


class TestUrlEncodeParams(unittest.TestCase):

    def test_url_encode_params(self):
        from WmAgentScripts.utils import url_encode_params
        params = {"query1": "test1", "query2": ["test2", "test3"]}
        result = url_encode_params(params)
        self.assertEqual(result, "query2=test2&query2=test3&query1=test1")


class TestGET(unittest.TestCase):

    def test_get(self):

        class MockResponse:
            def __init__(self, *args, **kwargs):
                self.response = None, 404

            def request(self, *args, **kwargs):
                if args[1] == 'test.json':
                    self.response = {"key1": "value1"}, 200
                elif args[1] == 'anothertest.json':
                    self.response = {"key2": "value2"}, 200
                else:
                    self.response = None, 404

            def getresponse(self):
                return self.response

        from WmAgentScripts.utils import GET
        with patch('WmAgentScripts.utils.make_x509_conn', MockResponse):
            response = GET(
                url='http://someurl.com/',
                there='test.json',
                l=False)
            self.assertDictEqual({"key1": "value1"}, response[0])
            self.assertEqual(200, response[1])

            response = GET(
                url='http://someurl.com/',
                there='anothertest.json',
                l=False)
            self.assertDictEqual({"key2": "value2"}, response[0])
            self.assertEqual(200, response[1])

            response = GET(url=None, there=None, l=False)
            self.assertEqual((None, 404), response)

    def test_get_json_object(self):

        class MockResponseStringIo:
            def __init__(self, *args, **kwargs):
                self.response = None, 404

            def request(self, *args, **kwargs):
                if args[1] == 'test.json':
                    self.response = {"key1": "value1"}, 200
                elif args[1] == 'anothertest.json':
                    self.response = {"key2": "value2"}, 200
                else:
                    self.response = None, 404

            def getresponse(self):
                return ContextualStringIO(json.dumps(self.response))

        from WmAgentScripts.utils import GET
        with patch('WmAgentScripts.utils.make_x509_conn', MockResponseStringIo):
            response = GET(
                url='http://someurl.com/',
                there='test.json',
                l=True)
            self.assertDictEqual({"key1": "value1"}, response[0])
            self.assertEqual(200, response[1])

            response = GET(
                url='http://someurl.com/',
                there='anothertest.json',
                l=True)
            self.assertDictEqual({"key2": "value2"}, response[0])
            self.assertEqual(200, response[1])

            response = GET(url=None, there=None, l=True)
            self.assertEqual([None, 404], response)


class TestGetSubscription(unittest.TestCase):

    def testGetSubscription(self):

        class MockResponseStringIo:
            def __init__(self, *args, **kwargs):
                self.response = None, 404

            def request(self, *args, **kwargs):
                self.response = {"phedex": "value1"}

            def getresponse(self):
                return ContextualStringIO(json.dumps(self.response))

        from WmAgentScripts.utils import getSubscriptions
        with patch('WmAgentScripts.utils.make_x509_conn', MockResponseStringIo):
            response = getSubscriptions(
                url='http://someurl.com/',
                dataset="somedataset")
            self.assertEqual(response, "value1")


class TestListRequests(unittest.TestCase):

    def testListRequests(self):

        class MockResponseStringIo:
            def __init__(self, *args, **kwargs):
                self.response = None, 404

            def request(self, *args, **kwargs):
                self.response = {"phedex": {
                    "request": [{
                        "node": [
                            {"name": "someSite"},
                            {"name": "someSite1"}
                        ],
                        "id": "someId"
                    }]}
                }

            def getresponse(self):
                return ContextualStringIO(json.dumps(self.response))

        from WmAgentScripts.utils import listRequests
        with patch('WmAgentScripts.utils.make_x509_conn', MockResponseStringIo):
            response = listRequests(
                url='http://someurl.com/',
                dataset="somedataset",
                site=None)
            self.assertDictEqual(
                response, {
                    'someSite1': ['someId'], 'someSite': ['someId']})
            response = listRequests(
                url='http://someurl.com/',
                dataset="somedataset",
                site='someSite')
            self.assertDictEqual(response, {'someSite': ['someId']})


class TestListRequests(unittest.TestCase):

    def testListCustodial(self):

        class MockResponseStringIo:
            def __init__(self, *args, **kwargs):
                self.response = None, 404

            def request(self, *args, **kwargs):
                self.response = {"phedex": {
                    "request": [{
                        "node": [
                            {"name": "someSite"},
                            {"name": "someSite1"}
                        ],
                        "id": "someId",
                        "type": "xfer"
                    }]}
                }

            def getresponse(self):
                return ContextualStringIO(json.dumps(self.response))

        from WmAgentScripts.utils import listCustodial
        with patch('WmAgentScripts.utils.make_x509_conn', MockResponseStringIo):
            response = listCustodial(
                url='http://someurl.com/',
            )
            print response
            self.assertDictEqual(
                response, {
                    'someSite1': ['someId'], 'someSite': ['someId']})


class TestListSubscriptions(unittest.TestCase):

    def testListSubscriptions(self):

        class MockResponseStringIo:
            def __init__(self, *args, **kwargs):
                self.response = None, 404

            def request(self, *args, **kwargs):
                self.response = {"phedex": {
                    "request": [{
                        "node": [{
                                "name": "someSite",
                                "decision": "approved",
                                "time_decided": 1300
                            },
                            {
                                "name": "someSite1",
                                "decision": "pending",
                                "time_decided": 1400
                            },
                            {
                                "name": "someSiteMSS",
                                "decision": "pending",
                                "time_decided": 1400
                            },
                        ],
                        "id": "someId",
                        "type": "xfer"
                    }]}
                }

            def getresponse(self):
                return ContextualStringIO(json.dumps(self.response))

        from WmAgentScripts.utils import listSubscriptions
        with patch('WmAgentScripts.utils.make_x509_conn', MockResponseStringIo):
            response = listSubscriptions(
                url='http://someurl.com/',
                dataset='somedataset'
            )
            self.assertDictEqual(
                response, {'someSite': ('someId', True), 'someSite1': ('someId', False)})

            response = listSubscriptions(
                url='http://someurl.com/',
                dataset='somedataset',
                within_sites=['someSite']
            )
            self.assertDictEqual(
                response, {'someSite': ('someId', True)})

if __name__ == '__main__':
    unittest.main()
