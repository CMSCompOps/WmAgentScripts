import sys

import unittest
import mock
from mock import MagicMock, patch, mock_open
import json


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
            result = open_json_file('filename')
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


if __name__ == '__main__':
    unittest.main()
