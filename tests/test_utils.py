import sys

import unittest
from mock import MagicMock


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



if __name__ == '__main__':
    unittest.main()
