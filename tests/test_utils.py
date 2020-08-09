import sys
import httplib

import unittest
import json
import mock
from mock import MagicMock, patch, mock_open

from StringIO import StringIO

sys.modules['dbs'] = MagicMock()
sys.modules['dbs.apis'] = MagicMock()
sys.modules['dbs.apis.dbsClient'] = MagicMock()

reqmgr_url = "cmsweb.cern.ch"
test_dataset = "/TTJets_mtop1695_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/RunIIWinter15GS-MCRUN2_71_V1-v1/GEN-SIM"


class ContextualStringIO(StringIO):
    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False


def mock_make_x509_conn(url=reqmgr_url, max_try=5):
    tries = 0
    while tries < max_try:
        try:
            conn = httplib.HTTPSConnection(url)
            return conn
        except Exception as e:
            print e
            tries += 1
            pass
    return None


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
        from WmAgentScripts.utils import getSubscriptions
        with patch('WmAgentScripts.utils.make_x509_conn', mock_make_x509_conn):
            response = getSubscriptions(
                url=reqmgr_url,
                dataset=test_dataset)
            self.assertEqual(response['dataset'], [])
            self.assertEqual(response['instance'], 'prod')
            self.assertEqual(response['request_call'], 'subscriptions')
            self.assertEqual(
                response['request_url'],
                'http://cmsweb.cern.ch:7001/phedex/datasvc/json/prod/subscriptions')


class TestListRequests(unittest.TestCase):

    def testListRequests(self):

        from WmAgentScripts.utils import listRequests
        with patch('WmAgentScripts.utils.make_x509_conn', mock_make_x509_conn):
            response = listRequests(
                url=reqmgr_url,
                dataset=test_dataset,
                site=None)
            self.assertDictEqual(
                response,
                {
                    'T1_ES_PIC_Disk': [
                        445269,
                        453022],
                    'T2_CH_CSCS': [454057],
                    'T1_FR_CCIN2P3_Disk': [
                        441511,
                        460098],
                    'T2_US_Purdue': [
                        445274,
                        454061],
                    'XT1_UK_RAL_Disk': [454098],
                    'T0_CH_CERN_MSS': [
                        441510,
                        775255,
                        783438],
                    'T2_UK_SGrid_RALPP': [454063],
                    'T2_FR_GRIF_LLR': [454064],
                    'T2_BE_UCL': [454065],
                    'T2_ES_IFCA': [454066],
                    'T2_DE_RWTH': [454067],
                    'T2_FR_IPHC': [454101],
                    'T2_KR_KNU': [
                        454070,
                        454730],
                    'T2_DE_DESY': [
                        445267,
                        454071],
                    'T2_IT_Legnaro': [454073],
                    'T2_US_Caltech': [
                        445273,
                        454074],
                    'T1_DE_KIT_Disk': [
                        445265,
                        454075],
                    'T2_UK_London_Brunel': [454076],
                    'T2_RU_JINR': [454077],
                    'T2_IT_Pisa': [454078],
                    'T1_US_FNAL_Disk': [
                        445271,
                        454079],
                    'T2_EE_Estonia': [454080],
                    'T2_IT_Rome': [454081],
                    'T2_US_Florida': [454082],
                    'T2_FR_GRIF_IRFU': [454083],
                    'T1_IT_CNAF_Disk': [
                        445266,
                        454084],
                    'T2_FI_HIP': [454062],
                    'T1_RU_JINR_Disk': [454085],
                    'T2_UK_London_IC': [454086],
                    'T2_IT_Bari': [454087],
                    'T2_US_Nebraska': [
                        445276,
                        454088],
                    'T2_FR_CCIN2P3': [454089],
                    'T2_US_UCSD': [454090],
                    'T2_ES_CIEMAT': [
                        454091,
                        457804],
                    'T2_RU_IHEP': [454092],
                    'T2_US_Wisconsin': [
                        445268,
                        454093],
                    'T2_HU_Budapest': [454094],
                    'T2_CN_Beijing': [454095],
                    'T2_US_MIT': [454096],
                    'T2_BE_IIHE': [454097],
                    'T2_CH_CERN': [454103],
                    'T2_PT_NCG_Lisbon': [454069],
                    'T2_US_Vanderbilt': [454100],
                    'T2_BR_SPRACE': [454102]})

            response = listRequests(
                url='cmsweb.cern.ch',
                dataset=test_dataset,
                site='T1_ES_PIC_Disk')
            self.assertDictEqual(
                response, {
                    'T1_ES_PIC_Disk': [
                        445269, 453022]})


class TestListCustodial(unittest.TestCase):

    def testListCustodial(self):

        from WmAgentScripts.utils import listCustodial
        with patch('WmAgentScripts.utils.make_x509_conn', mock_make_x509_conn):
            response = listCustodial(
                url=reqmgr_url,
            )
            print response
            self.assertDictEqual(response,
                                 {'T1_US_FNAL_MSS': [2345013,
                                                      2345083,
                                                      2345230,
                                                      2345350,
                                                      2345586,
                                                      2345858,
                                                      2346380,
                                                      2346956,
                                                      2347154,
                                                      2347520,
                                                      2347851,
                                                      2349029,
                                                      2349077,
                                                      2349378,
                                                      2349379,
                                                      2349684,
                                                      2350365,
                                                      2350577,
                                                      2350849,
                                                      2351209,
                                                      2351236,
                                                      2352060,
                                                      2352170,
                                                      2352228,
                                                      2352416,
                                                      2352695,
                                                      2352781,
                                                      2352859,
                                                      2353136,
                                                      2353137,
                                                      2353267,
                                                      2353341,
                                                      2353361,
                                                      2353408,
                                                      2353415,
                                                      2353500,
                                                      2353597,
                                                      2353796,
                                                      2353830,
                                                      2353952,
                                                      2353963,
                                                      2354169,
                                                      2354231,
                                                      2354415,
                                                      2354580,
                                                      2354583,
                                                      2354684,
                                                      2354922,
                                                      2354937,
                                                      2354988,
                                                      2355007,
                                                      2355246,
                                                      2355284,
                                                      2355657],
                                  'T1_FR_CCIN2P3_MSS': [1319254,
                                                         1319259,
                                                         1319315,
                                                         1321435]})


class TestListSubscriptions(unittest.TestCase):

    def testListSubscriptions(self):

        from WmAgentScripts.utils import listSubscriptions
        with patch('WmAgentScripts.utils.make_x509_conn', mock_make_x509_conn):
            response = listSubscriptions(
                url=reqmgr_url,
                dataset=test_dataset
            )
            self.assertDictEqual(response, {})


class TestPass_to_dynamo(unittest.TestCase):

    def test_pass_to_dynamo(self):

        class MockResponseStringIo:
            response = {"result": "OK"}

            def __init__(self, *args, **kwargs):
                pass

            def request(self, *args, **kwargs):
                pass

            def getresponse(self):
                return ContextualStringIO(json.dumps(self.response))

        from WmAgentScripts.utils import pass_to_dynamo
        with patch('WmAgentScripts.utils.make_x509_conn', MockResponseStringIo):
            response = pass_to_dynamo(
                items=["items1", "item2"],
                N=10
            )
            self.assertTrue(response)
        MockResponseStringIo.response = {"result": "Not OK"}
        with patch('WmAgentScripts.utils.make_x509_conn', MockResponseStringIo):
            response = pass_to_dynamo(
                items=["items1", "item2"],
                N=10
            )
            self.assertFalse(response)


class TesCheckDownTime(unittest.TestCase):

    def testCheckDownTime(self):

        from WmAgentScripts.utils import checkDownTime

        with patch('WmAgentScripts.utils.make_x509_conn', mock_make_x509_conn):
            response = checkDownTime()
            self.assertFalse(response)


class TestIsJson(unittest.TestCase):

    def test_is_json(self):
        from WmAgentScripts.utils import is_json

        test_json = {
            "first": {"a": "A"},
            "second": "B"
        }

        self.assertTrue(is_json(json.dumps(test_json)))

        with self.assertRaises(TypeError):
            self.assertFalse(is_json(test_json))


class TestReadFile(unittest.TestCase):

    def test_read_file(self):

        from WmAgentScripts.utils import read_file
        test_json = {
            "first": {"a": "A"},
            "second": "B"
        }
        test_data = "test data"

        with patch('WmAgentScripts.utils.sendLog', return_value=None):
            mock_open_json = mock_open(read_data=json.dumps(test_json))
            with patch('__builtin__.open', mock_open_json):
                content = json.loads(read_file('filename.json'))
                self.assertDictEqual(content, test_json)

            mock_open_file = mock_open(read_data=test_data)
            with patch('__builtin__.open', mock_open_file):
                content = read_file('filename')
                self.assertEqual(content, test_data)

                textcontent = read_file('filename.json')
                self.assertEqual(textcontent, '{}')


class TestGetWMStats(unittest.TestCase):

    def test_getWMStats(self):
        class MockResponseStringIo:
            def __init__(self, *args, **kwargs):
                self.response = None, 404

            def request(self, *args, **kwargs):
                self.response = {"result": [200]}

            def getresponse(self):
                return ContextualStringIO(json.dumps(self.response))

        from WmAgentScripts.utils import getWMStats
        with patch('WmAgentScripts.utils.make_x509_conn', MockResponseStringIo):
            response = getWMStats(url='http://someurl.com/')


class TestCheckTransferApproval(unittest.TestCase):

    def test_checkTransferApproval(self):

        class MockResponseStringIo:

            def __init__(self, *args, **kwargs):
                self.response = None

            def request(self, *args, **kwargs):
                self.response = {"phedex": {
                    "request": [{
                        "node": [
                            {
                                "name": "someSite",
                                "decision": "approved",
                            },
                            {
                                "name": "someSite1",
                                "decision": "pending",
                            },
                        ],
                    }]}
                }

            def getresponse(self):
                return ContextualStringIO(json.dumps(self.response))

        from WmAgentScripts.utils import checkTransferApproval
        with patch('WmAgentScripts.utils.make_x509_conn', MockResponseStringIo):
            response = checkTransferApproval(
                url='http://someurl.com/',
                phedexid='someid'
            )
            self.assertDictEqual(
                response, {
                    'someSite': True,
                    'someSite1': False})


class TestGetNodesId(unittest.TestCase):

    def test_getNodesId(self):

        from WmAgentScripts.utils import getNodesId
        with patch('WmAgentScripts.utils.make_x509_conn', mock_make_x509_conn):
            response = getNodesId(
                url=reqmgr_url
            )
            self.assertDictEqual(response,
                                 {'T3_US_PuertoRico': '1521',
                                  'T3_RU_MEPhI': '2261',
                                  'T2_FI_HIP': '37',
                                  'T2_UK_SGrid_RALPP': '56',
                                  'T2_FR_GRIF_LLR': '34',
                                  'T3_IT_Trieste': '1481',
                                  'T3_TW_NTU_HEP': '1501',
                                  'T2_KR_KNU': '43',
                                  'T3_US_UMD': '681',
                                  'T3_US_Colorado': '741',
                                  'T1_UK_RAL_Disk': '2301',
                                  'T3_IT_Napoli': '68',
                                  'T3_US_Kansas': '581',
                                  'T1_IT_CNAF_Disk': '661',
                                  'T3_CH_CERN_OpenData': '2241',
                                  'T2_IT_Bari': '21',
                                  'T3_US_HEPCloud': '2221',
                                  'T2_US_UCSD': '62',
                                  'T2_RU_IHEP': '38',
                                  'T3_US_Vanderbilt_EC2': '1841',
                                  'T3_US_JHU': '1181',
                                  'T3_BY_NCPHEP': '1761',
                                  'T1_RU_JINR_Buffer': '2121',
                                  'T2_US_Vanderbilt': '242',
                                  'T3_US_UCR': '102',
                                  'T3_TW_NCU': '801',
                                  'T1_IT_CNAF_MSS': '8',
                                  'T2_CH_CSCS': '27',
                                  'T2_UA_KIPT': '341',
                                  'T2_PK_NCP': '1003',
                                  'T1_IT_CNAF_Buffer': '7',
                                  'T3_US_Brown': '1341',
                                  'T3_US_UCD': '1241',
                                  'T2_FR_IPHC': '181',
                                  'T3_US_OSU': '1201',
                                  'T3_GR_IASA_GR': '602',
                                  'T3_US_TAMU': '1441',
                                  'T2_IT_Rome': '55',
                                  'T2_UK_London_Brunel': '46',
                                  'T3_US_TTU': '123',
                                  'T2_EE_Estonia': '32',
                                  'T2_IN_TIFR': '281',
                                  'T1_UK_RAL_Buffer': '18',
                                  'T2_CN_Beijing': '22',
                                  'T1_RU_JINR_MSS': '2122',
                                  'T2_US_Florida': '33',
                                  'T1_US_FNAL_MSS': '10',
                                  'T3_GR_IASA_HG': '601',
                                  'T3_US_Princeton_ICSE': '1021',
                                  'T3_IT_MIB': '1401',
                                  'T3_US_FNALXEN': '1161',
                                  'T3_US_Rutgers': '701',
                                  'T1_FR_CCIN2P3_Buffer': '13',
                                  'T3_IR_IPM': '1621',
                                  'T2_US_Wisconsin': '65',
                                  'T2_HU_Budapest': '26',
                                  'T2_DE_RWTH': '54',
                                  'T2_BR_SPRACE': '58',
                                  'T2_CH_CERN': '1561',
                                  'T2_BR_UERJ': '36',
                                  'T3_MX_Cinvestav': '1281',
                                  'T3_US_FNALLPC': '881',
                                  'T1_US_FNAL_Disk': '1781',
                                  'T3_KR_KISTI': '1921',
                                  'T1_ES_PIC_MSS': '17',
                                  'T3_IN_VBU': '2081',
                                  'T3_IT_Firenze': '1041',
                                  'T0_CH_CERN_MSS': '2',
                                  'T2_ES_IFCA': '541',
                                  'T3_US_UVA': '1321',
                                  'T3_TH_CHULA': '1981',
                                  'T3_US_NotreDame': '1361',
                                  'T2_DE_DESY': '29',
                                  'T3_US_UIowa': '71',
                                  'T3_HU_Debrecen': '1941',
                                  'T2_US_Caltech': '28',
                                  'T3_CH_CMSAtHome': '2321',
                                  'T3_FR_IPNL': '421',
                                  'T1_US_FNAL_Buffer': '9',
                                  'T3_BG_UNI_SOFIA': '2021',
                                  'T1_ES_PIC_Buffer': '15',
                                  'T1_UK_RAL_MSS': '19',
                                  'T1_RU_JINR_Disk': '1745',
                                  'T3_CN_PKU': '521',
                                  'T2_UK_London_IC': '47',
                                  'T2_US_Nebraska': '51',
                                  'T2_ES_CIEMAT': '59',
                                  'T3_US_Princeton': '70',
                                  'T1_FR_CCIN2P3_Disk': '1861',
                                  'T3_DE_Karlsruhe': '66',
                                  'T2_KR_KISTI': '2281',
                                  'T3_KR_UOS': '1601',
                                  'T3_IT_Perugia': '69',
                                  'T1_ES_PIC_Disk': '16',
                                  'T2_CH_CERNBOX': '2141',
                                  'T3_US_Minnesota': '67',
                                  'T1_FR_CCIN2P3_MSS': '14',
                                  'T2_TR_METU': '381',
                                  'T2_AT_Vienna': '63',
                                  'T2_US_Purdue': '53',
                                  'T2_TW_NCHC': '2181',
                                  'T3_US_Rice': '901',
                                  'T3_HR_IRB': '1882',
                                  'T1_DE_KIT_MSS': '1262',
                                  'T2_BE_UCL': '24',
                                  'T3_US_FIT': '1101',
                                  'T2_UK_SGrid_Bristol': '25',
                                  'T2_PT_NCG_Lisbon': '1221',
                                  'T2_IT_Legnaro': '45',
                                  'T1_DE_KIT_Disk': '1821',
                                  'T2_RU_ITEP': '40',
                                  'T3_US_SDSC': '2001',
                                  'T2_RU_JINR': '42',
                                  'T2_IT_Pisa': '52',
                                  'T2_GR_Ioannina': '761',
                                  'T3_US_MIT': '1744',
                                  'T3_US_UCSB': '2061',
                                  'T2_MY_UPM_BIRUNI': '1903',
                                  'T0_CH_CERN_Export': '1',
                                  'T2_FR_GRIF_IRFU': '82',
                                  'T3_US_UMiss': '1381',
                                  'T2_PL_Swierk': '1961',
                                  'T3_RU_FIAN': '1641',
                                  'T2_FR_CCIN2P3': '1081',
                                  'T2_PL_Warsaw': '64',
                                  'T2_US_MIT': '50',
                                  'T2_BE_IIHE': '23',
                                  'T2_RU_INR': '561',
                                  'T1_DE_KIT_Buffer': '1261',
                                  'T3_CH_PSI': '821',
                                  'T3_US_Baylor': '1721',
                                  'T3_IT_Bologna': '1541'})


class TestGetDatasetFileLocations(unittest.TestCase):

    def test_getDatasetFileLocations(self):

        from WmAgentScripts.utils import getDatasetFileLocations

        with patch('WmAgentScripts.utils.make_x509_conn', mock_make_x509_conn):
            response = getDatasetFileLocations(
                url=reqmgr_url,
                dataset=test_dataset
            )
            self.assertDictEqual(
                response, {})


class TestFindCustodialLocation(unittest.TestCase):
    def test_findCustodialLocation(self):
        from WmAgentScripts.utils import findCustodialLocation
        with patch('WmAgentScripts.utils.make_x509_conn', mock_make_x509_conn):
            response = findCustodialLocation(
                url=reqmgr_url,
                dataset=test_dataset,
                with_completion=True
            )
            print response
            self.assertEqual(
                response, ([], None))


class TestInvalidateFiles(unittest.TestCase):

    def test_invalidateFiles(self):

        class MockResponseStringIo:
            response = {"result": "OK"}

            def __init__(self, *args, **kwargs):
                pass

            def request(self, *args, **kwargs):
                pass

            def getresponse(self):
                return ContextualStringIO(json.dumps(self.response))

        from WmAgentScripts.utils import invalidateFiles

        with patch('WmAgentScripts.utils.httplib.HTTPSConnection', MockResponseStringIo):
            response = invalidateFiles(
                files=["file1", "file2"],
            )
            self.assertTrue(response)

        MockResponseStringIo.response = {"result": "Not OK"}
        with patch('WmAgentScripts.utils.httplib.HTTPSConnection', MockResponseStringIo):
            response = invalidateFiles(
                files=["file1", "file2"],
            )
            self.assertFalse(response)


class TestGetConfigurationFile(unittest.TestCase):

    def setUp(self):
        class MockResponseStringIo:

            def __init__(self, *args, **kwargs):
                self.response = None

            def request(self, *args, **kwargs):
                self.response = "Test1 line 1\nTest2 line 2\nTest3 line 3"

            def getresponse(self):
                return ContextualStringIO(self.response)

        self.mockresponse = MockResponseStringIo

    def test_getConfigurationFile(self):
        from WmAgentScripts.utils import getConfigurationFile, getConfigurationLine

        with patch('WmAgentScripts.utils.make_x509_conn', self.mockresponse):
            response = getConfigurationFile(
                url='http://someurl.com/',
                cacheid='cacheid'
            )
            self.assertEqual(
                response, "Test1 line 1\nTest2 line 2\nTest3 line 3")

    def test_getConfigurationLine(self):
        from WmAgentScripts.utils import getConfigurationLine

        with patch('WmAgentScripts.utils.make_x509_conn', self.mockresponse):

            response = getConfigurationLine(
                url='http://someurl.com/',
                cacheid='cacheid',
                token='Test2')

            self.assertEqual(response, "Test2 line 2")


class TestGetWorkflowByCampaign(unittest.TestCase):

    def test_getWorkflowByCampaign(self):

        class MockResponseStringIo:

            def __init__(self, *args, **kwargs):
                self.response = None

            def request(self, *args, **kwargs):
                self.response = {"result":
                                 [{"data": [
                                     {
                                         "name": "someSite",
                                     },
                                     {
                                         "name": "someSite1",
                                     },
                                 ],
                                 }]
                                 }

            def getresponse(self):
                return ContextualStringIO(json.dumps(self.response))

        from WmAgentScripts.utils import getWorkflowByCampaign

        with patch('WmAgentScripts.utils.make_x509_conn', MockResponseStringIo):
            response = getWorkflowByCampaign(
                url='http://someurl.com/',
                campaign='somecampaign',
                details=False
            )
            self.assertEqual(
                response, [{'data': [{'name': 'someSite'}, {'name': 'someSite1'}]}])

            response = getWorkflowByCampaign(
                url='http://someurl.com/',
                campaign='somecampaign',
                details=True
            )
            self.assertEqual(
                response, [[{'name': 'someSite'}, {'name': 'someSite1'}]])


if __name__ == '__main__':
    unittest.main()
