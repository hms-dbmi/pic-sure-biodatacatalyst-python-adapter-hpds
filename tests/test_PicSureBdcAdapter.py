import json
from PicSureClient import Connection
from PicSureHpdsLib.PicSureHpds import HpdsResourceConnection as BaseResourceConnection
from PicSureHpdsLib.PicSureHpds import Adapter as BaseAdapter
from PicSureHpdsLib.PicSureHpdsQuery import Query as BaseQuery
from PicSureBdcAdapter.PicSureBdcHpds import Adapter, ResourceConnection, Query
from PicSureBdcAdapter.QueryModifier import QueryModifier
import unittest
from unittest.mock import patch, MagicMock

class TestBdcAdapter(unittest.TestCase):

    @patch('PicSureClient.Connection')
    def test_Adapter_create(self, mock_picsure_connection):
        # Just have to put some kind of JSON response so that there is a value to parse
        mock_picsure_connection.return_value._api_obj.return_value.profile.return_value = '{"testjson":"awesome"}'

        conn = mock_picsure_connection()
        adapter = Adapter(conn)
        # test that it was created correctly
        self.assertIsInstance(adapter, BaseAdapter)

    @patch('PicSureClient.Connection')
    def test_ResourceConnection_create(self, mock_picsure_connection):
        # Just have to put some kind of JSON response so that there is a value to parse
        mock_picsure_connection.return_value._api_obj.return_value.profile.return_value = '{"testjson":"awesome"}'

        conn = mock_picsure_connection()
        test_uuid = "my-test-uuid"
        resource = ResourceConnection(conn, test_uuid)

        self.assertIsInstance(resource, BaseResourceConnection)
        self.assertEquals(resource.resource_uuid, test_uuid)

    @patch('PicSureClient.Connection')
    def test_Query_create_missing_queryTemplate(self, MockPicSureConnection):
        # Just have to put some kind of JSON response so that there is a value to parse
        MockPicSureConnection.return_value._api_obj.return_value.profile.return_value = '{"testjson":"awesome"}'

        conn = MockPicSureConnection()
        test_uuid = "my-test-uuid"
        resource = ResourceConnection(conn, test_uuid)

        query = resource.query()
        self.assertIsInstance(query, BaseQuery)
        self.assertIsInstance(query, Query)

    @patch('PicSureClient.Connection')
    def test_Query_create_with_queryTemplate(self, MockPicSureConnection):
        # Just have to put some kind of JSON response so that there is a value to parse
        MockPicSureConnection.return_value._api_obj.return_value.profile.return_value = '{"testjson":"awesome"}'

        conn = MockPicSureConnection()
        test_uuid = "my-test-uuid"
        resource = ResourceConnection(conn, test_uuid)
        queryTemplate = '{	"categoryFilters": {' \
                        '		"\\\\_harmonized_consent\\\\": ["consent1"],' \
                        '		"\\\\_topmed_consents\\\\": ["consent2"],' \
                        '		"\\\\_consents\\\\": ["consent3"]' \
                        '	},' \
                        '	"numericFilters": {},' \
                        '	"requiredFields": [],' \
                        '   "fields": ["\\\\_Topmed Study Accession with Subject ID\\\\", "\\\\_Parent Study Accession with Subject ID\\\\"],' \
                        '	"variantInfoFilters": [{' \
                        '		"categoryVariantInfoFilters": {},' \
                        '		"numericVariantInfoFilters": {}' \
                        '	}],' \
                        '	"expectedResultType": ["COUNT"]' \
                        '}'

        query = Query(conn, load_query=queryTemplate)

        # confirm caching of consents from query template
        self.assertDictEqual(query._default_query_consents, {
            'harmonized': {'\\_harmonized_consent\\': {'HpdsDataType': '','type': 'categorical','values': ['consent1']}},
            'topmed': {'\\_topmed_consents\\': {'HpdsDataType': '','type': 'categorical','values': ['consent2']}}
        })


    @patch('PicSureClient.Connection')
    def test_Query_consents_minimal(self, MockPicSureConnection):
        # Just have to put some kind of JSON response so that there is a value to parse
        MockPicSureConnection.return_value._api_obj.return_value.profile.return_value = '{"testjson":"awesome"}'

        conn = MockPicSureConnection()
        test_uuid = "my-test-uuid"
        resource = ResourceConnection(conn, test_uuid)
        queryTemplate = '{	"categoryFilters": {' \
                        '		"\\\\_harmonized_consent\\\\": ["consent1"],' \
                        '		"\\\\_topmed_consents\\\\": ["consent2"],' \
                        '		"\\\\_consents\\\\": ["consent3"]' \
                        '	},' \
                        '	"numericFilters": {},' \
                        '	"requiredFields": [],' \
                        '   "fields": ["\\\\_Topmed Study Accession with Subject ID\\\\", "\\\\_Parent Study Accession with Subject ID\\\\"],' \
                        '	"variantInfoFilters": [{' \
                        '		"categoryVariantInfoFilters": {},' \
                        '		"numericVariantInfoFilters": {}' \
                        '	}],' \
                        '	"expectedResultType": ["COUNT"]' \
                        '}'

        query = Query(conn, load_query=queryTemplate)

        # confirm that harmonized and topmed consents are removed
        filtered_query = query.buildQuery()
        self.assertIn("\\_consents\\", filtered_query['query']['categoryFilters'])
        self.assertNotIn("\\_harmonized_consent\\", filtered_query['query']['categoryFilters'])
        self.assertNotIn("\\_topmed_consents\\", filtered_query['query']['categoryFilters'])


    @patch('PicSureClient.Connection')
    def test_Query_consents_w_harmonized(self, MockPicSureConnection):
        # Just have to put some kind of JSON response so that there is a value to parse
        MockPicSureConnection.return_value._api_obj.return_value.profile.return_value = '{"testjson":"awesome"}'

        harmonized_path = QueryModifier.harmonizedPaths[0]

        conn = MockPicSureConnection()
        test_uuid = "my-test-uuid"
        resource = ResourceConnection(conn, test_uuid)
        queryTemplate = '{	"categoryFilters": {' \
                        '		"\\\\_harmonized_consent\\\\": ["consent1"],' \
                        '		"\\\\_topmed_consents\\\\": ["consent2"],' \
                        '		"\\\\_consents\\\\": ["consent3"]' \
                        '	},' \
                        '	"numericFilters": {},' \
                        '	"requiredFields": [],' \
                        '   "fields": ["\\\\_Topmed Study Accession with Subject ID\\\\", "\\\\_Parent Study Accession with Subject ID\\\\"],' \
                        '	"variantInfoFilters": [{' \
                        '		"categoryVariantInfoFilters": {},' \
                        '		"numericVariantInfoFilters": {}' \
                        '	}],' \
                        '	"expectedResultType": ["COUNT"]' \
                        '}'

        query = Query(conn, load_query=queryTemplate)
        query._lstFilter.data[harmonized_path] = {"HpdsDataType": "nothing", "type":"minmax"}

        # confirm that harmonized and topmed consents are removed
        filtered_query = query.buildQuery()
        self.assertIn("\\_consents\\", filtered_query['query']['categoryFilters'])
        self.assertIn("\\_harmonized_consent\\", filtered_query['query']['categoryFilters'])
        self.assertNotIn("\\_topmed_consents\\", filtered_query['query']['categoryFilters'])


    @patch('PicSureClient.Connection')
    def test_Query_consents_w_topmed(self, MockPicSureConnection):
        # Just have to put some kind of JSON response so that there is a value to parse
        MockPicSureConnection.return_value._api_obj.return_value.profile.return_value = '{"testjson":"awesome"}'

        conn = MockPicSureConnection()
        test_uuid = "my-test-uuid"
        resource = ResourceConnection(conn, test_uuid)
        queryTemplate = '{	"categoryFilters": {' \
                        '		"\\\\_harmonized_consent\\\\": ["consent1"],' \
                        '		"\\\\_topmed_consents\\\\": ["consent2"],' \
                        '		"\\\\_consents\\\\": ["consent3"]' \
                        '	},' \
                        '	"numericFilters": {},' \
                        '	"requiredFields": [],' \
                        '   "fields": ["\\\\_Topmed Study Accession with Subject ID\\\\", "\\\\_Parent Study Accession with Subject ID\\\\"],' \
                        '	"variantInfoFilters": [{' \
                        '		"categoryVariantInfoFilters": {},' \
                        '		"numericVariantInfoFilters": {}' \
                        '	}],' \
                        '	"expectedResultType": ["COUNT"]' \
                        '}'

        query = Query(conn, load_query=queryTemplate)
        query._lstFilter.data['genomic_info'] = {"HpdsDataType": "info", "type":"categorical", "values":["CHD8"]}

        # confirm that harmonized and topmed consents are removed
        filtered_query = query.buildQuery()
        self.assertIn("\\_consents\\", filtered_query['query']['categoryFilters'])
        self.assertIn("\\_topmed_consents\\", filtered_query['query']['categoryFilters'])
        self.assertNotIn("\\_harmonized_consent\\", filtered_query['query']['categoryFilters'])



    @patch('PicSureClient.Connection')
    def test_Query_consents_readded(self, MockPicSureConnection):
        # Just have to put some kind of JSON response so that there is a value to parse
        MockPicSureConnection.return_value._api_obj.return_value.profile.return_value = '{"testjson":"awesome"}'

        conn = MockPicSureConnection()
        test_uuid = "my-test-uuid"
        resource = ResourceConnection(conn, test_uuid)
        queryTemplate = '{	"categoryFilters": {' \
                        '		"\\\\_harmonized_consent\\\\": ["consent1"],' \
                        '		"\\\\_topmed_consents\\\\": ["consent2"],' \
                        '		"\\\\_consents\\\\": ["consent3"]' \
                        '	},' \
                        '	"numericFilters": {},' \
                        '	"requiredFields": [],' \
                        '   "fields": ["\\\\_Topmed Study Accession with Subject ID\\\\", "\\\\_Parent Study Accession with Subject ID\\\\"],' \
                        '	"variantInfoFilters": [{' \
                        '		"categoryVariantInfoFilters": {},' \
                        '		"numericVariantInfoFilters": {}' \
                        '	}],' \
                        '	"expectedResultType": ["COUNT"]' \
                        '}'

        query = Query(conn, load_query=queryTemplate)
        query.show()

        # confirm that harmonized and topmed consents are removed
        filtered_query = query.buildQuery()
        self.assertIn("\\_consents\\", filtered_query['query']['categoryFilters'])
        self.assertNotIn("\\_topmed_consents\\", filtered_query['query']['categoryFilters'])
        self.assertNotIn("\\_harmonized_consent\\", filtered_query['query']['categoryFilters'])

        # verify that harmonized and topmed consents are re-added
        harmonized_path = QueryModifier.harmonizedPaths[0]
        query._lstFilter.data['genomic_info'] = {"HpdsDataType": "info", "type":"categorical", "values":["CHD8"]}
        query._lstFilter.data[harmonized_path] = {"HpdsDataType": "nothing", "type":"minmax"}
        filtered_query = query.buildQuery()
        self.assertIn("\\_topmed_consents\\", filtered_query['query']['categoryFilters'])
        self.assertIn("\\_harmonized_consent\\", filtered_query['query']['categoryFilters'])

