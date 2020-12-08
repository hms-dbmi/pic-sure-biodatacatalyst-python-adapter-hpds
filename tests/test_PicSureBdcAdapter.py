import json
from PicSureClient import Connection
from PicSureHpdsLib.PicSureHpds import HpdsResourceConnection as BaseResourceConnection
from PicSureHpdsLib.PicSureHpds import Adapter as BaseAdapter
from PicSureHpdsLib.PicSureHpdsQuery import Query as BaseQuery
from PicSureBdcAdapter.PicSureBdcHpds import Adapter, ResourceConnection, Query
from PicSureBdcAdapter.ConsentsModifier import ConsentsModifier
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

        harmonized_path = ConsentsModifier.harmonizedPaths[0]

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
        harmonized_path = ConsentsModifier.harmonizedPaths[0]
        query._lstFilter.data['genomic_info'] = {"HpdsDataType": "info", "type":"categorical", "values":["CHD8"]}
        query._lstFilter.data[harmonized_path] = {"HpdsDataType": "nothing", "type":"minmax"}
        filtered_query = query.buildQuery()
        self.assertIn("\\_topmed_consents\\", filtered_query['query']['categoryFilters'])
        self.assertIn("\\_harmonized_consent\\", filtered_query['query']['categoryFilters'])



    @patch('PicSureClient.Connection')
    def test_Query_consents_list(self, MockPicSureConnection):
        # Just have to put some kind of JSON response so that there is a value to parse
        MockPicSureConnection.return_value._api_obj.return_value.profile.return_value = '{"testjson":"awesome"}'

        conn = MockPicSureConnection()
        test_uuid = "my-test-uuid"
        resource = ResourceConnection(conn, test_uuid)

        resource._profile_info = json.loads('{"uuid": "376e7181-4093-4637-9f32-7d4c77b848bd", "email": "someone@gmail.com", "privileges": ["PRIV_FENCE_some_consent_here"], "queryScopes": ["Gene_with_variant", "Variant_class", "Variant_consequence_calculated", "Variant_frequency_as_text", "Variant_frequency_in_gnomAD", "Variant_severity", "\\\\DCC Harmonized data set\\\\", "\\\\NHLBI GO-ESP: Lung Cohorts Exome Sequencing Project (Asthma): Genetic variants affecting susceptibility and severity ( phs000422 )\\\\", "\\\\NHLBI TOPMed: African American Coronary Artery Calcification (AA CAC) ( phs001412 )\\\\", "\\\\NHLBI TOPMed: African American Sarcoidosis Genetics Resource ( phs001207 )\\\\", "\\\\NHLBI TOPMed: Boston Early-Onset COPD Study in the TOPMed Program ( phs000946 )\\\\", "\\\\NHLBI TOPMed: Cardiovascular Health Study ( phs000287 )\\\\", "\\\\NHLBI TOPMed: Cardiovascular Health Study ( phs001368 )\\\\", "\\\\NHLBI TOPMed: Cleveland Clinic Atrial Fibrillation Study ( phs000820 )\\\\", "\\\\NHLBI TOPMed: Cleveland Clinic Atrial Fibrillation Study ( phs001189 )\\\\", "\\\\NHLBI TOPMed: GeneSTAR (Genetic Study of Atherosclerosis Risk) ( phs001074 )\\\\", "\\\\NHLBI TOPMed: GeneSTAR (Genetic Study of Atherosclerosis Risk) ( phs001218 )\\\\", "\\\\NHLBI TOPMed: Genes-environments and Admixture in Latino Asthmatics (GALA II) Study ( phs000920 )\\\\", "\\\\NHLBI TOPMed: Genes-environments and Admixture in Latino Asthmatics (GALA II) Study ( phs001180 )\\\\", "\\\\NHLBI TOPMed: Genetic Epidemiology Network of Arteriopathy (GENOA) ( phs001238 )\\\\", "\\\\NHLBI TOPMed: Genetic Epidemiology Network of Arteriopathy (GENOA) ( phs001345 )\\\\", "\\\\NHLBI TOPMed: Genetic Epidemiology Network of Salt Sensitivity (GenSalt) ( phs000784 )\\\\", "\\\\NHLBI TOPMed: Genetic Epidemiology Network of Salt Sensitivity (GenSalt) ( phs001217 )\\\\", "\\\\NHLBI TOPMed: Genetic Epidemiology of COPD (COPDGene) in the TOPMed Program ( phs000179 )\\\\", "\\\\NHLBI TOPMed: Genetic Epidemiology of COPD (COPDGene) in the TOPMed Program ( phs000951 )\\\\", "\\\\NHLBI TOPMed: Genetics of Cardiometabolic Health in the Amish ( phs000956 )\\\\", "\\\\NHLBI TOPMed: Genetics of Lipid Lowering Drugs and Diet Network (GOLDN) ( phs001359 )\\\\", "\\\\NHLBI TOPMed: Heart and Vascular Health Study (HVH) ( phs000993 )\\\\", "\\\\NHLBI TOPMed: Heart and Vascular Health Study (HVH) ( phs001013 )\\\\", "\\\\NHLBI TOPMed: HyperGEN - Genetics of Left Ventricular (LV) Hypertrophy ( phs001293 )\\\\", "\\\\NHLBI TOPMed: MESA and MESA Family AA-CAC ( phs000209 )\\\\", "\\\\NHLBI TOPMed: MESA and MESA Family AA-CAC ( phs001416 )\\\\", "\\\\NHLBI TOPMed: MGH Atrial Fibrillation Study ( phs001001 )\\\\", "\\\\NHLBI TOPMed: MGH Atrial Fibrillation Study ( phs001062 )\\\\", "\\\\NHLBI TOPMed: Novel Risk Factors for the Development of Atrial Fibrillation in Women ( phs001040 )\\\\", "\\\\NHLBI TOPMed: Partners HealthCare Biobank ( phs001024 )\\\\", "\\\\NHLBI TOPMed: San Antonio Family Heart Study (WGS) ( phs001215 )\\\\", "\\\\NHLBI TOPMed: Study of African Americans, Asthma, Genes and Environment (SAGE) Study ( phs000921 )\\\\", "\\\\NHLBI TOPMed: The Cleveland Family Study (WGS) ( phs000284 )\\\\", "\\\\NHLBI TOPMed: The Cleveland Family Study (WGS) ( phs000954 )\\\\", "\\\\NHLBI TOPMed: The Genetic Epidemiology of Asthma in Costa Rica ( phs000988 )\\\\", "\\\\NHLBI TOPMed: The Genetics and Epidemiology of Asthma in Barbados ( phs001143 )\\\\", "\\\\NHLBI TOPMed: The Jackson Heart Study ( phs000286 )\\\\", "\\\\NHLBI TOPMed: The Jackson Heart Study ( phs000964 )\\\\", "\\\\NHLBI TOPMed: The Vanderbilt AF Ablation Registry ( phs000997 )\\\\", "\\\\NHLBI TOPMed: The Vanderbilt Atrial Fibrillation Registry ( phs001032 )\\\\", "\\\\NHLBI TOPMed: Trans-Omics for Precision Medicine Whole Genome Sequencing Project: ARIC ( phs000280 )\\\\", "\\\\NHLBI TOPMed: Trans-Omics for Precision Medicine Whole Genome Sequencing Project: ARIC ( phs001211 )\\\\", "\\\\NHLBI TOPMed: Whole Genome Sequencing and Related Phenotypes in the Framingham Heart Study ( phs000007 )\\\\", "\\\\NHLBI TOPMed: Whole Genome Sequencing and Related Phenotypes in the Framingham Heart Study ( phs000974 )\\\\", "\\\\NHLBI TOPMed: Whole Genome Sequencing of Venous Thromboembolism (WGS of VTE) ( phs001402 )\\\\", "\\\\NHLBI TOPMed: Women\'s Health Initiative (WHI) ( phs000200 )\\\\", "\\\\NHLBI TOPMed: Women\'s Health Initiative (WHI) ( phs001237 )\\\\"], "queryTemplate": "{\\"categoryFilters\\":{\\"\\\\\\\\_harmonized_consent\\\\\\\\\\":[\\"phs001001.c1\\",\\"phs001217.c1\\",\\"phs001217.c0\\",\\"phs001001.c2\\",\\"phs000956.c2\\",\\"phs001402.c1\\",\\"phs000956.c0\\",\\"phs001189.c1\\",\\"phs001143.c1\\",\\"phs001143.c0\\",\\"phs000988.c1\\",\\"phs000286.c4\\",\\"phs001207.c1\\",\\"phs000286.c3\\",\\"phs001207.c0\\",\\"phs000286.c2\\",\\"phs000286.c1\\",\\"phs000286.c0\\",\\"phs000200.c1\\",\\"phs000988.c0\\",\\"phs001040.c1\\",\\"phs000997.c1\\",\\"phs000007.c2\\",\\"phs000007.c1\\",\\"phs000179.c1\\",\\"phs000179.c2\\",\\"phs000179.c0\\",\\"phs001180.c2\\",\\"phs001293.c1\\",\\"phs001293.c2\\",\\"phs001293.c0\\",\\"phs000209.c1\\",\\"phs000209.c2\\",\\"phs001215.c0\\",\\"phs001238.c1\\",\\"phs001215.c1\\",\\"phs000921.c2\\",\\"phs001032.c1\\",\\"phs001013.c1\\",\\"phs001013.c2\\",\\"phs000280.c1\\",\\"phs000284.c1\\",\\"phs000280.c2\\",\\"phs000200.c2\\",\\"phs001218.c0\\",\\"phs001218.c2\\",\\"phs001189.c0\\",\\"phs000287.c1\\",\\"phs000287.c2\\",\\"phs000287.c3\\",\\"phs000287.c4\\",\\"phs001412.c2\\",\\"phs001024.c1\\",\\"phs001412.c1\\",\\"phs001412.c0\\"],\\"\\\\\\\\_topmed_consents\\\\\\\\\\":[\\"phs001001.c1\\",\\"phs001217.c1\\",\\"phs001001.c2\\",\\"phs001345.c1\\",\\"phs000820.c1\\",\\"phs000956.c2\\",\\"phs000946.c1\\",\\"phs001402.c1\\",\\"phs001368.c2\\",\\"phs001189.c1\\",\\"phs001368.c1\\",\\"phs001143.c1\\",\\"phs000988.c1\\",\\"phs000286.c4\\",\\"phs000286.c3\\",\\"phs001207.c1\\",\\"phs000286.c2\\",\\"phs000286.c1\\",\\"phs000422.c1\\",\\"phs000286.c0\\",\\"phs000200.c1\\",\\"phs001040.c1\\",\\"phs000951.c2\\",\\"phs000951.c1\\",\\"phs000997.c1\\",\\"phs001237.c1\\",\\"phs001237.c2\\",\\"phs000007.c2\\",\\"phs000007.c1\\",\\"phs000993.c1\\",\\"phs000179.c1\\",\\"phs000993.c2\\",\\"phs000179.c2\\",\\"phs000920.c2\\",\\"phs000179.c0\\",\\"phs001293.c1\\",\\"phs001180.c2\\",\\"phs001293.c2\\",\\"phs001368.c4\\",\\"phs000209.c1\\",\\"phs000209.c2\\",\\"phs000784.c1\\",\\"phs001074.c2\\",\\"phs001238.c1\\",\\"phs001215.c1\\",\\"phs000954.c1\\",\\"phs000921.c2\\",\\"phs001032.c1\\",\\"phs001013.c1\\",\\"phs001013.c2\\",\\"phs000280.c1\\",\\"phs000284.c1\\",\\"phs000280.c2\\",\\"phs000974.c1\\",\\"phs000200.c2\\",\\"phs001218.c2\\",\\"phs000974.c2\\",\\"phs000964.c3\\",\\"phs001062.c1\\",\\"phs000964.c4\\",\\"phs001062.c2\\",\\"phs001359.c1\\",\\"phs000964.c1\\",\\"phs000964.c2\\",\\"phs001211.c2\\",\\"phs000287.c1\\",\\"phs000287.c2\\",\\"phs001211.c1\\",\\"phs000287.c3\\",\\"phs000287.c4\\",\\"phs001416.c1\\",\\"phs001412.c2\\",\\"phs001024.c1\\",\\"phs001412.c1\\",\\"phs001416.c2\\"],\\"\\\\\\\\_consents\\\\\\\\\\":[\\"phs001001.c1\\",\\"phs001217.c1\\",\\"phs001217.c0\\",\\"phs001001.c2\\",\\"phs001345.c1\\",\\"phs000820.c1\\",\\"phs000956.c2\\",\\"phs000946.c1\\",\\"phs001368.c2\\",\\"phs001402.c1\\",\\"phs000956.c0\\",\\"phs001368.c1\\",\\"phs001189.c1\\",\\"phs001143.c1\\",\\"phs001143.c0\\",\\"phs000988.c1\\",\\"phs000286.c4\\",\\"phs001207.c1\\",\\"phs000286.c3\\",\\"phs000286.c2\\",\\"phs001207.c0\\",\\"phs000286.c1\\",\\"phs000422.c1\\",\\"phs000286.c0\\",\\"phs000200.c1\\",\\"phs000988.c0\\",\\"phs001040.c1\\",\\"phs000951.c2\\",\\"phs000951.c1\\",\\"phs000997.c1\\",\\"phs001237.c1\\",\\"phs001237.c2\\",\\"phs000007.c2\\",\\"phs000007.c1\\",\\"phs000993.c1\\",\\"phs000179.c1\\",\\"phs000920.c2\\",\\"phs000993.c2\\",\\"phs000179.c2\\",\\"phs000179.c0\\",\\"phs001180.c2\\",\\"phs001293.c1\\",\\"phs001293.c2\\",\\"phs001293.c0\\",\\"phs001368.c4\\",\\"phs000209.c1\\",\\"phs000209.c2\\",\\"phs001074.c0\\",\\"phs001215.c0\\",\\"phs000784.c1\\",\\"phs000784.c0\\",\\"phs001074.c2\\",\\"phs001238.c1\\",\\"phs001215.c1\\",\\"phs000954.c1\\",\\"phs000921.c2\\",\\"phs001032.c1\\",\\"phs001013.c1\\",\\"phs001013.c2\\",\\"phs000280.c1\\",\\"phs000284.c1\\",\\"phs000280.c2\\",\\"phs000974.c1\\",\\"phs001218.c0\\",\\"phs000200.c2\\",\\"phs001218.c2\\",\\"phs000974.c2\\",\\"phs001062.c1\\",\\"phs000964.c3\\",\\"phs001062.c2\\",\\"phs000964.c4\\",\\"phs001359.c1\\",\\"phs000964.c0\\",\\"phs001189.c0\\",\\"phs000964.c1\\",\\"phs000964.c2\\",\\"phs001211.c2\\",\\"phs000287.c1\\",\\"phs000287.c2\\",\\"phs001211.c1\\",\\"phs000287.c3\\",\\"phs000287.c4\\",\\"phs001416.c1\\",\\"phs001412.c2\\",\\"phs001024.c1\\",\\"phs001412.c1\\",\\"phs001416.c2\\",\\"phs001412.c0\\"]},\\"numericFilters\\":{},\\"requiredFields\\":[],\\"fields\\":[\\"\\\\\\\\_Topmed Study Accession with Subject ID\\\\\\\\\\",\\"\\\\\\\\_Parent Study Accession with Subject ID\\\\\\\\\\"],\\"variantInfoFilters\\":[{\\"categoryVariantInfoFilters\\":{},\\"numericVariantInfoFilters\\":{}}],\\"expectedResultType\\":[\\"COUNT\\"]}"}')
        consent_df = resource.list_consents()

        self.assertEqual(len(consent_df), 88, "expecting to have 88 records in the consents dataframe")

