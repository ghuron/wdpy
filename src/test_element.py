from unittest import TestCase, mock

from wd import Element, Model


class TestElement(TestCase):
    @classmethod
    def setUp(cls):
        cls.wd = (item := Element('0000 0001 2197 5163'))
        item.qid = None

    @mock.patch('wd.Wikidata.load', return_value=None)
    @mock.patch('wd.Wikidata.type_of', return_value='wikibase-item')
    def test_obtain_claim_self_reference(self, _, __):
        self.wd.qid = 'Q5'
        self.assertIsNone(self.wd.obtain_claim(Model.create_snak('P397', 'Q5')))

    @mock.patch('wd.Wikidata.load', return_value=None)
    def test_prepare_data_null_items(self, load_items):
        self.wd.qid = 'Q1'
        self.assertDictEqual({'labels': {}, 'claims': {}}, self.wd.entity)
        load_items.assert_called_with({'Q1'})

    @mock.patch('wd.Wikidata.type_of', return_value='wikibase-item')
    def test_obtain_claims_empty_entity(self, _):
        claim = self.wd.obtain_claim(Model.create_snak('P31', 'Q5'))
        self.assertEqual('P31', claim['mainsnak']['property'])
        self.assertEqual('Q5', claim['mainsnak']['datavalue']['value']['id'])

    @mock.patch('wd.Wikidata.type_of', return_value='time')
    def testIgnoreInsignificantDatePart(self, _):
        self.wd.obtain_claim(Model.create_snak('P575', '1999'))
        (snak1999 := Model.create_snak('P575', '1999-12-31'))['datavalue']['value']['precision'] = 9
        self.assertIsNotNone(self.wd.find_claim(snak1999))


@mock.patch('wd.Wikidata.type_of', return_value='wikibase-item')
class TestKeepOnlyBestValue(TestCase):
    @classmethod
    def setUp(cls):
        Model._config = {}
        cls.wd = (item := Element('0000 0001 2197 5163'))
        item.qid = None

    def test_remove_if_no_qualifier(self, _):
        Model._config = {'P31': {'id': 'P2241'}}
        self.wd.obtain_claim(Model.create_snak('P31', 'Q5'))
        self.wd.remove_all_but_one('P31')
        self.assertCountEqual([], self.wd.entity['claims']['P31'])

    @mock.patch('wd.Claim.get_latest_ref_date', return_value=20241231)
    def test_remove_second_claim_with_latest_publication_date(self, _, __):
        claim = self.wd.obtain_claim(Model.create_snak('P31', 'Q523'))
        self.wd.obtain_claim(Model.create_snak('P31', 'Q524'))
        self.wd.remove_all_but_one('P31')
        self.assertCountEqual([claim], self.wd.entity['claims']['P31'])

    @mock.patch('wd.Claim.get_latest_ref_date', return_value=20241231)
    def test_no_modification_if_no_value_encountered(self, _, __):
        claim1 = self.wd.obtain_claim(Model.create_snak('P31', 'Q523'))
        claim1['mainsnak'].pop('datavalue')
        self.wd.obtain_claim(Model.create_snak('P31', 'Q524'))
        self.wd.obtain_claim(Model.create_snak('P31', 'Q523'))
        self.wd.remove_all_but_one('P31')
        self.assertCountEqual([claim1], self.wd.entity['claims']['P31'])

    @mock.patch('wd.Claim.get_latest_ref_date', return_value=20241231)
    def test_process_groups_separately(self, _, __):
        Element._config = {'P31': {'id': 'P2241'}}
        claim1 = self.wd.obtain_claim(Model.create_snak('P31', 'Q523'))
        claim1['qualifiers'] = {'P2241': [Model.create_snak('P2241', 'Q111')]}
        claim2 = self.wd.obtain_claim(Model.create_snak('P31', 'Q524'))
        claim2['qualifiers'] = {'P2241': [Model.create_snak('P2241', 'Q222')]}
        self.wd.remove_all_but_one('P31')
        self.assertNotIn('remove', claim1)
        self.assertNotIn('remove', claim2)
