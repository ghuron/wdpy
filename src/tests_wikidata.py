#!/usr/bin/python3
from decimal import Decimal
from unittest import TestCase, mock

from wikidata import WikiData


class TestWikiData(TestCase):
    @classmethod
    @mock.patch.multiple(WikiData, __abstractmethods__=set())
    def setUp(cls):
        cls.wd = WikiData('0000 0001 2197 5163')

    def test_format_float(self):
        self.assertEqual('0.12345679', WikiData.format_float('0.123456789', 8))

    def test_format_zero(self):
        self.assertEqual(0, Decimal(WikiData.format_float('+0E-7', 8)))

    def test_obtain_claims_empty_entity(self):
        claim = self.wd.obtain_claim(WikiData.create_snak('P31', 'Q5'))
        self.assertEqual('P31', claim['mainsnak']['property'])
        self.assertEqual('Q5', claim['mainsnak']['datavalue']['value']['id'])

    @mock.patch('wikidata.WikiData.api_call', return_value=None)
    def test_load_items_none(self, api_call):
        self.assertIsNone(WikiData.load_items(['Q1', 'Q2']))
        api_call.assert_called_with('wbgetentities', {'props': 'claims|info|labels', 'ids': 'Q1|Q2'})

    @mock.patch('wikidata.WikiData.api_call', return_value=None)
    def test_load_items_single(self, api_call):
        self.assertIsNone(WikiData.load_items(['Q3']))
        api_call.assert_called_with('wbgetentities', {'props': 'claims|info|labels', 'ids': 'Q3'})

    @mock.patch('logging.log')
    def test_trace_without_entity(self, info):
        self.wd.trace('test')
        info.assert_called_with(20, 'test')
        self.wd.entity = None
        self.wd.trace('test')
        info.assert_called_with(20, 'test')

    @mock.patch('wikidata.WikiData.api_call', return_value={'query': {'search': [{'title': 'Q1091618'}]}})
    def test_api_search(self, api_call):
        value = WikiData.api_search('haswbstatement:"P3083=HD 1"')
        self.assertEqual('Q1091618', value)
        api_call.assert_called_with('query', {'list': 'search', 'srsearch': 'haswbstatement:"P3083=HD 1"'})

    def test_obtain_claim_self_reference(self):
        self.wd.qid = 'Q5'
        self.wd.obtain_claim({'datavalue': {'value': 'id'}, 'property': 'P213'})  # should not throw an exception
        self.assertIsNone(self.wd.obtain_claim(WikiData.create_snak('P397', 'Q5')))

    @mock.patch('wikidata.WikiData.load_items', return_value=None)
    def test_prepare_data_null_items(self, load_items):
        self.wd.qid = 'Q1'
        self.wd.prepare_data()
        load_items.assert_called_with(['Q1'])

    def test_date_parser(self):
        self.assertIsNone(WikiData.parse_date(''))
        self.assertEqual('+1987-00-00T00:00:00Z', WikiData.parse_date('1987')['time'])
        self.assertEqual(9, WikiData.parse_date('1987')['precision'])
        self.assertEqual(0, WikiData.parse_date('1987')['timezone'])
        self.assertEqual(0, WikiData.parse_date('1987')['before'])
        self.assertEqual(0, WikiData.parse_date('1987')['after'])
        self.assertEqual('http://www.wikidata.org/entity/Q1985727', WikiData.parse_date('1987')['calendarmodel'])
        self.assertEqual('+2009-04-00T00:00:00Z', WikiData.parse_date('2009-04')['time'])
        self.assertEqual(10, WikiData.parse_date('2009-04')['precision'])
        self.assertEqual('+2009-04-12T00:00:00Z', WikiData.parse_date('2009-04-12')['time'])
        self.assertEqual(11, WikiData.parse_date('2009-4-12')['precision'])
        self.assertEqual('+2009-04-02T00:00:00Z', WikiData.parse_date('2009-04-2')['time'])
        self.assertEqual('+3456-02-01T00:00:00Z', WikiData.parse_date('1/2/3456')['time'])
        self.assertEqual('+1903-01-00T00:00:00Z', WikiData.parse_date('01/1903')['time'])
        self.assertIsNone(WikiData.parse_date('29/16/1924'))


class TestAddRefs(TestCase):
    @classmethod
    @mock.patch.multiple(WikiData, __abstractmethods__=set())
    def setUp(cls):
        wd = WikiData('0000 0001 2197 5163')
        WikiData.db_property = 'P213'
        WikiData.db_ref = 'Q423048'
        wd.entity = {'claims': {}}
        cls.wd = wd

    def test_add_refs_when_no_external_id(self):
        claim = {}
        self.wd.add_refs(claim, set())
        self.assertEqual('Q423048', claim['references'][0]['snaks']['P248'][0]['datavalue']['value']['id'])
        self.assertEqual('0000 0001 2197 5163', claim['references'][0]['snaks']['P213'][0]['datavalue']['value'])

    def test_add_missing_foreign_id(self):
        self.wd.obtain_claim(self.wd.create_snak('P213', '0000 0001 2197 5163'))  # add claim with external id
        claim = {}
        self.wd.add_refs(claim, set())  # add without external id
        self.assertNotIn('P213', claim['references'][0]['snaks'])
        self.assertEqual('Q423048', claim['references'][0]['snaks']['P248'][0]['datavalue']['value']['id'])

        self.wd.entity = {'claims': {}}  # remove claim with external id
        self.wd.add_refs(claim, set())
        self.assertEqual('0000 0001 2197 5163', claim['references'][0]['snaks']['P213'][0]['datavalue']['value'])

    def test_add_refs_without_foreign_id_if_other_sources(self):
        claim = {}
        self.wd.add_refs(claim, {'Q51905050'})
        self.assertEqual('Q423048', claim['references'][0]['snaks']['P248'][0]['datavalue']['value']['id'])
        self.assertNotIn('P213', claim['references'][0]['snaks'])

    def test_add_refs_2_equal_sources(self):
        claim = {}
        self.wd.add_refs(claim, {'Q51905050'})
        self.wd.add_refs(claim, {'Q51905050'})
        self.assertEqual(2, len(claim['references']))
        self.assertIn(claim['references'][0]['snaks']['P248'][0]['datavalue']['value']['id'], ['Q51905050', 'Q423048'])
        self.assertIn(claim['references'][1]['snaks']['P248'][0]['datavalue']['value']['id'], ['Q51905050', 'Q423048'])

    def test_add_refs_empty_after_source(self):
        claim = {}
        self.wd.add_refs(claim, {'Q51905050'})
        self.wd.add_refs(claim, set())

    def test_remove_P143(self):
        claim = {'references': [{'snaks': {'P248': [WikiData.create_snak('P248', 'Q423048')],
                                           'P143': [WikiData.create_snak('P143', 'Q328')]}}]}
        self.wd.add_refs(claim, set())
        self.assertIn('P248', claim['references'][0]['snaks'])
        self.assertNotIn('P143', claim['references'][0]['snaks'])

    def test_try_to_add_second_id(self):
        claim = {}
        self.wd.add_refs(claim, set())
        self.wd.external_id = '0000 0001 2146 438X'
        self.wd.add_refs(claim, set())
        self.assertEqual('0000 0001 2197 5163', claim['references'][0]['snaks']['P213'][0]['datavalue']['value'])


class TestFindClaim(TestCase):
    @classmethod
    @mock.patch.multiple(WikiData, __abstractmethods__=set())
    def setUp(cls):
        cls.wd = WikiData('0000 0001 2197 5163')

    def testIgnoreInsignificantDatePart(self):
        self.assertIsNotNone(WikiData.find_claim({'time': '+1999-12-31T00:00:00Z', 'precision': 9},
                                                 [self.wd.obtain_claim(WikiData.create_snak('P575', '1999'))]))
