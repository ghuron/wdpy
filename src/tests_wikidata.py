#!/usr/bin/python3
from decimal import Decimal
from unittest import TestCase, mock
from unittest.mock import MagicMock

from requests import exceptions

from wd import Wikidata, Model, Element


class TestWikiData(TestCase):
    @mock.patch('requests.Session.get', return_value=MagicMock(status_code=200, content='get-response'))
    def test_request_get_200(self, mock_get):
        self.assertEqual('get-response', Wikidata.request('https://test.test').content)
        mock_get.assert_called_with('https://test.test')

    @mock.patch('requests.Session.get', return_value=MagicMock(status_code=400, content='get-response'))
    def test_request_get_404(self, _):
        self.assertIsNone(Wikidata.request('https://test.test'))

    @mock.patch('requests.Session.get', side_effect=exceptions.ConnectionError)
    @mock.patch('logging.error')
    def test_request_get_exception(self, mock_error, mock_get):
        self.assertIsNone(Wikidata.request('https://test.test'))
        mock_get.assert_called_with('https://test.test')
        mock_error.assert_called_with('https://test.test exception:  POST {}')

    @mock.patch('requests.Session.post', return_value=MagicMock(status_code=200, content='post-response'))
    def test_request_post_200(self, _):
        self.assertEqual('post-response', Wikidata.request("https://test.test", data={'1': 1}).content)

    @mock.patch('wd.Wikidata.call', return_value=None)
    def test_load_items_none(self, api_call):
        self.assertIsNone(Wikidata.load(['Q1', 'Q2']))
        api_call.assert_called_with('wbgetentities', {'props': 'claims|info|labels|aliases', 'ids': 'Q1|Q2'})

    @mock.patch('wd.Wikidata.call', return_value=None)
    def test_load_items_single(self, api_call):
        self.assertIsNone(Wikidata.load(['Q3']))
        api_call.assert_called_with('wbgetentities', {'props': 'claims|info|labels|aliases', 'ids': 'Q3'})

    @mock.patch('wd.Wikidata.request', return_value=None)
    def test_call_failed(self, _):
        self.assertIsNone(Wikidata.call('action', {'1': '1'}))

    @mock.patch('wd.Wikidata.call', return_value={'query': {'search': [{'title': 'Q1091618'}]}})
    def test_search(self, api_call):
        value = Wikidata.search('haswbstatement:"P3083=HD 1"')
        self.assertEqual('Q1091618', value)
        api_call.assert_called_with('query', {'list': 'search', 'srsearch': 'haswbstatement:"P3083=HD 1"'})


class TestAddRefs(TestCase):
    @classmethod
    @mock.patch.multiple(Element, __abstractmethods__=set())
    def setUp(cls):
        wd = Element('0000 0001 2197 5163')
        Element.db_property = 'P213'
        Element.db_ref = 'Q423048'
        wd.__entity = {'claims': {}}
        cls.wd = wd

    def test_add_refs_when_no_external_id(self):
        claim = {}
        self.wd.add_refs(claim, set())
        self.assertEqual('Q423048', claim['references'][0]['snaks']['P248'][0]['datavalue']['value']['id'])
        self.assertEqual('0000 0001 2197 5163', claim['references'][0]['snaks']['P213'][0]['datavalue']['value'])

    def test_add_missing_foreign_id(self):
        self.wd.obtain_claim(Model.create_snak('P213', '0000 0001 2197 5163'))  # add claim with external id
        new_claim = {}
        self.wd.add_refs(new_claim, set())  # add without external id
        self.assertNotIn('P213', new_claim['references'][0]['snaks'])
        self.assertEqual('Q423048', new_claim['references'][0]['snaks']['P248'][0]['datavalue']['value']['id'])

        self.wd.entity['claims'] = {}  # remove claim with external id
        self.wd.add_refs(new_claim, set())
        self.assertEqual('0000 0001 2197 5163', new_claim['references'][0]['snaks']['P213'][0]['datavalue']['value'])

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
        claim = {'references': [{'snaks': {'P248': [Element.create_snak('P248', 'Q423048')],
                                           'P143': [Element.create_snak('P143', 'Q328')]}}]}
        self.wd.add_refs(claim, set())
        self.assertIn('P248', claim['references'][0]['snaks'])
        self.assertNotIn('P143', claim['references'][0]['snaks'])

    def test_try_to_add_second_id(self):
        claim = {}
        self.wd.add_refs(claim, set())
        self.wd.external_id = '0000 0001 2146 438X'
        self.wd.add_refs(claim, set())
        self.assertEqual('0000 0001 2197 5163', claim['references'][0]['snaks']['P213'][0]['datavalue']['value'])


class TestModel(TestCase):
    @mock.patch('wd.Wikidata.type_of', return_value='time')
    def testIgnoreInsignificantDatePart(self, _):
        self.assertIsNotNone(
            Model.find_claim({'datavalue': {'value': {'time': '+1999-12-31T00:00:00Z', 'precision': 9}}},
                             [Model.create_claim(Model.create_snak('P575', '1999'))]))

    def test_format_float(self):
        self.assertEqual('0.12345679', Element.format_float('0.123456789', 8))
        self.assertEqual(0, Decimal(Element.format_float('+0E-7', 8)))

    def test_date_parser(self):
        self.assertIsNone(Element.parse_date(''))
        self.assertEqual('+1987-00-00T00:00:00Z', Model.parse_date('1987')['time'])
        self.assertEqual(9, Model.parse_date('1987')['precision'])
        self.assertEqual(0, Model.parse_date('1987')['timezone'])
        self.assertEqual(0, Model.parse_date('1987')['before'])
        self.assertEqual(0, Model.parse_date('1987')['after'])
        self.assertEqual('http://www.wikidata.org/entity/Q1985727', Model.parse_date('1987')['calendarmodel'])
        self.assertEqual('+2009-04-00T00:00:00Z', Model.parse_date('2009-04')['time'])
        self.assertEqual(10, Model.parse_date('2009-04')['precision'])
        self.assertEqual('+2009-04-12T00:00:00Z', Model.parse_date('2009-04-12')['time'])
        self.assertEqual(11, Model.parse_date('2009-4-12')['precision'])
        self.assertEqual('+2009-04-02T00:00:00Z', Model.parse_date('2009-04-2')['time'])
        self.assertEqual('+3456-02-01T00:00:00Z', Model.parse_date('1/2/3456')['time'])
        self.assertEqual('+1903-01-00T00:00:00Z', Model.parse_date('01/1903')['time'])
        self.assertIsNone(Model.parse_date('29/16/1924'))

    @mock.patch('wd.Wikidata.type_of', return_value='wikibase-item')
    def test_qualifier_filter(self, _):
        self.assertTrue(Model.qualifier_filter({'qualifiers': {}}, {}))
        self.assertFalse(Model.qualifier_filter({'qualifiers': {'P972': 'Q1'}}, {}))
        q2 = {'qualifiers': {'P972': [Element.create_snak('P972', 'Q2')]}}
        self.assertFalse(Model.qualifier_filter({'qualifiers': {'P1227': 'Q2'}}, q2))
        self.assertFalse(Model.qualifier_filter({'qualifiers': {'P972': 'Q1'}}, q2))
        self.assertTrue(Model.qualifier_filter({'qualifiers': {'P972': 'Q2'}}, q2))


class TestElement(TestCase):
    @classmethod
    @mock.patch.multiple(Element, __abstractmethods__=set())
    def setUp(cls):
        cls.wd = Element('0000 0001 2197 5163')

    @mock.patch('logging.log')
    def test_trace_without_entity(self, info):
        self.wd.trace('test')
        info.assert_called_with(20, 'test')
        self.wd.__entity = None
        self.wd.trace('test')
        info.assert_called_with(20, 'test')

    @mock.patch('wd.Wikidata.load', return_value=None)
    @mock.patch('wd.Wikidata.type_of', return_value='wikibase-item')
    def test_obtain_claim_self_reference(self, _, __):
        self.wd.qid = 'Q5'
        self.wd.obtain_claim({'datavalue': {'value': 'id'}, 'property': 'P213'})  # should not throw an exception
        self.assertIsNone(self.wd.obtain_claim(Model.create_snak('P397', 'Q5')))

    @mock.patch('wd.Wikidata.load', return_value=None)
    def test_prepare_data_null_items(self, load_items):
        self.wd.qid = 'Q1'
        self.assertDictEqual({'label': {}, 'claims': {}}, self.wd.entity)
        load_items.assert_called_with(['Q1'])

    @mock.patch('wd.Wikidata.type_of', return_value='wikibase-item')
    def test_obtain_claims_empty_entity(self, _):
        claim = self.wd.obtain_claim(Element.create_snak('P31', 'Q5'))
        self.assertEqual('P31', claim['mainsnak']['property'])
        self.assertEqual('Q5', claim['mainsnak']['datavalue']['value']['id'])
