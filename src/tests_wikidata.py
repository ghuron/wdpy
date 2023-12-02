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
        self.assertIsNone(Wikidata.load({'Q2', 'Q1'}))
        api_call.assert_called_with('wbgetentities', {'props': 'claims|info|labels|aliases', 'ids': 'Q1|Q2'})

    @mock.patch('wd.Wikidata.call', return_value=None)
    def test_load_items_single(self, api_call):
        self.assertIsNone(Wikidata.load({'Q3'}))
        api_call.assert_called_with('wbgetentities', {'props': 'claims|info|labels|aliases', 'ids': 'Q3'})

    @mock.patch('wd.Wikidata.request', return_value=None)
    def test_call_failed(self, _):
        self.assertIsNone(Wikidata.call('action', {'1': '1'}))

    @mock.patch('wd.Wikidata.call', return_value={'query': {'search': [{'title': 'Q1091618'}]}})
    def test_search(self, api_call):
        value = Wikidata.search('haswbstatement:"P3083=HD 1"')
        self.assertEqual('Q1091618', value)
        api_call.assert_called_with('query', {'list': 'search', 'srsearch': 'haswbstatement:"P3083=HD 1"'})


@mock.patch('wd.Wikidata.type_of', return_value='wikibase-item')
class TestReferences(TestCase):
    def setUp(self):
        Element.db_property, Element.db_ref = 'P3083', 'Q654724'
        self.wd = Element('')

    def test_add_external_id(self, _):
        self.wd.add_refs(claim := {})
        self.assertEqual(1, len(claim['references']))
        self.assertCountEqual(['Q654724'], Element.get_snaks(claim['references'][0], 'P248'))

    def test_existing_external_id(self, _):
        self.wd.add_refs(claim := ({'references': [{'snaks': {'P248': [Model.create_snak('P248', 'Q654724')]}}]}))
        self.assertEqual(1, len(claim['references']))
        self.assertCountEqual(['Q654724'], Element.get_snaks(claim['references'][0], 'P248'))

    def test_set_according_to(self, _):
        self.wd.add_refs(claim := ({'references': [{'snaks': {'P248': [Model.create_snak('P248', 'Q66061041')],
                                                              'P143': [Model.create_snak('P143', 'Q328')]
                                                              }}]}), {'Q66061041'})
        self.assertEqual(1, len(claim['references']))
        self.assertEqual(['Q66061041'], Element.get_snaks(claim['references'][0], 'P248'))
        self.assertEqual([], Element.get_snaks(claim['references'][0], 'P143'))
        self.assertCountEqual(['Q654724'], Element.get_snaks(claim['references'][0], 'P12132'))

    def test_add_accoridng_to(self, _):
        self.wd.add_refs(claim := {'references': [{'snaks': {'P248': [Model.create_snak('P248', 'Q66061041')],
                                                             'P12132': [Model.create_snak('P12132', 'Q1385430')],
                                                             }}]}, {'Q66061041'})
        self.assertEqual(1, len(claim['references']))
        self.assertEqual(['Q66061041'], Element.get_snaks(claim['references'][0], 'P248'))
        self.assertCountEqual(['Q654724', 'Q1385430'], Element.get_snaks(claim['references'][0], 'P12132'))

    def test_add_source(self, _):
        self.wd.add_refs(claim := {'references': [{'snaks': {'P248': [Model.create_snak('P248', 'Q66061041')]}}]},
                         {'Q222662'})
        self.assertEqual(2, len(claim['references']))
        self.assertCountEqual(['Q66061041'], Element.get_snaks(claim['references'][0], 'P248'))
        self.assertCountEqual([], Element.get_snaks(claim['references'][0], 'P12132'))
        self.assertCountEqual(['Q222662'], Element.get_snaks(claim['references'][1], 'P248'))
        self.assertCountEqual(['Q654724'], Element.get_snaks(claim['references'][1], 'P12132'))

    def test_explicit_aggregator_id(self, _):
        Element.db_property = 'P31'
        self.wd.add_refs(claim := {'references': [{'snaks': {'P248': [Model.create_snak('P248', 'Q654724')],
                                                             'P31': [Model.create_snak(Element.db_property, 'Q5')]}}]})
        self.assertEqual(1, len(claim['references']))
        self.assertCountEqual([], Element.get_snaks(claim['references'][0], Element.db_property))

    def test_remove_unconfirmed(self, _):
        self.wd.entity['claims'] = {'P31': [
            {'references': [{'snaks': {'P12132': [Model.create_snak('P12132', 'Q654724')]}}]},
            {'references': [{'snaks': {'P248': [Model.create_snak('P248', 'Q222662')]}}]},
            {'references': [{'snaks': {'P248': [Model.create_snak('P248', 'Q654724')]}, 'wdpy': 1},
                            {'snaks': {'P248': [Model.create_snak('P248', 'Q654724')]}}]},
        ]}
        self.wd.remove_unconfirmed({'P31'})
        self.assertIn('remove', self.wd.entity['claims']['P31'][0])
        self.assertNotIn('remove', self.wd.entity['claims']['P31'][1])
        self.assertNotIn('remove', self.wd.entity['claims']['P31'][2])


class TestModel(TestCase):
    @mock.patch('wd.Wikidata.type_of', return_value='time')
    def testIgnoreInsignificantDatePart(self, _):
        self.assertIsNotNone(
            Model.find_claim({'datavalue': {'value': {'time': '+1999-12-31T00:00:00Z', 'precision': 9}}},
                             [Model.create_claim(Model.create_snak('P575', '1999'))]))

    def test_format_float(self):
        self.assertEqual('0.12345679', Model.format_float('0.123456789', 8))
        self.assertEqual(0, Decimal(Model.format_float('+0E-7', 8)))

    def test_date_parser(self):
        self.assertIsNone(Model.parse_date(''))
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
        self.assertDictEqual({'labels': {}, 'claims': {}}, self.wd.entity)
        load_items.assert_called_with({'Q1'})

    @mock.patch('wd.Wikidata.type_of', return_value='wikibase-item')
    def test_obtain_claims_empty_entity(self, _):
        claim = self.wd.obtain_claim(Element.create_snak('P31', 'Q5'))
        self.assertEqual('P31', claim['mainsnak']['property'])
        self.assertEqual('Q5', claim['mainsnak']['datavalue']['value']['id'])
