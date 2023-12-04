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

    @mock.patch('wd.Wikidata.load')
    @mock.patch('wd.Wikidata.type_of', return_value='wikibase-item')
    def test_preload(self, mock_typeof, mock_load):
        mock_typeof.return_value = 'time'
        (fake_redirect := Element('')).entity['redirects'] = {'to': 'Q2222'}
        fake_redirect.obtain_claim(Element.create_snak('P577', '2022-02-02'))
        mock_load.return_value = {'Q1111': fake_redirect.entity, 'Q3333': Element('').entity}
        Element._redirects['Q4444'] = 'Q2222'

        mock_typeof.return_value = 'wikibase-item'
        Element.preload([{'snaks': {'P248': [Model.create_snak('P248', 'Q1111')]}},
                         {'snaks': {'P248': [Model.create_snak('P248', 'Q3333')]}},
                         {'snaks': {'P248': [Model.create_snak('P248', 'Q4444')]}},
                         {'snaks': {'P248': [Model.create_snak('P248', 'Q654724')]}}])
        mock_load.assert_called_once_with({'Q1111', 'Q3333'})
        self.assertEqual('Q2222', Element._redirects['Q1111'])
        self.assertNotIn('Q1111', Element._pub_dates)
        self.assertEqual(20220202, Element._pub_dates['Q2222'])
        self.assertIsNone(Element._pub_dates['Q3333'])

    @mock.patch('wd.Wikidata.type_of', return_value='wikibase-item')
    def test_remove_duplicates(self, _):
        references = [{'snaks': {'P248': [Model.create_snak('P248', 'Q1111')],
                                 'P12132': [Model.create_snak('P12132', 'Q5555')]}},
                      {'snaks': {'P248': [Model.create_snak('P248', 'Q2222')]}},
                      {'snaks': {'P248': [Model.create_snak('P248', 'Q3333')],
                                 'P12132': [Model.create_snak('P12132', 'Q4444')]}, 'wdpy': 1},
                      {'snaks': {'P248': [Model.create_snak('P248', 'Q1111')],
                                 'P12132': [Model.create_snak('P12132', 'Q5555'),
                                            Model.create_snak('P12132', 'Q6666')]}}]
        self.wd._redirects = {'Q3333': 'Q2222'}
        Element.db_ref = 'Q4444'
        self.assertEqual(2, len(result := self.wd.remove_duplicates(references)))
        self.assertCountEqual(['Q1111'], Element.get_snaks(result[0], 'P248'))
        self.assertCountEqual(['Q5555', 'Q6666'], Element.get_snaks(result[0], 'P12132'))
        self.assertCountEqual(['Q2222'], Element.get_snaks(result[1], 'P248'))
        self.assertCountEqual(['Q4444'], Element.get_snaks(result[1], 'P12132'))
        self.assertEqual(1, result[1]['wdpy'])


@mock.patch('wd.Wikidata.type_of', return_value='wikibase-item')
class TestKeepOnlyBestValue(TestCase):
    @classmethod
    def setUp(cls):
        cls.wd = Element('0000 0001 2197 5163')

    def test_remove_if_no_qualifier(self, _):
        self.wd.obtain_claim(Model.create_snak('P31', 'Q5'))
        self.wd.keep_only_best_value('P31', 'P2241')
        self.assertCountEqual([], self.wd.entity['claims']['P31'])

    @mock.patch('wd.Element.get_latest_ref_date', return_value=20241231)
    def test_remove_second_claim_with_latest_publication_date(self, _, __):
        claim = self.wd.obtain_claim(Model.create_snak('P31', 'Q523'))
        self.wd.obtain_claim(Model.create_snak('P31', 'Q524'))
        self.wd.keep_only_best_value('P31')
        self.assertCountEqual([claim], self.wd.entity['claims']['P31'])

    @mock.patch('wd.Element.get_latest_ref_date', return_value=20241231)
    def test_no_modification_if_no_value_encountered(self, _, __):
        claim1 = self.wd.obtain_claim(Model.create_snak('P31', 'Q523'))
        claim1['mainsnak'].pop('datavalue')
        self.wd.obtain_claim(Model.create_snak('P31', 'Q524'))
        self.wd.obtain_claim(Model.create_snak('P31', 'Q523'))
        self.wd.keep_only_best_value('P31')
        self.assertCountEqual([claim1], self.wd.entity['claims']['P31'])

    @mock.patch('wd.Element.get_latest_ref_date', return_value=20241231)
    def test_process_groups_separately(self, _, __):
        claim1 = self.wd.obtain_claim(Model.create_snak('P31', 'Q523'))
        claim1['qualifiers'] = {'P2241': [Model.create_snak('P2241', 'Q111')]}
        claim2 = self.wd.obtain_claim(Model.create_snak('P31', 'Q524'))
        claim2['qualifiers'] = {'P2241': [Model.create_snak('P2241', 'Q222')]}
        self.wd.keep_only_best_value('P31', 'P2241')
        self.assertNotIn('remove', claim1)
        self.assertNotIn('remove', claim2)
