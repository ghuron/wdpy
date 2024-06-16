#!/usr/bin/python3
import json
from decimal import Decimal
from unittest import TestCase, mock
from unittest.mock import MagicMock

from wd import Wikidata


class TestRequest(TestCase):
    from requests import exceptions

    @mock.patch('requests.Session.get', return_value=MagicMock(status_code=200, content='get-response'))
    def test_get_200(self, mock_get):
        self.assertEqual('get-response', Wikidata.request('https://test.test').content)
        mock_get.assert_called_with('https://test.test')

    @mock.patch('requests.Session.get', return_value=MagicMock(status_code=400, content='get-response'))
    @mock.patch('logging.error')
    def test_get_404(self, _, __):
        self.assertIsNone(Wikidata.request('https://test.test'))

    @mock.patch('requests.Session.get', side_effect=exceptions.ConnectionError)
    @mock.patch('logging.error')
    def test_get_exception(self, mock_error, mock_get):
        self.assertIsNone(Wikidata.request('https://test.test'))
        mock_get.assert_called_with('https://test.test')
        mock_error.assert_called_with('https://test.test exception:  POST {}')

    @mock.patch('requests.Session.post', return_value=MagicMock(status_code=200, content='post-response'))
    def test_post_200(self, _):
        self.assertEqual('post-response', Wikidata.request("https://test.test", data={'1': 1}).content)


class TestCall(TestCase):
    @mock.patch('wd.Wikidata.request', return_value=None)
    def test_call_failed(self, _):
        self.assertIsNone(Wikidata.call('do', {'p': '1'}))

    @mock.patch('wd.Wikidata.request', return_value=MagicMock(json=lambda: json.loads('')))
    @mock.patch('logging.error')
    def test_malformed_json(self, _, __):
        self.assertIsNone(Wikidata.call('do', {'p': '1'}))


class TestLoad(TestCase):
    @mock.patch('wd.Wikidata.call', return_value=None)
    def test_multiple_items_none(self, api_call):
        self.assertIsNone(Wikidata.load({'Q2', 'Q1'}))
        api_call.assert_called_with('wbgetentities', {'props': 'claims|info|labels|aliases', 'ids': 'Q1|Q2'})

    @mock.patch('wd.Wikidata.call', return_value={'entities': {}})
    def test_no_entities(self, _):
        self.assertEqual({}, Wikidata.load({'Q1'}))


class TestSearch(TestCase):
    @mock.patch('wd.Wikidata.call', return_value={'query': {'search': [{'title': 'Q1091618'}]}})
    def test_search(self, api_call):
        self.assertEqual('Q1091618', Wikidata.search('haswbstatement:"P3083=HD 1"'))
        api_call.assert_called_with('query', {'list': 'search', 'srsearch': 'haswbstatement:"P3083=HD 1"'})


class TestStatic(TestCase):
    def test_format_float(self):
        self.assertEqual('0.12345679', Wikidata.format_float('0.123456789', 8))
        self.assertEqual(0, Decimal(Wikidata.format_float('+0E-7', 8)))

    def test_date_parser(self):
        self.assertIsNone(Wikidata.parse_date(''))
        self.assertEqual('+1987-00-00T00:00:00Z', Wikidata.parse_date('1987')['time'])
        self.assertEqual(9, Wikidata.parse_date('1987')['precision'])
        self.assertEqual(0, Wikidata.parse_date('1987')['timezone'])
        self.assertEqual(0, Wikidata.parse_date('1987')['before'])
        self.assertEqual(0, Wikidata.parse_date('1987')['after'])
        self.assertEqual('http://www.wikidata.org/entity/Q1985727', Wikidata.parse_date('1987')['calendarmodel'])
        self.assertEqual('+2009-04-00T00:00:00Z', Wikidata.parse_date('2009-04')['time'])
        self.assertEqual(10, Wikidata.parse_date('2009-04')['precision'])
        self.assertEqual('+2009-04-12T00:00:00Z', Wikidata.parse_date('2009-04-12')['time'])
        self.assertEqual(11, Wikidata.parse_date('2009-4-12')['precision'])
        self.assertEqual('+2009-04-02T00:00:00Z', Wikidata.parse_date('2009-04-2')['time'])
        self.assertEqual('+3456-02-01T00:00:00Z', Wikidata.parse_date('1/2/3456')['time'])
        self.assertEqual('+1903-01-00T00:00:00Z', Wikidata.parse_date('01/1903')['time'])
        self.assertIsNone(Wikidata.parse_date('29/16/1924'))

    @mock.patch('wd.Wikidata.type_of', return_value='wikibase-item')
    def test_qualifier_filter(self, _):
        self.assertTrue(Wikidata.qualifier_filter({'qualifiers': []}, {}))
        self.assertFalse(Wikidata.qualifier_filter({'qualifiers': [('P972', 'Q1')]}, {}))
        q2 = {'qualifiers': {'P972': [Wikidata.create_snak('P972', 'Q2')]}}
        self.assertFalse(Wikidata.qualifier_filter({'qualifiers': [('P1227', 'Q2')]}, q2))
        self.assertFalse(Wikidata.qualifier_filter({'qualifiers': [('P972', 'Q1')]}, q2))
        self.assertTrue(Wikidata.qualifier_filter({'qualifiers': [('P972', 'Q2')]}, q2))
