#!/usr/bin/python3
import json
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
