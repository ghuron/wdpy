import re
import urllib.error
from typing import Optional
from urllib.request import Request
from unittest import TestCase, mock

import wdpy

class TestEncodeMultipart(TestCase):
    def test_plain_fields(self):
        self.assertEqual(re.sub(r'\n *', '\r\n', '''--bnd
                    Content-Disposition: form-data; name="foo"
                    
                    bar
                    --bnd
                    Content-Disposition: form-data; name="baz"
                    
                    1
                    --bnd--
                    ''').encode('utf-8'), 
                    wdpy.core._encode_multipart('bnd', {'foo': 'bar', 'baz': 1}))

    def test_list_as_csv(self):
        self.assertEqual(re.sub(r'\n *', '\r\n', '''--XYZ
                    Content-Disposition: form-data; name="csv"; filename="csv.csv"
                    Content-Type: text/csv
                    
                    a,b
                    c,d

                    --XYZ--
                    ''').encode('utf-8'), 
                    wdpy.core._encode_multipart('XYZ', {'csv': ['a,b', 'c,d']}))


class TestBuildRequest(TestCase):
    def test_returns_request(self):
        self.assertIsInstance(wdpy.core.build_request('http://ex.com'), Request)

    def test_user_agent(self):
        self.assertEqual(wdpy.core.build_request('http://ex.com').get_header('User-agent'),
                         'github.com/ghuron/wdpy')

    def test_custom_header_merged(self):
        self.assertEqual(wdpy.core.build_request('http://ex.com', headers={'Accept': 'text/csv'})
                         .get_header('Accept'), 'text/csv')

    def test_no_data_without_params(self):
        self.assertIsNone(wdpy.core.build_request('http://ex.com').data)

    def test_urlencoded_params(self):
        self.assertEqual(wdpy.core.build_request('http://ex.com', params={'a': '1', 'b': '2'}).data,
                         b'a=1&b=2')

    def test_multipart_params(self):
        req = wdpy.core.build_request('http://ex.com', params={'f': 'v'},
                                      headers={'Content-Type': 'multipart/form-data'})
        self.assertIn(b'Content-Disposition: form-data', req.data)


class TestExec(TestCase):
    @mock.patch('wdpy.core.build_opener')
    def test_csv(self, build_mock: Optional[mock.MagicMock] = None):
        if build_mock:
            r = mock.MagicMock()
            r.__enter__.return_value.read.return_value = b'id,e\nQ353673,Q49836\nQ854830,Q49836'
            build_mock.return_value.open.return_value = r
        self.assertEqual(wdpy.core.exec('SELECT...'), {'Q353673': {'e': 'Q49836'}, 'Q854830': {'e': 'Q49836'}})

    @mock.patch('wdpy.core.build_opener')
    def test_fail(self, build_mock: Optional[mock.MagicMock] = None):
        if build_mock:
            build_mock.return_value.open.side_effect = Exception('boom')
        self.assertIsNone(wdpy.core.exec('SELECT...'))

class TestParseCSV(TestCase):
    @mock.patch('wdpy.core.build_opener')
    def test_accept_header(self, build_mock: Optional[mock.MagicMock] = None):
        if build_mock:
            r = mock.MagicMock()
            r.__enter__.return_value.read.return_value = b''
            build_mock.return_value.open.return_value = r
        req = wdpy.core.build_request('http://example.com')
        wdpy.core.parse_csv(req)
        self.assertEqual(req.get_header('Accept'), 'text/csv')

    @mock.patch('wdpy.core.build_opener')
    def test_uid(self, build_mock: Optional[mock.MagicMock] = None):
        if build_mock:
            r = mock.MagicMock()
            r.__enter__.return_value.read.return_value = b'id,val\n1,a\n2,b'
            build_mock.return_value.open.return_value = r
        req = wdpy.core.build_request('http://example.com')
        self.assertEqual(wdpy.core.parse_csv(req), {'1': {'val': 'a'}, '2': {'val': 'b'}})

    @mock.patch('wdpy.core.build_opener')
    def test_gid(self, build_mock: Optional[mock.MagicMock] = None):
        if build_mock:
            r = mock.MagicMock()
            r.__enter__.return_value.read.return_value = b'gid,val\nA,1\nA,2\nB,3'
            build_mock.return_value.open.return_value = r
        req = wdpy.core.build_request('http://example.com')
        self.assertEqual(wdpy.core.parse_csv(req), {'A': [{'val': '1'}, {'val': '2'}], 'B': [{'val': '3'}]})

    @mock.patch('wdpy.core.build_opener')
    def test_url_stripping(self, build_mock: Optional[mock.MagicMock] = None):
        if build_mock:
            r = mock.MagicMock()
            r.__enter__.return_value.read.return_value = b'item,prop\nhttp://www.wikidata.org/entity/Q1,http://www.wikidata.org/entity/P1'
            build_mock.return_value.open.return_value = r
        req = wdpy.core.build_request('http://example.com')
        self.assertEqual(wdpy.core.parse_csv(req), {'Q1': {'prop': 'P1'}})

    @mock.patch('wdpy.core.build_opener')
    def test_quoted(self, build_mock: Optional[mock.MagicMock] = None):
        if build_mock:
            r = mock.MagicMock()
            r.__enter__.return_value.read.return_value = b'id,val\n"1","a"'
            build_mock.return_value.open.return_value = r
        req = wdpy.core.build_request('http://example.com')
        self.assertEqual(wdpy.core.parse_csv(req), {'1': {'val': 'a'}})

    @mock.patch('wdpy.core.build_opener')
    def test_fail(self, build_mock: Optional[mock.MagicMock] = None):
        if build_mock:
            build_mock.return_value.open.side_effect = Exception('boom')
        req = wdpy.core.build_request('http://example.com')
        self.assertIsNone(wdpy.core.parse_csv(req))

    @mock.patch('wdpy.core.build_opener')
    def test_empty_string(self, build_mock: Optional[mock.MagicMock] = None):
        if build_mock:
            r = mock.MagicMock()
            r.__enter__.return_value.read.return_value = b''
            build_mock.return_value.open.return_value = r
        req = wdpy.core.build_request('http://example.com')
        self.assertEqual(wdpy.core.parse_csv(req), {})

    @mock.patch('wdpy.core.build_opener')
    def test_headers_only(self, build_mock: Optional[mock.MagicMock] = None):
        if build_mock:
            r = mock.MagicMock()
            r.__enter__.return_value.read.return_value = b'id,val\n'
            build_mock.return_value.open.return_value = r
        req = wdpy.core.build_request('http://example.com')
        self.assertEqual(wdpy.core.parse_csv(req), {})


class TestFetchJson(TestCase):


    @mock.patch('wdpy.core._action_api_opener.open')


    def test_success(self, open_mock=None):


        r = mock.MagicMock(read=lambda: b'{"result": "ok"}')


        r.__enter__.return_value = r


        open_mock.return_value = r


        self.assertEqual(wdpy.core.fetch_json('action', param='val'), {'result': 'ok'})





    @mock.patch('wdpy.core.logging.error')


    @mock.patch('wdpy.core._action_api_opener.open')


    def test_fail(self, open_mock=None, _=None):


        open_mock.side_effect = Exception('fail')


        self.assertIsNone(wdpy.core.fetch_json('action'))





    @mock.patch('wdpy.core._action_api_opener.open')


    @mock.patch('wdpy.core.logging.error')


    def test_error(self, log_mock=None, open_mock=None):


        r = mock.MagicMock(read=lambda: b'bad')


        r.__enter__.return_value = r


        open_mock.return_value = r


        self.assertIsNone(wdpy.core.fetch_json('action'))

    @mock.patch('wdpy.core._action_api_opener.open')
    def test_uses_cookie_opener(self, open_mock):
        r = mock.MagicMock(read=lambda: b'{}')
        r.__enter__.return_value = r
        open_mock.return_value = r
        wdpy.core.fetch_json('action')
        open_mock.assert_called_once()





class TestLogon(TestCase):



    @mock.patch('wdpy.core.fetch_json', side_effect=[{'query': {'tokens': {'logintoken': 'lt'}}}, 
                                                     {'login': {'result': 'Success'}}, 
                                                     {'query': {'tokens': {'csrftoken': 'ct'}}}])
    def test_success(self, _=None):
        self.assertTrue(wdpy.core.logon('user', 'pass'))

    @mock.patch('wdpy.core._login', None)
    @mock.patch('wdpy.core._psw', None)
    @mock.patch('wdpy.core.logging.error')
    def test_fail_creds(self, *_):
        self.assertFalse(wdpy.core.logon())

    @mock.patch('wdpy.core.fetch_json', side_effect=[{'query': {'tokens': {'logintoken': 'lt'}}}, 
                                                     {'login': {'result': 'Failed'}}])
    @mock.patch('wdpy.core.logging.error')
    def test_fail_login(self, log_mock=None, fetch_mock=None):
        self.assertFalse(wdpy.core.logon('user', 'pass'))


class TestApiWrite(TestCase):
    @mock.patch('wdpy.core.fetch_json', return_value={'success': 1})
    @mock.patch('wdpy.core._csrf_token', 'token')
    def test_success(self, _=None):
        self.assertEqual(wdpy.core.api_write('action'), {'success': 1})

    @mock.patch('wdpy.core.logon', return_value=False)
    @mock.patch('wdpy.core.fetch_json', side_effect=[{'error': {'code': 'badtoken'}}])
    @mock.patch('wdpy.core.logging.error')
    @mock.patch('wdpy.core._csrf_token', 'expired')
    def test_expired_token(self, token_mock=None, log_mock=None, fetch_mock=None, logon_mock=None):
        self.assertIsNone(wdpy.core.api_write('action'))

    @mock.patch('wdpy.core.logon', return_value=False)
    @mock.patch('wdpy.core.logging.error')
    @mock.patch('wdpy.core._csrf_token', '')
    def test_no_token(self, token_mock=None, log_mock=None, logon_mock=None):
        self.assertIsNone(wdpy.core.api_write('action'))

    @mock.patch('wdpy.core.fetch_json', return_value={'error': {'code': 'err'}})
    @mock.patch('wdpy.core.logging.error')
    @mock.patch('wdpy.core._csrf_token', 'token')
    def test_error(self, token_mock=None, log_mock=None, fetch_mock=None):
        self.assertIsNone(wdpy.core.api_write('action'))


class TestSearch(TestCase):
    @mock.patch('wdpy.core.fetch_json', return_value={'query': {'search': [{'title': 'Q28518421'}]}})
    def test_success(self, *_):
        self.assertEqual(wdpy.core.search('"2007 PD2"'), ['Q28518421'])

    @mock.patch('wdpy.core.fetch_json', return_value={'searchinfo': {'totalhits': 0}, 'search': []})
    def test_empty(self, *_):
        self.assertEqual(wdpy.core.search('PD2S'), [])

    @mock.patch('wdpy.core.fetch_json', return_value=None)
    def test_fail(self, _=None):
        self.assertIsNone(wdpy.core.search('query'))


class TestHasWbStatement(TestCase):
    @mock.patch('wdpy.core.search', return_value=['Q1400128'])
    def test_p1979(self, *_):
        self.assertEqual(wdpy.core.haswbstatement('P1979', ('4044394', 'Urr Ida')), ['Q1400128'])

    @mock.patch('wdpy.core.search', return_value=['Q3427'])
    def test_general(self, *_):
        self.assertEqual(wdpy.core.haswbstatement('P3083', '* alf Lyr'), ['Q3427'])

