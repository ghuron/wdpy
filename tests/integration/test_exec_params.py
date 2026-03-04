from unittest import TestCase, mock
from typing import Optional
import wdpy.core
from wdpy.connectors.astro import AstroItem

class TestExecParams(TestCase):
    @mock.patch('wdpy.core.build_opener')
    def test_core_exec_uid_column(self, build_mock: Optional[mock.MagicMock] = None):
        if build_mock:
            r = mock.MagicMock()
            r.__enter__.return_value.read.return_value = b'uid,val\n1,a\n2,b'
            build_mock.return_value.open.return_value = r
        self.assertEqual(wdpy.core.exec('SELECT ?uid ...'), {'1': {'val': 'a'}, '2': {'val': 'b'}})

    @mock.patch('wdpy.core.build_opener')
    def test_core_exec_gid_column(self, build_mock: Optional[mock.MagicMock] = None):
        if build_mock:
            r = mock.MagicMock()
            r.__enter__.return_value.read.return_value = b'gid,val\nA,1\nA,2\nB,3'
            build_mock.return_value.open.return_value = r
        self.assertEqual(wdpy.core.exec('SELECT ?gid ...'),
                         {'A': [{'val': '1'}, {'val': '2'}], 'B': [{'val': '3'}]})

    @mock.patch('wdpy.core.build_opener')
    def test_astro_exec_uid_column(self, build_mock: Optional[mock.MagicMock] = None):
        if build_mock:
            r = mock.MagicMock()
            r.__enter__.return_value.read.return_value = b'uid,val\n1,a\n2,b'
            build_mock.return_value.open.return_value = r
        with mock.patch.object(AstroItem, '_config', {'tap_endpoint': 'http://example.com/tap'}):
            self.assertEqual(AstroItem.exec('SELECT...'), {'1': {'val': 'a'}, '2': {'val': 'b'}})

    @mock.patch('wdpy.core.build_opener')
    def test_astro_exec_gid_column(self, build_mock: Optional[mock.MagicMock] = None):
        if build_mock:
            r = mock.MagicMock()
            r.__enter__.return_value.read.return_value = b'gid,val\nA,1\nA,2\nB,3'
            build_mock.return_value.open.return_value = r
        with mock.patch.object(AstroItem, '_config', {'tap_endpoint': 'http://example.com/tap'}):
            self.assertEqual(AstroItem.exec('SELECT...'),
                             {'A': [{'val': '1'}, {'val': '2'}], 'B': [{'val': '3'}]})

    @mock.patch('wdpy.core.build_opener')
    def test_core_exec_plain(self, build_mock: Optional[mock.MagicMock] = None):
        if build_mock:
            r = mock.MagicMock()
            r.__enter__.return_value.read.return_value = b'col1,col2\nval1,val2'
            build_mock.return_value.open.return_value = r
        self.assertEqual(wdpy.core.exec('SELECT...'), {'val1': {'col2': 'val2'}})

    @mock.patch('wdpy.core.build_opener')
    def test_core_exec_uid_empty_result(self, build_mock: Optional[mock.MagicMock] = None):
        if build_mock:
            r = mock.MagicMock()
            r.__enter__.return_value.read.return_value = b'uid,val\n'
            build_mock.return_value.open.return_value = r
        self.assertEqual(wdpy.core.exec('SELECT...'), {})

    @mock.patch('wdpy.core.build_opener')
    def test_core_exec_gid_empty_result(self, build_mock: Optional[mock.MagicMock] = None):
        if build_mock:
            r = mock.MagicMock()
            r.__enter__.return_value.read.return_value = b'gid,val\n'
            build_mock.return_value.open.return_value = r
        self.assertEqual(wdpy.core.exec('SELECT...'), {})

    @mock.patch('wdpy.core.build_opener')
    def test_url_stripping(self, build_mock: Optional[mock.MagicMock] = None):
        if build_mock:
            r = mock.MagicMock()
            r.__enter__.return_value.read.return_value = b'item,label\nhttp://www.wikidata.org/entity/Q123,Label'
            build_mock.return_value.open.return_value = r
        self.assertEqual(wdpy.core.exec('SELECT...'), {'Q123': {'label': 'Label'}})
