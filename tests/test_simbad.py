from typing import Optional
from unittest import mock, TestCase

from wdpy.connectors.simbad import SimbadItem


class TestExec(TestCase):


    @mock.patch('wdpy.core.build_opener')
    def test_exec_with_table1(self, build_mock: Optional[mock.MagicMock] = None):
        if build_mock:
            r = mock.MagicMock()
            r.__enter__.return_value.read.return_value = b'col1,id\n"HD 0",\n"HD 1","HD      1"\n'
            build_mock.return_value.open.return_value = r


        query = 'SELECT col1, id FROM TAP_UPLOAD.table1 LEFT JOIN ident ON col1=id'


        self.assertEqual(SimbadItem.exec(query, ['HD 0', 'HD 1']), 


                         {'HD 0': {'id': ''}, 'HD 1': {'id': 'HD 1'}})




