from unittest import TestCase, mock

from adql import ADQL


class TestSimbad(TestCase):
    def setUp(self):
        ADQL._parents = {}

    def test_get_by_any_id_hit(self):
        ADQL._parents = {'hd 1': 'Q1'}
        self.assertEqual('Q1', ADQL.get_parent_object('HD 1'))
        self.assertDictEqual({'hd 1': 'Q1'}, ADQL._parents)

    @mock.patch('adql.ADQL.tap_query', return_value={'HD 1': 0})
    def test_get_by_any_id_miss_and_hit(self, _):
        ADQL._parents = {'hd 1': 'Q1'}
        self.assertEqual('Q1', ADQL.get_parent_object('HIP 1'))
        self.assertDictEqual({'hd 1': 'Q1', 'hip 1': 'Q1'}, ADQL._parents)

    @mock.patch('adql.ADQL.tap_query', return_value={'HD 2': 0})
    @mock.patch('adql.ADQL.get_by_id', return_value='Q2')
    def test_get_by_any_id_miss_and_miss(self, _, __):
        self.assertEqual('Q2', ADQL.get_parent_object('HIP 2'))
        self.assertDictEqual({'hd 2': 'Q2', 'hip 2': 'Q2'}, ADQL._parents)

    @mock.patch('adql.ADQL.tap_query', return_value=None)
    def test_get_by_incorrect_id(self, _):
        self.assertIsNone(ADQL.get_parent_object('QQQ'))

    @mock.patch('wd.Wikidata.request', return_value=None)
    def test_tap_query_exception(self, mock_post):
        self.assertIsNone(ADQL.tap_query('https://simbad.u-strasbg.fr/simbad/sim-tap', 'select * from basic'))
        mock_post.assert_called_with('https://simbad.u-strasbg.fr/simbad/sim-tap/sync',
                                     data={'request': 'doQuery', 'lang': 'adql', 'format': 'csv', 'maxrec': -1,
                                           'query': 'select * from basic'}, stream=True)
