from unittest import TestCase, mock

from simbad_dap import SimbadDAP


class TestSimbad(TestCase):
    def setUp(cls):
        SimbadDAP.cache = {}

    def test_get_by_any_id_hit(self):
        SimbadDAP.cache = {'hd 1': 'Q1'}
        self.assertEqual('Q1', SimbadDAP.get_by_any_id('HD 1'))
        self.assertDictEqual({'hd 1': 'Q1'}, SimbadDAP.cache)

    @mock.patch('adql.ADQL.tap_query', return_value={'HD 1': 0})
    def test_get_by_any_id_miss_and_hit(self, _):
        SimbadDAP.cache = {'hd 1': 'Q1'}
        self.assertEqual('Q1', SimbadDAP.get_by_any_id('HIP 1'))
        self.assertDictEqual({'hd 1': 'Q1', 'hip 1': 'Q1'}, SimbadDAP.cache)

    @mock.patch('adql.ADQL.tap_query', return_value={'HD 2': 0})
    @mock.patch('adql.ADQL.get_by_id', return_value='Q2')
    def test_get_by_any_id_miss_and_miss(self, _, __):
        self.assertEqual('Q2', SimbadDAP.get_by_any_id('HIP 2'))
        self.assertDictEqual({'hd 2': 'Q2', 'hip 2': 'Q2'}, SimbadDAP.cache)

    @mock.patch('adql.ADQL.tap_query', return_value=None)
    def test_get_by_incorrect_id(self, _):
        self.assertIsNone(SimbadDAP.get_by_any_id('QQQ'))

    @mock.patch('adql.ADQL.request', return_value=None)
    def test_tap_query_exception(self, mock_post):
        self.assertIsNone(SimbadDAP.tap_query('https://simbad.u-strasbg.fr/simbad/sim-tap', 'select * from basic'))
        mock_post.assert_called_with('https://simbad.u-strasbg.fr/simbad/sim-tap/sync',
                                     data={'request': 'doQuery', 'lang': 'adql', 'format': 'csv', 'maxrec': -1,
                                           'query': 'select * from basic'}, stream=True)

    def test_parse_input_use_parent(self):
        simbad = SimbadDAP('HD 1')
        simbad.dataset = {simbad.external_id: []}
        simbad.prepare_data()
        self.assertEqual(simbad.db_property, simbad.input_snaks[0]['property'])
        self.assertEqual(simbad.external_id, simbad.input_snaks[0]['datavalue']['value'])
