from unittest import TestCase, mock

from adql import Model


class TestSimbad(TestCase):
    def setUp(self):
        Model._parents = {}

    def test_get_by_any_id_hit(self):
        Model._parents = {'hd 1': 'Q1'}
        self.assertEqual('Q1', Model.get_parent_snak('HD 1')['datavalue']['value']['id'])
        self.assertDictEqual({'hd 1': 'Q1'}, Model._parents)

    @mock.patch('adql.Model.tap_query', return_value={'HD 1': 0})
    def test_get_by_any_id_miss_and_hit(self, _):
        Model._parents = {'hd 1': 'Q1'}
        self.assertEqual('Q1', Model.get_parent_snak('HIP 1')['datavalue']['value']['id'])
        self.assertDictEqual({'hd 1': 'Q1', 'hip 1': 'Q1'}, Model._parents)

    @mock.patch('adql.Model.tap_query', return_value={'HD 2': 0})
    @mock.patch('adql.Element.get_by_id', return_value='Q2')
    def test_get_by_any_id_miss_and_miss(self, _, __):
        self.assertEqual('Q2', Model.get_parent_snak('HIP 2')['datavalue']['value']['id'])
        self.assertDictEqual({'hd 2': 'Q2', 'hip 2': 'Q2'}, Model._parents)

    @mock.patch('adql.Model.tap_query', return_value=None)
    def test_get_by_incorrect_id(self, _):
        self.assertIsNone(Model.get_parent_snak('QQQ'))

    @mock.patch('wd.Wikidata.request', return_value=None)
    def test_tap_query_exception(self, mock_post):
        self.assertIsNone(Model.tap_query('https://simbad.u-strasbg.fr/simbad/sim-tap', 'select * from basic'))
        mock_post.assert_called_with('https://simbad.u-strasbg.fr/simbad/sim-tap/sync',
                                     data={'request': 'doQuery', 'lang': 'adql', 'format': 'csv', 'maxrec': -1,
                                           'query': 'select * from basic'}, stream=True)
