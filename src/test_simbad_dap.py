from unittest import TestCase, mock

from requests import exceptions

from simbad_dap import SimbadDAP


class TestSimbad(TestCase):
    @mock.patch('requests.post', side_effect=exceptions.ConnectionError)
    @mock.patch('logging.error')
    def test_tap_query_exception(self, mock_error, mock_post):
        self.assertEqual({}, SimbadDAP.tap_query('https://simbad.u-strasbg.fr/simbad/sim-tap', 'select * from basic'))
        mock_post.assert_called_with('https://simbad.u-strasbg.fr/simbad/sim-tap/sync',
                                     data={'request': 'doQuery', 'lang': 'adql', 'format': 'csv', 'maxrec': -1,
                                           'query': 'select * from basic'}, stream=True)
        mock_error.assert_called_with('Error while retrieving results of the query: select * from basic')

    def test_parse_input_use_parent(self):
        simbad = SimbadDAP('HD 1')
        simbad.cache = {simbad.external_id: []}
        simbad.prepare_data()
        self.assertEqual(simbad.db_property, simbad.input_snaks[0]['property'])
        self.assertEqual(simbad.external_id, simbad.input_snaks[0]['datavalue']['value'])
