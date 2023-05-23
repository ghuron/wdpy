from unittest import TestCase, mock

from exoplanet_eu import ExoplanetEu


class TestExoplanetEu(TestCase):
    @classmethod
    def setUp(cls):
        cls.exo = ExoplanetEu('55 Cnc e')

    def test_obtain_existing_v_mag(self):
        claim = self.exo.obtain_claim(ExoplanetEu.create_snak('P1215', '8.88'))
        claim['qualifiers'] = {'P1227': [ExoplanetEu.create_snak('P1227', 'Q4892529')]}  # V diapason
        claim = self.exo.obtain_claim(ExoplanetEu.create_snak('P1215', '9.99'))
        self.assertIsNone(claim)

    def test_ignore_existing_g_mag(self):
        claim = self.exo.obtain_claim(ExoplanetEu.create_snak('P1215', '8.88'))
        claim['qualifiers'] = {'P1227': [ExoplanetEu.create_snak('P1227', 'Q66659648')]}  # G diapason
        claim = self.exo.obtain_claim(ExoplanetEu.create_snak('P1215', '9.99'))
        self.assertEqual('9.99', claim['mainsnak']['datavalue']['value']['amount'])

    @mock.patch('wikidata.WikiData.api_search', return_value='Q50668')
    def test_get_by_id(self, api_search):
        value = ExoplanetEu.get_by_id('55 Cnc e')
        self.assertEqual('Q50668', value)
        api_search.assert_called_with('haswbstatement:"P5653=55 Cnc e"')


class TestParseValue(TestCase):
    def test_parse_simple_float(self):
        value = ExoplanetEu.parse_value('P1096', '0.98')['datavalue']['value']
        self.assertEqual('0.98', value['amount'])
        self.assertEqual('1', value['unit'])

    def test_parse_exp_float_unit(self):
        value = ExoplanetEu.parse_value('P2120', '2e-06 RJ')['datavalue']['value']
        self.assertEqual('0.000002', value['amount'])
        self.assertEqual('http://www.wikidata.org/entity/Q3421309', value['unit'])

    def test_parse_error_unit(self):
        value = ExoplanetEu.parse_value('P2051', '0.082 (± 0.0041)  MJ')['datavalue']['value']
        self.assertEqual('0.082', value['amount'])
        self.assertEqual('0.0779', value['lowerBound'])
        self.assertEqual('0.0861', value['upperBound'])
        self.assertEqual('http://www.wikidata.org/entity/Q651336', value['unit'])

    def test_parse_exp_error(self):
        value = ExoplanetEu.parse_value('P2146', '1.30618608 (± 3.8e-07) day')['datavalue']['value']
        self.assertEqual('1.30618608', value['amount'])
        self.assertEqual('1.3061857', value['lowerBound'])
        self.assertEqual('1.30618646', value['upperBound'])

    def test_parse_diff_min_max(self):
        value = ExoplanetEu.parse_value('P2216', '353.0(-88.0 +73.0) m/s')['datavalue']['value']
        self.assertEqual('353', value['amount'])
        self.assertEqual('265', value['lowerBound'])
        self.assertEqual('426', value['upperBound'])
        self.assertEqual('http://www.wikidata.org/entity/Q182429', value['unit'])

    def test_parse_one_letter_unit(self):
        value = ExoplanetEu.parse_value('P2216', '5991.0 (± 50.0) K')['datavalue']['value']
        self.assertEqual('http://www.wikidata.org/entity/Q11579', value['unit'])

    def test_ra_fraction_seconds(self):
        value = ExoplanetEu.parse_value('P6257', '18:57:39.0')['datavalue']['value']
        self.assertEqual('284.4125', value['amount'])
        self.assertEqual('http://www.wikidata.org/entity/Q28390', value['unit'])

    def test_dec_negative_seconds(self):
        value = ExoplanetEu.parse_value('P6258', '-41:32:09')['datavalue']['value']
        self.assertEqual('-41.536', value['amount'])

    def test_parse_junk(self):
        self.assertIsNone(ExoplanetEu.parse_value('P6257', 'aa:bb:cc'))
        self.assertEqual('aa:bb:cc', ExoplanetEu.create_snak('P213', 'aa:bb:cc')['datavalue']['value'])
