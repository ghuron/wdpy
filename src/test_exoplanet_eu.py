from unittest import TestCase, mock

from exoplanet_eu import ExoplanetEu


class TestExoplanetEu(TestCase):
    @classmethod
    def setUp(cls):
        cls.exo = ExoplanetEu('55 Cnc e')

    def test_obtain_claim(self):
        (snak := ExoplanetEu.create_snak('P1215', '8.88'))['qualifiers'] = {'P1227': 'Q4892529'}  # V diapason
        self.assertEqual(0, (claim := self.exo.obtain_claim(snak))['mespos'])  # mespos present and always 0

        (snak := ExoplanetEu.create_snak('P1215', '9.99'))['qualifiers'] = {'P1227': 'Q4892529'}  # V diapason
        self.assertIsNone(self.exo.obtain_claim(snak))  # Do not mess with existing V mag

        claim['qualifiers'] = {'P1227': [ExoplanetEu.create_snak('P1227', 'Q66659648')]}  # G diapason
        self.assertEqual('9.99', self.exo.obtain_claim(snak)['mainsnak']['datavalue']['value']['amount'])

        claim = self.exo.obtain_claim(ExoplanetEu.create_snak('P4501', 0.5))
        self.assertEqual('Q2832068', claim['qualifiers']['P1013'][0]['datavalue']['value']['id'])  # Always geometric

    @mock.patch('wikidata.WikiData.api_search', return_value='Q50668')
    def test_get_by_id(self, api_search):
        value = ExoplanetEu.get_by_id('55 Cnc e')
        self.assertEqual('Q50668', value)
        api_search.assert_called_with('haswbstatement:"P5653=55 Cnc e"')

    def test_parse_value(self):
        value = ExoplanetEu.parse_value('P1096', '0.98')['datavalue']['value']
        self.assertDictEqual({'unit': '1', 'amount': '0.98'}, value)

        value = ExoplanetEu.parse_value('P2120', '2e-06 RJ')['datavalue']['value']
        self.assertDictEqual({'unit': 'http://www.wikidata.org/entity/Q3421309', 'amount': '0.000002'}, value)

        value = ExoplanetEu.parse_value('P2051', '0.082 (± 0.0041)  MJ')['datavalue']['value']
        self.assertDictEqual({'unit': 'http://www.wikidata.org/entity/Q651336', 'amount': '0.082',
                              'lowerBound': '0.0779', 'upperBound': '0.0861'}, value)

        value = ExoplanetEu.parse_value('P1096', '1.30618608 (± 3.8e-07)')['datavalue']['value']
        self.assertDictEqual({'unit': '1', 'amount': '1.30618608',
                              'lowerBound': '1.3061857', 'upperBound': '1.30618646'}, value)

        value = ExoplanetEu.parse_value('P2216', '353.0(-88.0 +73.0) m/s')['datavalue']['value']
        self.assertDictEqual({'unit': 'http://www.wikidata.org/entity/Q182429', 'amount': '353',
                              'lowerBound': '265', 'upperBound': '426'}, value)

        value = ExoplanetEu.parse_value('P2216', '5991.0 (± 50.0) K')['datavalue']['value']
        self.assertDictEqual({'unit': 'http://www.wikidata.org/entity/Q11579', 'amount': '5991',
                              'lowerBound': '5941', 'upperBound': '6041'}, value)

        value = ExoplanetEu.parse_value('P6257', '18:57:39.0')['datavalue']['value']
        self.assertDictEqual({'unit': 'http://www.wikidata.org/entity/Q28390', 'amount': '284.4125'}, value)

        value = ExoplanetEu.parse_value('P6258', '-41:32:09')['datavalue']['value']
        self.assertDictEqual({'unit': 'http://www.wikidata.org/entity/Q28390', 'amount': '-41.536'}, value)

        self.assertIsNone(ExoplanetEu.parse_value('P6257', 'aa:bb:cc'))

    def test_create_snak(self):
        self.assertEqual('aa:bb:cc', ExoplanetEu.create_snak('P213', 'aa:bb:cc')['datavalue']['value'])
