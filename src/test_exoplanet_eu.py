from unittest import TestCase, mock
from unittest.mock import MagicMock

from exoplanet_eu import Element, Model
from wd import Wikidata


def mock_type_off(p):
    return {'P248': 'wikibase-item', 'P528': 'string', 'P575': 'time', 'P1013': 'wikibase-item', 'P1215': 'quantity',
            'P1227': 'wikibase-item', 'P4501': 'quantity', 'P5653': 'external-id'}[p]


class TestExoplanetEu(TestCase):
    @mock.patch('wd.Wikidata.search', return_value=None)
    @mock.patch('wd.Wikidata.type_of', side_effect=mock_type_off)
    def test_obtain_claim(self, _, __):
        exo = Element('55 Cnc e')

        (snak := Wikidata.create_snak('P1215', '8.88'))['qualifiers'] = [('P1227', 'Q4892529')]  # V diapason
        claim = exo.obtain_claim(snak)

        (snak := Wikidata.create_snak('P1215', '9.99'))['qualifiers'] = [('P1227', 'Q4892529')]  # V diapason
        self.assertIsNone(exo.obtain_claim(snak))  # Do not mess with existing V mag

        claim['qualifiers'] = {'P1227': [Wikidata.create_snak('P1227', 'Q66659648')]}  # G diapason
        self.assertEqual('9.99', exo.obtain_claim(snak)['mainsnak']['datavalue']['value']['amount'])

        claim = exo.obtain_claim(Wikidata.create_snak('P4501', 0.5))
        self.assertEqual('Q2832068', claim['qualifiers']['P1013'][0]['datavalue']['value']['id'])  # Always geometric


class TestModel(TestCase):
    @mock.patch('wd.Wikidata.type_of', return_value='quantity')
    def test_parse_value(self, _):
        value = Model.transform('P1096', '0.98')['datavalue']['value']
        self.assertDictEqual({'unit': '1', 'amount': '0.98'}, value)

        value = Model.transform('P2120', '2e-06 RJ')['datavalue']['value']
        self.assertDictEqual({'unit': 'http://www.wikidata.org/entity/Q3421309', 'amount': '0.000002'}, value)

        value = Model.transform('P2051', '0.082 (± 0.0041)  MJ')['datavalue']['value']
        self.assertDictEqual({'unit': 'http://www.wikidata.org/entity/Q651336', 'amount': '0.082',
                              'lowerBound': '0.0779', 'upperBound': '0.0861'}, value)

        value = Model.transform('P1096', '1.30618608 (± 3.8e-07)')['datavalue']['value']
        self.assertDictEqual({'unit': '1', 'amount': '1.30618608',
                              'lowerBound': '1.3061857', 'upperBound': '1.30618646'}, value)

        value = Model.transform('P2216', '353.0(+73.0 -88.0 ) m/s')['datavalue']['value']
        self.assertDictEqual({'unit': 'http://www.wikidata.org/entity/Q182429', 'amount': '353',
                              'lowerBound': '265', 'upperBound': '426'}, value)

        value = Model.transform('P2216', '5991.0 (± 50.0) K')['datavalue']['value']
        self.assertDictEqual({'unit': 'http://www.wikidata.org/entity/Q11579', 'amount': '5991',
                              'lowerBound': '5941', 'upperBound': '6041'}, value)

        value = Model.transform('P6257', '18:57:39.0')['datavalue']['value']
        self.assertDictEqual({'unit': 'http://www.wikidata.org/entity/Q28390', 'amount': '284.4125'}, value)

        value = Model.transform('P6258', '-41:32:09')['datavalue']['value']
        self.assertDictEqual({'unit': 'http://www.wikidata.org/entity/Q28390', 'amount': '-41.536'}, value)

        self.assertIsNone(Model.transform('P6257', 'aa:bb:cc'))

    @mock.patch('wd.Wikidata.type_of', return_value='external-id')
    def test_create_snak(self, _):
        self.assertEqual('aa:bb:cc', Model.transform('P213', 'aa:bb:cc')['datavalue']['value'])

    @mock.patch('wd.Wikidata.type_of', side_effect=mock_type_off)
    @mock.patch('wd.Wikidata.request', return_value=MagicMock(content='''<div id="planet-detail-basic-info">
            <dd class="col-sm-8"> Kepler-338 d </dd>
            <div class="row d-flex justify-content-between "><span>2022</span></div>
            <div class="row collapse" id="planet_field_publications_discovered"><ul class="list-group">
            <li class="list-group-item">
                <a href="#publication_2540">
                    <i class="bi-file-earmark-text" aria-hidden="true" aria-label="document icon"></i>
                    TOI-969: a late-K dwarf with a hot mini-Neptune in the desert and an eccentric cold Jupiter
            </a></li></ul></div></div'''))
    def test_prepare_data(self, _, __):
        input_snaks = Model.prepare_data('kepler_338_d--1930').input_snaks
        self.assertEqual('+2022-00-00T00:00:00Z', input_snaks[2]['datavalue']['value']['time'])
        self.assertEqual(['Q54012702'], input_snaks[2]['source'])

    @mock.patch('wd.Wikidata.request', return_value=None)
    def test_failed_request(self, _):
        self.assertIsNone(Model.prepare_data('test'))
