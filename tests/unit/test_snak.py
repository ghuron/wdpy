import json
from datetime import date, datetime
from typing import Optional
from unittest import TestCase, mock
from wdpy import Snak

ETHALONS = {
    'item': (
        Snak('P31', ('Q13442814',)),
        {'datatype': 'wikibase-item', 'datavalue': {'type': 'wikibase-entityid',
         'value': {'entity-type': 'item', 'id': 'Q13442814', 'numeric-id': 13442814}},
         'property': 'P31', 'snaktype': 'value'}
    ),
    'quantity': (
        Snak('P6879', ('+5214', '+5181', '+5247', 'http://www.wikidata.org/entity/Q11579')),
        {'datatype': 'quantity', 'datavalue': {'type': 'quantity',
         'value': {'amount': '+5214', 'lowerBound': '+5181', 'upperBound': '+5247',
         'unit': 'http://www.wikidata.org/entity/Q11579'}},
         'property': 'P6879', 'snaktype': 'value'}
    ),
    'time': (
        Snak('P813', ('20240526', '11', 'Q1985727')),
        {'datatype': 'time', 'datavalue': {'type': 'time',
         'value': {'after': 0, 'before': 0, 'calendarmodel':
         'http://www.wikidata.org/entity/Q1985727', 'precision': 11,
         'time': '+2024-05-26T00:00:00Z', 'timezone': 0}},
         'property': 'P813', 'snaktype': 'value'}
    ),
    'monolingual': (
        Snak('P1448', ('Toliman', 'en')),
        {'datatype': 'monolingualtext', 'datavalue': {'type': 'monolingualtext',
         'value': {'language': 'en', 'text': 'Toliman'}},
         'property': 'P1448', 'snaktype': 'value'}
    ),
    'string': (
        Snak('P215', ('K1V',)),
        {'datatype': 'string', 'datavalue': {'type': 'string', 'value': 'K1V'},
         'property': 'P215', 'snaktype': 'value'}
    ),
    'novalue': (
        Snak('P818', None, 'novalue'),
        {'datatype': 'external-id', 'property': 'P818', 'snaktype': 'novalue'}
    )
}

class TypeOf(TestCase):
    @mock.patch.object(Snak, '_PROPERTY_TYPES', None)
    def test_sparql_queried_once(self):
        with mock.patch('wdpy.snak.exec', return_value={
            'http://www.wikidata.org/entity/P6': {'t': 'http://wikiba.se/ontology#WikibaseItem'},
            'http://www.wikidata.org/entity/P10': {'t': 'http://wikiba.se/ontology#CommonsMedia'},
            'http://www.wikidata.org/entity/P212': {'t': 'http://wikiba.se/ontology#ExternalId'},
            'http://www.wikidata.org/entity/P215': {'t': 'http://wikiba.se/ontology#String'},
            'http://www.wikidata.org/entity/P569': {'t': 'http://wikiba.se/ontology#Time'},
            'http://www.wikidata.org/entity/P625': {'t': 'http://wikiba.se/ontology#GlobeCoordinate'},
            'http://www.wikidata.org/entity/P854': {'t': 'http://wikiba.se/ontology#Url'},
            'http://www.wikidata.org/entity/P1081': {'t': 'http://wikiba.se/ontology#Quantity'},
            'http://www.wikidata.org/entity/P1448': {'t': 'http://wikiba.se/ontology#Monolingualtext'},
            'http://www.wikidata.org/entity/P1647': {'t': 'http://wikiba.se/ontology#WikibaseProperty'},
            'http://www.wikidata.org/entity/P2534': {'t': 'http://wikiba.se/ontology#Math'},
            'http://www.wikidata.org/entity/P3896': {'t': 'http://wikiba.se/ontology#GeoShape'},
            'http://www.wikidata.org/entity/P4045': {'t': 'http://wikiba.se/ontology#TabularData'},
            'http://www.wikidata.org/entity/P5188': {'t': 'http://wikiba.se/ontology#WikibaseLexeme'},
            'http://www.wikidata.org/entity/P5972': {'t': 'http://wikiba.se/ontology#WikibaseSense'},
            'http://www.wikidata.org/entity/P8017': {'t': 'http://wikiba.se/ontology#WikibaseForm'},
            'http://www.wikidata.org/entity/P12861': {'t': 'http://wikiba.se/ontology#WikibaseEntitySchema'}
        }) as exec_mock:
            self.assertIsNone(Snak.type_of('P1'))
            self.assertEqual(Snak.type_of('P6'), 'wikibase-item')
            self.assertEqual(Snak.type_of('P10'), 'commonsMedia')
            self.assertEqual(Snak.type_of('P212'), 'external-id')
            self.assertEqual(Snak.type_of('P215'), 'string')
            self.assertEqual(Snak.type_of('P569'), 'time')
            self.assertEqual(Snak.type_of('P625'), 'globecoordinate')
            self.assertEqual(Snak.type_of('P854'), 'url')
            self.assertEqual(Snak.type_of('P1081'), 'quantity')
            self.assertEqual(Snak.type_of('P1448'), 'monolingualtext')
            self.assertEqual(Snak.type_of('P1647'), 'wikibase-property')
            self.assertEqual(Snak.type_of('P2534'), 'math')
            self.assertEqual(Snak.type_of('P3896'), 'geo-shape')
            self.assertEqual(Snak.type_of('P4045'), 'tabular-data')
            self.assertEqual(Snak.type_of('P5972'), 'wikibase-sense')
            self.assertEqual(Snak.type_of('P5188'), 'wikibase-lexeme')
            self.assertEqual(Snak.type_of('P8017'), 'wikibase-form')
            self.assertEqual(Snak.type_of('P12861'), 'entity-schema')
            exec_mock.assert_called_once()
            self.assertEqual(Snak.type_of('P6'), 'wikibase-item')
            exec_mock.assert_called_once()

    @mock.patch.object(Snak, '_PROPERTY_TYPES', None)
    def test_not_existing(self):
        with mock.patch('wdpy.snak.exec', return_value={}):
            self.assertIsNone(Snak.type_of('P1'))

class Parse(TestCase):
    def test_item(self):
        expected, payload = ETHALONS['item']
        res = Snak.parse(payload)
        self.assertEqual(res.property, expected.property)
        self.assertEqual(res.value, expected.value)

    def test_quantity(self):
        expected, payload = ETHALONS['quantity']
        self.assertEqual(Snak.parse(payload).value, expected.value)

    def test_time(self):
        expected, payload = ETHALONS['time']
        self.assertEqual(Snak.parse(payload).value, expected.value)

    def test_monolingual(self):
        expected, payload = ETHALONS['monolingual']
        self.assertEqual(Snak.parse(payload).value, expected.value)

    def test_string(self):
        expected, payload = ETHALONS['string']
        self.assertEqual(Snak.parse(payload).value, expected.value)

    def test_novalue(self):
        expected, payload = ETHALONS['novalue']
        res = Snak.parse(payload)
        self.assertEqual(res.property, expected.property)
        self.assertIsNone(res.value)
        self.assertEqual(res.snaktype, expected.snaktype)

@mock.patch('wdpy.snak.exec', return_value={
    'http://www.wikidata.org/entity/P31': {'t': 'http://wikiba.se/ontology#WikibaseItem'},
    'http://www.wikidata.org/entity/P813': {'t': 'http://wikiba.se/ontology#Time'},
    'http://www.wikidata.org/entity/P215': {'t': 'http://wikiba.se/ontology#String'},
    'http://www.wikidata.org/entity/P6879': {'t': 'http://wikiba.se/ontology#Quantity'},
    'http://www.wikidata.org/entity/P1448': {'t': 'http://wikiba.se/ontology#Monolingualtext'},
    'http://www.wikidata.org/entity/P818': {'t': 'http://wikiba.se/ontology#ExternalId'}
})
class Json(TestCase):
    def setUp(self):
        self.type_of_patcher = mock.patch.object(Snak, 'type_of')
        self.mock_type_of = self.type_of_patcher.start()
        self.mock_type_of.side_effect = self._type_of_side_effect

    def tearDown(self):
        self.type_of_patcher.stop()

    def _type_of_side_effect(self, property_id):
        mapping = {
            'P31': 'wikibase-item',
            'P813': 'time',
            'P215': 'string',
            'P6879': 'quantity',
            'P1448': 'monolingualtext',
            'P818': 'external-id'
        }
        return mapping.get(property_id)

    def test_item(self, *_):
        snak, expected = ETHALONS['item']
        self.assertEqual(json.loads(snak.json()), expected)

    def test_quantity(self, *_):
        snak, expected = ETHALONS['quantity']
        self.assertEqual(json.loads(snak.json()), expected)

    def test_time(self, *_):
        snak, expected = ETHALONS['time']
        self.assertEqual(json.loads(snak.json()), expected)

    @mock.patch.object(Snak, 'type_of', return_value='time')
    def test_time_no_precision_defaults_to_11(self, *_):
        snak = Snak('P813', ('20240526',))
        payload = json.loads(snak.json())
        self.assertEqual(11, payload['datavalue']['value']['precision'])

    @mock.patch.object(Snak, 'type_of', return_value='time')
    def test_time_precision_suffix_in_time_string(self, *_):
        snak = Snak('P813', ('+2013-01-00T00:00:00Z/10',))
        payload = json.loads(snak.json())
        self.assertEqual(10, payload['datavalue']['value']['precision'])
        self.assertEqual('+2013-01-00T00:00:00Z', payload['datavalue']['value']['time'])

    def test_monolingual(self, *_):
        snak, expected = ETHALONS['monolingual']
        self.assertEqual(json.loads(snak.json()), expected)

    def test_string(self, *_):
        snak, expected = ETHALONS['string']
        self.assertEqual(json.loads(snak.json()), expected)

    def test_novalue(self, *_):
        snak, expected = ETHALONS['novalue']
        self.assertEqual(json.loads(snak.json()), expected)

@mock.patch('wdpy.snak.exec', return_value={})
class Create(TestCase):
    @mock.patch.object(Snak, 'type_of', return_value='time')
    def test_date_parser(self, *_):
        self.assertEqual(('19870000', '9', 'Q1985727'), Snak.create('P813', '1987').value)
        self.assertEqual(('20090400', '10', 'Q1985727'), Snak.create('P813', '2009-04').value)
        self.assertEqual(('20090412', '11', 'Q1985727'), Snak.create('P813', '2009-04-12').value)
        self.assertEqual(('20090402', '11', 'Q1985727'), Snak.create('P813', '2009-04-2').value)
        self.assertEqual(('34560102', '11', 'Q1985727'), Snak.create('P813', '1/2/3456').value)
        self.assertEqual(('19030100', '10', 'Q1985727'), Snak.create('P813', '01/1903').value)
        self.assertEqual(('20130100', '10', 'Q1985727'), Snak.create('P813', '2013-01-00').value)

    @mock.patch.object(Snak, 'type_of', return_value='wikibase-item')
    def test_snaktype(self, *_):
        novalue_snak = Snak.create('P31', 'Q5', 'novalue')
        self.assertEqual('P31', novalue_snak.property)
        self.assertIsNone(novalue_snak.value)
        self.assertEqual('novalue', novalue_snak.snaktype)
        somevalue_snak = Snak.create('P31', None, 'somevalue')
        self.assertEqual('P31', somevalue_snak.property)
        self.assertIsNone(somevalue_snak.value)
        self.assertEqual('somevalue', somevalue_snak.snaktype)

    @mock.patch.object(Snak, 'type_of', return_value='wikibase-item')
    def test_none_value(self, *_):
        snak = Snak.create('P31', None)
        self.assertEqual('P31', snak.property)
        self.assertIsNone(snak.value)
        self.assertEqual('value', snak.snaktype)

    @mock.patch.object(Snak, 'type_of', return_value='wikibase-item')
    def test_tuple_list_value(self, *_):
        self.assertEqual(('Q5',), Snak.create('P31', ('Q5',)).value)
        self.assertEqual(('Q5',), Snak.create('P31', ['Q5']).value)

    @mock.patch.object(Snak, 'type_of', return_value='quantity')
    def test_quantity_value(self, *_):
        self.assertEqual(('10', '', '', '1'), Snak.create('P1081', 10).value)
        self.assertEqual(('10.5', '', '', '1'), Snak.create('P1081', '10.5').value)

    @mock.patch.object(Snak, 'type_of', return_value='time')
    def test_time_date_objects(self, *_):
        expected = ('20240526', '11', 'Q1985727')
        self.assertEqual(expected, Snak.create('P813', date(2024, 5, 26)).value)
        self.assertEqual(expected, Snak.create('P813', datetime(2024, 5, 26)).value)

    @mock.patch.object(Snak, 'type_of', return_value='time')
    def test_time_unparseable(self, *_):
        self.assertIsNone(Snak.create('P813', 'not a date'))
        self.assertIsNone(Snak.create('P813', '  '))

    @mock.patch.object(Snak, 'type_of', return_value='quantity')
    def test_quantity_unparseable(self, *_):
        self.assertIsNone(Snak.create('P1081', '  '))

    @mock.patch.object(Snak, 'type_of', return_value='string')
    def test_default_string(self, *_):
        self.assertEqual(('hello',), Snak.create('P215', 'hello').value)
        self.assertEqual(('123',), Snak.create('P215', 123).value)

class CanMergeReferences(TestCase):
    def test_identical(self):
        s = Snak('P248', ('Q1',))
        self.assertTrue(Snak.can_merge_references([s], [s]))

    def test_different_p248(self):
        self.assertFalse(Snak.can_merge_references(
            [Snak('P248', ('Q1',))], [Snak('P248', ('Q2',))]))

    def test_different_p12132(self):
        self.assertFalse(Snak.can_merge_references(
            [Snak('P12132', ('Q3',))], [Snak('P12132', ('Q4',))]))

    @mock.patch.object(Snak, 'type_of', return_value='wikibase-item')
    def test_unsupported_property(self, *_):
        self.assertFalse(Snak.can_merge_references([Snak('P31', ('Q5',))], []))

    @mock.patch.object(Snak, 'type_of', return_value='external-id')
    def test_external_id_match(self, *_):
        self.assertTrue(Snak.can_merge_references(
            [Snak('P818', ('1',))], [Snak('P818', ('1',))]))

    @mock.patch.object(Snak, 'type_of', return_value='external-id')
    def test_external_id_mismatch(self, *_):
        self.assertFalse(Snak.can_merge_references(
            [Snak('P818', ('1',))], [Snak('P818', ('2',))]))

class TryToMergeReferences(TestCase):
    def test_basic(self):
        s1, s2 = Snak('P248', ('Q1',)), Snak('P12132', ('X',))
        source, target = [s1, s2], [s1]
        self.assertTrue(Snak.try_to_merge_references(source, target))
        self.assertEqual(source, target)
        self.assertIsNot(source, target)

    def test_prune(self):
        s1, s2 = Snak('P248', ('Q1',)), Snak('P12132', ('X',))
        source, target = [s1], [s1, s2]
        self.assertTrue(Snak.try_to_merge_references(source, target))
        self.assertEqual(source, target)

    def test_fails_validation(self):
        s1, s2 = Snak('P248', ('Q1',)), Snak('P248', ('Q2',))
        source, target = [s1], [s2]
        self.assertFalse(Snak.try_to_merge_references(source, target))
        self.assertEqual([s2], target)

class GetP248Qids(TestCase):
    def test_extraction(self):
        items = [[Snak('P248', ('Q1',)), Snak('P31', ('Q5',))],
                 [Snak('P248', ('Q2',)), Snak('P248', ('Q1',))]]
        self.assertEqual(Snak.get_p248_qids(items), {'Q1', 'Q2'})

    def test_empty(self):
        self.assertEqual(Snak.get_p248_qids([]), set())
        self.assertEqual(Snak.get_p248_qids([[Snak('P31', ('Q5',))]]), set())

    def test_missing_values(self):
        items = [[Snak('P248', None), Snak('P248', ()),
                  Snak('P248', ('',)), Snak('P248', ('Q3',))]]
        self.assertEqual(Snak.get_p248_qids(items), {'Q3'})

class ResolveRedirect(TestCase):
    def test_basic(self):
        s = Snak('P248', ('Q2',))
        Snak.resolve_redirect([[s]], {'Q2': 'Q1'})
        self.assertEqual(s.value, ('Q1',))

    def test_no_match(self):
        s = Snak('P248', ('Q3',))
        Snak.resolve_redirect([[s]], {'Q2': 'Q1'})
        self.assertEqual(s.value, ('Q3',))

    def test_with_extra_values(self):
        s = Snak('P813', ('20240526', '11', 'Q1985727'))
        # P813 is not P248, so it won't be resolved by the new bulk logic
        # Wait, if I incorporate lines 84-87, it ONLY handles P248.
        # Let's check if the user wanted ONLY P248 or ALL snaks.
        # Lines 84-87 were: if snak.property == 'P248': snak.resolve_redirect(_REDIRECTS)
        # So yes, only P248.
        Snak.resolve_redirect([[s]], {'20240526': '20240527'})
        self.assertEqual(s.value, ('20240526', '11', 'Q1985727'))
