from decimal import Decimal
from unittest import TestCase, mock

from wd import Model


class TestModel(TestCase):
    def test_format_float(self):
        self.assertEqual('0.12345679', Model.format_float('0.123456789', 8))
        self.assertEqual(0, Decimal(Model.format_float('+0E-7', 8)))

    def test_date_parser(self):
        self.assertIsNone(Model.parse_date(''))
        self.assertEqual('+1987-00-00T00:00:00Z', Model.parse_date('1987')['time'])
        self.assertEqual(9, Model.parse_date('1987')['precision'])
        self.assertEqual(0, Model.parse_date('1987')['timezone'])
        self.assertEqual(0, Model.parse_date('1987')['before'])
        self.assertEqual(0, Model.parse_date('1987')['after'])
        self.assertEqual('http://www.wikidata.org/entity/Q1985727', Model.parse_date('1987')['calendarmodel'])
        self.assertEqual('+2009-04-00T00:00:00Z', Model.parse_date('2009-04')['time'])
        self.assertEqual(10, Model.parse_date('2009-04')['precision'])
        self.assertEqual('+2009-04-12T00:00:00Z', Model.parse_date('2009-04-12')['time'])
        self.assertEqual(11, Model.parse_date('2009-4-12')['precision'])
        self.assertEqual('+2009-04-02T00:00:00Z', Model.parse_date('2009-04-2')['time'])
        self.assertEqual('+3456-02-01T00:00:00Z', Model.parse_date('1/2/3456')['time'])
        self.assertEqual('+1903-01-00T00:00:00Z', Model.parse_date('01/1903')['time'])
        self.assertIsNone(Model.parse_date('29/16/1924'))

    @mock.patch('wd.Wikidata.type_of', return_value='wikibase-item')
    def test_qualifier_filter(self, _):
        self.assertTrue(Model.qualifier_filter({'qualifiers': {}}, {}))
        self.assertFalse(Model.qualifier_filter({'qualifiers': {'P972': 'Q1'}}, {}))
        q2 = {'qualifiers': {'P972': [Model.create_snak('P972', 'Q2')]}}
        self.assertFalse(Model.qualifier_filter({'qualifiers': {'P1227': 'Q2'}}, q2))
        self.assertFalse(Model.qualifier_filter({'qualifiers': {'P972': 'Q1'}}, q2))
        self.assertTrue(Model.qualifier_filter({'qualifiers': {'P972': 'Q2'}}, q2))
