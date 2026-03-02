import json
from unittest import TestCase, mock
from wdpy import Snak, Statement, References

@mock.patch('wdpy.Snak.type_of', return_value='string')
class Parse(TestCase):
    def test_basic(self, *_):
        s = Statement.parse({'mainsnak': {'property': 'P31'}, 'qualifiers': 
                            {'P433': [{'property': 'P433', 'snaktype': 'value',
                             'datavalue': {'type': 'string', 'value': '1'}}]}})
        self.assertEqual(s.mainsnak.property, 'P31')
        self.assertEqual(s.qualifiers[0].property, 'P433')

@mock.patch('wdpy.Snak.type_of', return_value='string')
class Json(TestCase):
    def test_basic(self, *_):
        s = Statement(Snak('P31', ('Q5',)))
        s.set_qualifier('P433', '1')
        self.assertEqual(json.loads(s.json())['qualifiers']['P433'][0]
                         ['datavalue']['value'], '1')

    def test_full(self, *_):
        s = Statement(Snak('P31', ('Q5',)), id='MyID', rank='preferred',
                      qualifiers=[Snak('P433', ('1',))],
                      references=References([{'snaks': {'P248':
                                 [Snak('P248', ('Q123',))]}}]))
        res = json.loads(s.json())
        self.assertEqual(res['id'], 'MyID')
        self.assertEqual(res['rank'], 'preferred')
        self.assertEqual(len(res['references']), 1)

    def test_minimal(self, *_):
        res = json.loads(Statement(Snak('P31', ('Q5',))).json())
        self.assertNotIn('qualifiers', res)
        self.assertNotIn('references', res)

@mock.patch('wdpy.Snak.type_of', return_value='string')
class SetRank(TestCase):
    def test_preferred(self, *_):
        s = Statement(Snak('P31', ('Q5',)))
        s.set_rank('preferred', 'reason')
        self.assertEqual(s.rank, 'preferred')
        self.assertEqual(s.qualifiers[0].property, 'P7452')

@mock.patch('wdpy.Snak.type_of', return_value='string')
class SetQualifier(TestCase):
    def test_basic(self, *_):
        s = Statement(Snak('P31', ('Q5',)))
        s.set_qualifier('P433', '1')
        self.assertEqual(s.qualifiers[0].value, ('1',))
