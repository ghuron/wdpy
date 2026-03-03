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


class Upsert(TestCase):

    # --- _qualifiers_present_in ---

    def test_no_qualifiers_matches_any_candidate(self):
        incoming = Statement(Snak('P31', ('Q5',)))
        candidate = Statement(Snak('P31', ('Q5',)), qualifiers=[Snak('P580', ('2020',))])
        self.assertTrue(incoming._qualifiers_present_in(candidate))

    def test_self_has_qualifiers_candidate_has_none(self):
        incoming = Statement(Snak('P31', ('Q5',)), qualifiers=[Snak('P580', ('2020',))])
        candidate = Statement(Snak('P31', ('Q5',)))
        self.assertFalse(incoming._qualifiers_present_in(candidate))

    def test_qualifier_property_absent_in_candidate(self):
        incoming = Statement(Snak('P31', ('Q5',)), qualifiers=[Snak('P580', ('2020',))])
        candidate = Statement(Snak('P31', ('Q5',)), qualifiers=[Snak('P582', ('2021',))])
        self.assertFalse(incoming._qualifiers_present_in(candidate))

    def test_qualifier_value_differs(self):
        incoming = Statement(Snak('P31', ('Q5',)), qualifiers=[Snak('P580', ('2020',))])
        candidate = Statement(Snak('P31', ('Q5',)), qualifiers=[Snak('P580', ('2019',))])
        self.assertFalse(incoming._qualifiers_present_in(candidate))

    def test_qualifier_matches(self):
        q = Snak('P580', ('2020',))
        incoming = Statement(Snak('P31', ('Q5',)), qualifiers=[q])
        candidate = Statement(Snak('P31', ('Q5',)), qualifiers=[q])
        self.assertTrue(incoming._qualifiers_present_in(candidate))

    def test_candidate_extra_qualifiers_still_matches(self):
        incoming = Statement(Snak('P31', ('Q5',)), qualifiers=[Snak('P580', ('2020',))])
        candidate = Statement(Snak('P31', ('Q5',)),
                              qualifiers=[Snak('P580', ('2020',)), Snak('P582', ('2021',))])
        self.assertTrue(incoming._qualifiers_present_in(candidate))

    def test_all_qualifiers_must_match(self):
        incoming = Statement(Snak('P31', ('Q5',)),
                             qualifiers=[Snak('P580', ('2020',)), Snak('P582', ('2021',))])
        candidate = Statement(Snak('P31', ('Q5',)), qualifiers=[Snak('P580', ('2020',))])
        self.assertFalse(incoming._qualifiers_present_in(candidate))

    # --- upsert: matching logic ---

    def test_empty_existing_returns_none(self):
        self.assertIsNone(Statement(Snak('P31', ('Q5',))).upsert([]))

    def test_value_mismatch_returns_none(self):
        incoming = Statement(Snak('P31', ('Q5',)))
        self.assertIsNone(incoming.upsert([Statement(Snak('P31', ('Q6',)))]))

    def test_value_match_returns_candidate(self):
        incoming = Statement(Snak('P31', ('Q5',)))
        candidate = Statement(Snak('P31', ('Q5',)))
        self.assertIs(incoming.upsert([candidate]), candidate)

    def test_qualifier_mismatch_returns_none(self):
        incoming = Statement(Snak('P31', ('Q5',)), qualifiers=[Snak('P580', ('2020',))])
        candidate = Statement(Snak('P31', ('Q5',)), qualifiers=[Snak('P580', ('2019',))])
        self.assertIsNone(incoming.upsert([candidate]))

    def test_novalue_match(self):
        incoming = Statement(Snak('P31', None, 'novalue'))
        candidate = Statement(Snak('P31', None, 'novalue'))
        self.assertIs(incoming.upsert([candidate]), candidate)

    def test_somevalue_match(self):
        incoming = Statement(Snak('P31', None, 'somevalue'))
        candidate = Statement(Snak('P31', None, 'somevalue'))
        self.assertIs(incoming.upsert([candidate]), candidate)

    def test_novalue_vs_value_no_match(self):
        incoming = Statement(Snak('P31', None, 'novalue'))
        self.assertIsNone(incoming.upsert([Statement(Snak('P31', ('Q5',)))]))

    def test_value_vs_novalue_no_match(self):
        incoming = Statement(Snak('P31', ('Q5',)))
        self.assertIsNone(incoming.upsert([Statement(Snak('P31', None, 'novalue'))]))

    def test_first_match_wins(self):
        incoming = Statement(Snak('P31', ('Q5',)))
        c1, c2 = Statement(Snak('P31', ('Q5',))), Statement(Snak('P31', ('Q5',)))
        self.assertIs(incoming.upsert([c1, c2]), c1)

    def test_skips_non_matching_before_match(self):
        incoming = Statement(Snak('P31', ('Q5',)))
        miss = Statement(Snak('P31', ('Q6',)))
        hit = Statement(Snak('P31', ('Q5',)))
        self.assertIs(incoming.upsert([miss, hit]), hit)

    # --- upsert: reference merging ---

    def test_no_incoming_references_leaves_candidate_unchanged(self):
        incoming = Statement(Snak('P31', ('Q5',)))
        candidate = Statement(Snak('P31', ('Q5',)))
        incoming.upsert([candidate])
        self.assertIsNone(candidate.references)

    @mock.patch('wdpy.references._preload')
    def test_references_merged_into_candidate_with_none_refs(self, *_):
        ref = Snak('P248', ('Q123',))
        incoming = Statement(Snak('P31', ('Q5',)),
                             references=References([{'snaks': {'P248': [ref]}}]))
        candidate = Statement(Snak('P31', ('Q5',)))
        incoming.upsert([candidate])
        self.assertIsNotNone(candidate.references)
        self.assertEqual(len(candidate.references._items), 1)

    @mock.patch('wdpy.references._preload')
    def test_different_references_both_kept(self, *_):
        incoming = Statement(Snak('P31', ('Q5',)),
                             references=References([{'snaks': {'P248': [Snak('P248', ('Q2',))]}}]))
        candidate = Statement(Snak('P31', ('Q5',)),
                              references=References([{'snaks': {'P248': [Snak('P248', ('Q1',))]}}]))
        incoming.upsert([candidate])
        self.assertEqual(len(candidate.references._items), 2)

    @mock.patch('wdpy.references._preload')
    @mock.patch.object(Snak, 'try_to_merge_references', return_value=True)
    def test_identical_references_merged_not_duplicated(self, *_):
        ref = {'snaks': {'P248': [Snak('P248', ('Q1',))]}}
        incoming = Statement(Snak('P31', ('Q5',)), references=References([ref]))
        candidate = Statement(Snak('P31', ('Q5',)), references=References([ref]))
        incoming.upsert([candidate])
        self.assertEqual(len(candidate.references._items), 1)
