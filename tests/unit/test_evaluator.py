import unittest
from unittest.mock import patch

import wdpy
from wdpy.evaluator.rules.single_best_rank import SingleBestRankRule
from wdpy.evaluator.rules.ref_snak_redundancy import RefSnakRedundancyRule


def _item(*props_and_stmts):
    """Build a loaded Item from (prop, [Statement, ...]) pairs."""
    item = wdpy.Item('Q1')
    item._loaded = True
    item.claims = dict(props_and_stmts)
    return item


def _stmt(prop, value=('Q5',), rank='normal', qualifiers=None, refs=None):
    s = wdpy.Statement(wdpy.Snak(prop, value), rank=rank, qualifiers=qualifiers)
    if refs is not None:
        s.references = refs
    return s


def _qual(prop, value):
    return wdpy.Snak(prop, (value,))


def _refs_with_snaks(*snaks):
    r = wdpy.References()
    r._items = [list(snaks)]
    return r


# ── SingleBestRankRule ────────────────────────────────────────────────────────

class TestSingleBestRankRule(unittest.TestCase):

    def setUp(self):
        self.rule = SingleBestRankRule()

    def _check(self, item):
        return self.rule.check(item, {})

    @patch('wdpy.Snak.type_of', return_value='string')
    def test_single_normal_no_violation(self, _):
        item = _item(('P31', [_stmt('P31')]))
        self.assertEqual([], self._check(item))

    @patch('wdpy.Snak.type_of', return_value='string')
    def test_single_preferred_is_violation(self, _):
        item = _item(('P31', [_stmt('P31', rank='preferred')]))
        violations = self._check(item)
        self.assertEqual(1, len(violations))
        self.assertEqual('single_best_rank', violations[0].rule)
        self.assertEqual('P31', violations[0].property_id)

    @patch('wdpy.Snak.type_of', return_value='string')
    def test_single_preferred_plus_deprecated_is_violation(self, _):
        item = _item(('P31', [
            _stmt('P31', rank='preferred'),
            _stmt('P31', rank='deprecated'),
        ]))
        violations = self._check(item)
        self.assertEqual(1, len(violations))
        self.assertEqual('single_best_rank', violations[0].rule)

    @patch('wdpy.Snak.type_of', return_value='string')
    def test_two_normal_no_preferred_is_violation(self, _):
        item = _item(('P577', [_stmt('P577'), _stmt('P577')]))
        violations = self._check(item)
        self.assertEqual(1, len(violations))
        self.assertEqual('single_best_rank', violations[0].rule)
        self.assertEqual('P577', violations[0].property_id)

    @patch('wdpy.Snak.type_of', return_value='string')
    def test_two_preferred_is_violation(self, _):
        item = _item(('P577', [
            _stmt('P577', rank='preferred', value=('20230101', '11', 'Q1985727')),
            _stmt('P577', rank='preferred', value=('20230102', '11', 'Q1985727')),
        ]))
        violations = self._check(item)
        self.assertEqual(2, len(violations))
        self.assertTrue(all(v.rule == 'single_best_rank' for v in violations))

    @patch('wdpy.Snak.type_of', return_value='string')
    def test_one_preferred_one_normal_no_violation(self, _):
        # One preferred + one normal is valid (preferred is the single best)
        item = _item(('P577', [
            _stmt('P577', rank='preferred', value=('20230101', '11', 'Q1985727')),
            _stmt('P577', rank='normal', value=('2023', '9', 'Q1985727')),
        ]))
        self.assertEqual([], self._check(item))

    @patch('wdpy.Snak.type_of', return_value='string')
    def test_authors_different_ordinals_no_violation(self, _):
        item = _item(
            ('P2093', [
                _stmt('P2093', value=('Alice',), qualifiers=[_qual('P1545', '1')]),
                _stmt('P2093', value=('Bob',), qualifiers=[_qual('P1545', '2')]),
            ])
        )
        self.assertEqual([], self._check(item))

    @patch('wdpy.Snak.type_of', return_value='string')
    def test_authors_same_ordinal_is_violation(self, _):
        item = _item(
            ('P2093', [
                _stmt('P2093', value=('Alice',), qualifiers=[_qual('P1545', '1')]),
                _stmt('P2093', value=('A. Smith',), qualifiers=[_qual('P1545', '1')]),
            ])
        )
        violations = self._check(item)
        self.assertEqual(1, len(violations))
        self.assertEqual('single_best_rank', violations[0].rule)

    @patch('wdpy.Snak.type_of', return_value='string')
    def test_p50_and_p2093_same_ordinal_is_violation(self, _):
        item = _item(
            ('P50', [_stmt('P50', value=('Q42',), qualifiers=[_qual('P1545', '1')])]),
            ('P2093', [_stmt('P2093', value=('Alice',), qualifiers=[_qual('P1545', '1')])]),
        )
        violations = self._check(item)
        self.assertEqual(1, len(violations))

    @patch('wdpy.Snak.type_of', return_value='string')
    def test_author_deprecated_not_counted(self, _):
        item = _item(
            ('P2093', [
                _stmt('P2093', value=('Alice',), qualifiers=[_qual('P1545', '1')]),
                _stmt('P2093', value=('A. Smith',), rank='deprecated',
                      qualifiers=[_qual('P1545', '1')]),
            ])
        )
        self.assertEqual([], self._check(item))

    @patch('wdpy.Snak.type_of', return_value='string')
    def test_all_deprecated_no_violation(self, _):
        item = _item(('P31', [
            _stmt('P31', rank='deprecated'),
            _stmt('P31', rank='deprecated'),
        ]))
        self.assertEqual([], self._check(item))


# ── RefSnakRedundancyRule ─────────────────────────────────────────────────────

class TestRefSnakRedundancyRule(unittest.TestCase):

    def setUp(self):
        self.rule = RefSnakRedundancyRule()

    def _check(self, item):
        return self.rule.check(item, {})

    def _p248_snak(self, qid='Q180445'):
        return wdpy.Snak('P248', (qid,))

    def _ext_snak(self, prop='P819', value='2024abc'):
        return wdpy.Snak(prop, (value,))

    @patch('wdpy.evaluator.rules.ref_snak_redundancy.Snak.type_of', return_value='external-id')
    def test_no_violation_when_two_statements_for_ext_id(self, _):
        # Two non-deprecated P819 statements → reference snak is NOT redundant
        refs = _refs_with_snaks(self._p248_snak(), self._ext_snak('P819'))
        stmt = _stmt('P1476', value=('Title', 'en'), refs=refs)
        item = _item(
            ('P1476', [stmt]),
            ('P819', [_stmt('P819'), _stmt('P819', value=('other',))]),
        )
        self.assertEqual([], self._check(item))

    @patch('wdpy.evaluator.rules.ref_snak_redundancy.Snak.type_of', return_value='external-id')
    def test_violation_when_one_statement_for_ext_id(self, _):
        # Exactly one non-deprecated P819 → reference snak is redundant
        refs = _refs_with_snaks(self._p248_snak(), self._ext_snak('P819'))
        stmt = _stmt('P1476', value=('Title', 'en'), refs=refs)
        item = _item(
            ('P1476', [stmt]),
            ('P819', [_stmt('P819')]),
        )
        violations = self._check(item)
        self.assertEqual(1, len(violations))
        self.assertEqual('ref_snak_redundancy', violations[0].rule)
        self.assertEqual('P1476', violations[0].property_id)

    @patch('wdpy.evaluator.rules.ref_snak_redundancy.Snak.type_of', return_value='external-id')
    def test_no_violation_when_ext_id_not_on_item(self, _):
        # P819 not in item claims at all → count is 0, not a violation
        refs = _refs_with_snaks(self._p248_snak(), self._ext_snak('P819'))
        stmt = _stmt('P1476', value=('Title', 'en'), refs=refs)
        item = _item(('P1476', [stmt]))
        self.assertEqual([], self._check(item))

    @patch('wdpy.evaluator.rules.ref_snak_redundancy.Snak.type_of', return_value='external-id')
    def test_no_violation_when_only_deprecated_statement(self, _):
        # One deprecated P819 → active count is 0 → not a violation
        refs = _refs_with_snaks(self._p248_snak(), self._ext_snak('P819'))
        stmt = _stmt('P1476', value=('Title', 'en'), refs=refs)
        item = _item(
            ('P1476', [stmt]),
            ('P819', [_stmt('P819', rank='deprecated')]),
        )
        self.assertEqual([], self._check(item))

    @patch('wdpy.evaluator.rules.ref_snak_redundancy.Snak.type_of', return_value='external-id')
    def test_no_violation_when_ref_lacks_p248(self, _):
        # Reference has external ID snak but no P248 → rule does not apply
        refs = _refs_with_snaks(self._ext_snak('P819'))
        stmt = _stmt('P1476', value=('Title', 'en'), refs=refs)
        item = _item(
            ('P1476', [stmt]),
            ('P819', [_stmt('P819')]),
        )
        self.assertEqual([], self._check(item))

    @patch('wdpy.evaluator.rules.ref_snak_redundancy.Snak.type_of', return_value='external-id')
    def test_no_violation_when_ref_has_only_p248(self, _):
        refs = _refs_with_snaks(self._p248_snak())
        stmt = _stmt('P1476', value=('Title', 'en'), refs=refs)
        item = _item(
            ('P1476', [stmt]),
            ('P819', [_stmt('P819')]),
        )
        self.assertEqual([], self._check(item))

    @patch('wdpy.evaluator.rules.ref_snak_redundancy.Snak.type_of', return_value='string')
    def test_no_violation_when_non_external_id_snak_in_ref(self, _):
        # Snak in reference is not external-id type → not flagged
        non_ext = wdpy.Snak('P813', ('20240101', '11', 'Q1985727'))
        refs = _refs_with_snaks(self._p248_snak(), non_ext)
        stmt = _stmt('P1476', value=('Title', 'en'), refs=refs)
        item = _item(('P1476', [stmt]))
        self.assertEqual([], self._check(item))


# ── RankQualifierRule ─────────────────────────────────────────────────────────

from wdpy.evaluator.rules.rank_qualifier import RankQualifierRule


class TestRankQualifierRule(unittest.TestCase):

    def _check(self, item):
        return RankQualifierRule().check(item, {})

    def test_normal_rank_no_violation(self):
        item = _item(('P577', [_stmt('P577')]))
        self.assertEqual([], self._check(item))

    def test_preferred_with_p7452_no_violation(self):
        item = _item(('P577', [
            _stmt('P577', rank='preferred', qualifiers=[_qual('P7452', 'Q71536040')]),
        ]))
        self.assertEqual([], self._check(item))

    def test_deprecated_with_p2241_no_violation(self):
        item = _item(('P577', [
            _stmt('P577', rank='deprecated', qualifiers=[_qual('P2241', 'Q42727519')]),
        ]))
        self.assertEqual([], self._check(item))

    def test_preferred_without_p7452_is_violation(self):
        stmt = _stmt('P577', rank='preferred')
        item = _item(('P577', [stmt]))
        violations = self._check(item)
        self.assertEqual(1, len(violations))
        self.assertEqual('rank_qualifier', violations[0].rule)
        self.assertEqual('P577', violations[0].property_id)

    def test_deprecated_without_p2241_is_violation(self):
        stmt = _stmt('P577', rank='deprecated')
        item = _item(('P577', [stmt]))
        violations = self._check(item)
        self.assertEqual(1, len(violations))
        self.assertEqual('rank_qualifier', violations[0].rule)
        self.assertEqual('P577', violations[0].property_id)


if __name__ == '__main__':
    unittest.main()
