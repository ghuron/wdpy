"""Integration tests for the evaluation rules against real Wikidata items.

These tests load real items from Wikidata (no write required) and verify that
the structural rules produce no false positives on well-maintained items.
They also confirm that rules correctly fire on synthetic known-bad fixtures.

Run with:
    python -m unittest tests/integration/test_evaluate.py
"""
import unittest

import wdpy
from wdpy import Item
from wdpy.evaluator.rules.single_best_rank import SingleBestRankRule
from wdpy.evaluator.rules.ref_snak_redundancy import RefSnakRedundancyRule

# Items expected to be in a consistent, bot-maintained state.
# These are arXiv preprints with stable Wikidata records.
_CURATED_QIDS = [
    'Q68126267',  # referenced in project examples
]


class TestStructuralRulesOnRealItems(unittest.TestCase):
    """Structural rules should produce zero violations on well-maintained items."""

    def _load(self, qid: str) -> Item:
        item = Item(qid)
        item._ensure_loaded()
        return item

    def test_single_best_rank_on_curated_items(self):
        rule = SingleBestRankRule()
        for qid in _CURATED_QIDS:
            with self.subTest(qid=qid):
                item = self._load(qid)
                violations = rule.check(item, {})
                self.assertEqual(
                    [], violations,
                    msg=f'{qid}: {[str(v) for v in violations]}',
                )

    def test_ref_snak_redundancy_on_curated_items(self):
        rule = RefSnakRedundancyRule()
        for qid in _CURATED_QIDS:
            with self.subTest(qid=qid):
                item = self._load(qid)
                violations = rule.check(item, {})
                self.assertEqual(
                    [], violations,
                    msg=f'{qid}: {[str(v) for v in violations]}',
                )


class TestKnownBadFixtures(unittest.TestCase):
    """Rules must fire on synthetic items that violate invariants."""

    def _item(self, **claims):
        item = Item('Q1')
        item._loaded = True
        item.claims = {p: stmts for p, stmts in claims.items()}
        return item

    def _stmt(self, prop, value=('Q5',), rank='normal', qualifiers=None):
        return wdpy.Statement(wdpy.Snak(prop, value), rank=rank, qualifiers=qualifiers)

    def _qual(self, prop, value):
        return wdpy.Snak(prop, (value,))

    def test_single_best_rank_fires_on_double_normal(self):
        rule = SingleBestRankRule()
        item = self._item(P577=[
            self._stmt('P577', value=('20230101', '11', 'Q1985727')),
            self._stmt('P577', value=('2023', '9', 'Q1985727')),
        ])
        from unittest.mock import patch
        with patch('wdpy.Snak.type_of', return_value='time'):
            violations = rule.check(item, {})
        self.assertTrue(violations, 'Expected a violation for two normal-rank P577 statements')
        self.assertEqual('single_best_rank', violations[0].rule)

    def test_single_best_rank_fires_on_double_author(self):
        rule = SingleBestRankRule()
        item = self._item(P2093=[
            self._stmt('P2093', value=('Alice',), qualifiers=[self._qual('P1545', '1')]),
            self._stmt('P2093', value=('A. Smith',), qualifiers=[self._qual('P1545', '1')]),
        ])
        from unittest.mock import patch
        with patch('wdpy.Snak.type_of', return_value='string'):
            violations = rule.check(item, {})
        self.assertTrue(violations, 'Expected a violation for duplicate P2093 at ordinal 1')

    def test_ref_snak_redundancy_fires_on_redundant_snak(self):
        rule = RefSnakRedundancyRule()
        p248 = wdpy.Snak('P248', ('Q180445',))
        p819 = wdpy.Snak('P819', ('2024abc',))
        refs = wdpy.References()
        refs._items = [[p248, p819]]
        stmt = wdpy.Statement(
            wdpy.Snak('P1476', ('Title', 'en')),
            references=refs,
        )
        item = self._item(
            P1476=[stmt],
            P819=[self._stmt('P819', value=('2024abc',))],
        )
        from unittest.mock import patch
        with patch('wdpy.evaluator.rules.ref_snak_redundancy.Snak.type_of',
                   return_value='external-id'):
            violations = rule.check(item, {})
        self.assertTrue(violations, 'Expected a violation for redundant P819 reference snak')
        self.assertEqual('ref_snak_redundancy', violations[0].rule)


if __name__ == '__main__':
    unittest.main()
