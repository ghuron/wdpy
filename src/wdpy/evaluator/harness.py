from __future__ import annotations
import logging
from typing import List, Optional

from wdpy import Item
from .rule import Rule, Violation
from .rules.single_best_rank import SingleBestRankRule
from .rules.ref_snak_redundancy import RefSnakRedundancyRule
from .rules.rank_qualifier import RankQualifierRule


_DEFAULT_RULES: List[Rule] = [
    SingleBestRankRule(),
    RefSnakRedundancyRule(),
    RankQualifierRule(),
]


class Evaluator:
    """Run invariant rules against a Wikidata item after sync+write.

    Two usage patterns::

        # Owns the full pipeline:
        violations = Evaluator().evaluate('Q123456')

        # You own sync+write, evaluator just checks:
        item = Item('Q123456')
        if synced := item.sync():
            qid = synced.write()
            violations = Evaluator().check(item, qid)
    """

    def __init__(self, rules: Optional[List[Rule]] = None) -> None:
        self.rules: List[Rule] = rules if rules is not None else list(_DEFAULT_RULES)

    def check(self, item: Item, written_qid: Optional[str]) -> List[Violation]:
        """Re-fetch written_qid from Wikidata and run all rules.

        item._loaded_claims provides the pre-sync state.
        Returns an empty list if written_qid is None.
        """
        if not written_qid:
            return []
        post_write = Item(written_qid)
        post_write._ensure_loaded()
        violations: List[Violation] = []
        for rule in self.rules:
            try:
                violations.extend(rule.check(post_write, item._loaded_claims))
            except Exception:
                logging.exception('Rule %s raised for %s', type(rule).__name__, written_qid)
        for v in violations:
            v.qid = written_qid
        return violations

    def evaluate(self, qid: str) -> List[Violation]:
        """Sync-write the item then run check(). Convenience for automated sweeps."""
        item = Item(qid)
        synced = item.sync()
        if synced is None:
            logging.warning('sync() returned None for %s — skipping evaluation', qid)
            return []
        return self.check(item, synced.write())
