"""Run the sync evaluator against one or more Wikidata items.

Usage:
    python -m wdpy.evaluator Q68126267 [Q<id> ...]
"""
from __future__ import annotations
import logging
import sys

from .harness import Evaluator


def main() -> int:
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
    qids = [a for a in sys.argv[1:] if a.startswith('Q')]
    if not qids:
        print('Usage: python -m wdpy.evaluator Q<id> [Q<id> ...]', file=sys.stderr)
        return 2

    evaluator = Evaluator()
    total_violations = 0
    for qid in qids:
        print(f'Evaluating {qid}...')
        violations = evaluator.evaluate(qid)
        if violations:
            for v in violations:
                print(f'  VIOLATION: {v}')
            total_violations += len(violations)
        else:
            print('  OK')

    if total_violations:
        print(f'\n{total_violations} violation(s) found across {len(qids)} item(s).')
        return 1
    print(f'\nAll {len(qids)} item(s) passed.')
    return 0
