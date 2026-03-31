from __future__ import annotations
from typing import Dict, List

from wdpy import Item, Statement
from ..rule import Violation

_AUTHOR_PROPS = {'P50', 'P2093'}
_ORDINAL_PROP = 'P1545'


class SingleBestRankRule:
    """Every property must have exactly one 'best'-ranked statement.

    Exception: P50 and P2093 (authors) must have exactly one non-deprecated
    statement per P1545 qualifier value, combined across both properties.

    'Best' rank means: if any non-deprecated statement has preferred rank,
    exactly one must have preferred.  If none have preferred, exactly one
    non-deprecated statement must exist total.
    """

    def check(
        self,
        post_write_item: Item,
        pre_sync_claims: Dict[str, List[Statement]],
    ) -> List[Violation]:
        violations: List[Violation] = []
        claims = post_write_item.claims

        author_stmts = claims.get('P50', []) + claims.get('P2093', [])
        if author_stmts:
            violations.extend(_check_authors(author_stmts))

        for prop, stmts in claims.items():
            if prop in _AUTHOR_PROPS:
                continue
            active = [s for s in stmts if s.rank != 'deprecated']
            if len(active) <= 1:
                continue
            preferred = [s for s in active if s.rank == 'preferred']
            if len(preferred) == 0:
                violations.append(Violation(
                    rule='single_best_rank',
                    property_id=prop,
                    statement_id=None,
                    detail=(
                        f'{len(active)} non-deprecated statements with no preferred rank; '
                        f'expected exactly one best'
                    ),
                ))
            elif len(preferred) > 1:
                for s in preferred:
                    violations.append(Violation(
                        rule='single_best_rank',
                        property_id=prop,
                        statement_id=s.id,
                        detail=(
                            f'Multiple preferred-rank statements ({len(preferred)} total); '
                            f'expected exactly one best'
                        ),
                    ))
        return violations


def _check_authors(statements: List[Statement]) -> List[Violation]:
    groups: Dict[str, List[Statement]] = {}

    for s in statements:
        ordinal = next(
            (q.value[0] for q in (s.qualifiers or [])
             if q.property == _ORDINAL_PROP and q.value),
            None,
        )
        if ordinal is not None:
            groups.setdefault(ordinal, []).append(s)

    violations: List[Violation] = []
    for ordinal, group in groups.items():
        active = [s for s in group if s.rank != 'deprecated']
        if len(active) > 1:
            for s in active[1:]:
                violations.append(Violation(
                    rule='single_best_rank',
                    property_id=s.mainsnak.property,
                    statement_id=s.id,
                    detail=f'Multiple author statements for P1545={ordinal!r}; expected exactly one',
                ))
    return violations
