from __future__ import annotations
from typing import Dict, List

from wdpy import Item, Statement
from ..rule import Violation


class RankQualifierRule:
    """Non-normal rank statements must carry an explanatory qualifier.

    preferred rank → P7452 (reason for preferred rank) must be present.
    deprecated rank → P2241 (reason for deprecation) must be present.
    """

    def check(
        self,
        post_write_item: Item,
        pre_sync_claims: Dict[str, List[Statement]],
    ) -> List[Violation]:
        violations: List[Violation] = []
        for prop, stmts in post_write_item.claims.items():
            for stmt in stmts:
                if stmt.rank == 'preferred':
                    if not any(q.property == 'P7452' for q in (stmt.qualifiers or [])):
                        violations.append(Violation(
                            rule='rank_qualifier',
                            property_id=prop,
                            statement_id=stmt.id,
                            detail='Preferred-rank statement is missing P7452 (reason for preferred rank)',
                        ))
                elif stmt.rank == 'deprecated':
                    if not any(q.property == 'P2241' for q in (stmt.qualifiers or [])):
                        violations.append(Violation(
                            rule='rank_qualifier',
                            property_id=prop,
                            statement_id=stmt.id,
                            detail='Deprecated-rank statement is missing P2241 (reason for deprecation)',
                        ))
        return violations
