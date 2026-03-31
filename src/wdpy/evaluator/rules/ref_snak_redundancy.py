from __future__ import annotations
from typing import Dict, List

from wdpy import Item, Snak, Statement
from ..rule import Violation


class RefSnakRedundancyRule:
    """References must not include a redundant external ID snak.

    When a reference contains a P248 (stated in) snak, any additional snak
    for external ID property Px is redundant if the item already has exactly
    one non-deprecated statement for Px — that statement unambiguously
    identifies the source, so citing it again in the reference adds no
    information.  Such snaks should have been stripped by _strip_ref_snaks
    during write().
    """

    def check(
        self,
        post_write_item: Item,
        pre_sync_claims: Dict[str, List[Statement]],
    ) -> List[Violation]:
        # Count non-deprecated statements per external-id property on the item.
        ext_id_counts: Dict[str, int] = {
            prop: sum(1 for s in stmts if s.rank != 'deprecated')
            for prop, stmts in post_write_item.claims.items()
            if Snak.type_of(prop) == 'external-id'
        }

        violations: List[Violation] = []
        for prop, stmts in post_write_item.claims.items():
            for stmt in stmts:
                if not stmt.references:
                    continue
                for ref_group in stmt.references._items:
                    if not any(s.property == 'P248' for s in ref_group):
                        continue
                    for snak in ref_group:
                        if snak.property == 'P248':
                            continue
                        if ext_id_counts.get(snak.property) == 1:
                            violations.append(Violation(
                                rule='ref_snak_redundancy',
                                property_id=prop,
                                statement_id=stmt.id,
                                detail=(
                                    f'Reference contains {snak.property} snak but item has '
                                    f'exactly one non-deprecated {snak.property} statement; '
                                    f'the reference snak is redundant'
                                ),
                            ))
        return violations
