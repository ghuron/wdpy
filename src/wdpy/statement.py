from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional
from wdpy import Snak, References, api_write

@dataclass
class Statement:
    """Wrapper around a Wikibase statement."""

    mainsnak: Snak
    id: Optional[str] = None
    rank: Optional[Literal['normal', 'preferred', 'deprecated']] = None
    qualifiers: Optional[List[Snak]] = None
    references: Optional[References] = None

    @classmethod
    def parse(cls, wikibase_statement: Dict[str, Any]) -> Statement:
        """Parse Wikibase payload into a Statement."""

        raw_quals = wikibase_statement.get('qualifiers') or {}
        qualifiers = [
            Snak.parse(s) for snaks in raw_quals.values() for s in (snaks or [])
        ]
        return cls(
            mainsnak=Snak.parse(wikibase_statement.get("mainsnak", {})),
            id=wikibase_statement.get("id"),
            rank=wikibase_statement.get("rank") or "normal",
            qualifiers=qualifiers or None,
            references=References(wikibase_statement.get('references')) or None,
        )

    def set_qualifier(self, property_id: str, value: Any) -> None:
        items = value if isinstance(value, list) else [value]
        snaks = [
            item if isinstance(item, Snak) else Snak.create(property_id, item)
            for item in items
        ]
        self.qualifiers = (self.qualifiers or []) + [s for s in snaks if s]

    def set_rank(self, rank: Literal['normal', 'preferred', 'deprecated'],
                 reason: Optional[str] = None) -> None:
        self.rank = rank
        if not reason:
            return
        if rank == 'preferred':
            self.set_qualifier('P7452', reason)
        elif rank == 'deprecated':
            self.set_qualifier('P2241', reason)

    def json(self) -> str:
        """Generate Wikibase JSON by concatenating Snak JSON payloads."""

        parts = [f'"mainsnak":{self.mainsnak.json()}', '"type":"statement"']
        if self.id:
            parts.append(f'"id":"{self.id}"')
        if self.rank:
            parts.append(f'"rank":"{self.rank}"')
        if self.qualifiers:
            groups: Dict[str, List[str]] = {}
            for s in self.qualifiers:
                groups.setdefault(s.property, []).append(s.json())
            quals = ','.join(f'"{p}":[{",".join(v)}]' for p, v in groups.items())
            if quals:
                parts.append(f'"qualifiers":{{{quals}}}')
        if self.references:
            parts.append(f'"references":{self.references.json()}')
        return '{' + ','.join(parts) + '}'

    def _qualifiers_present_in(self, other: Statement) -> bool:
        """Return True if every qualifier of self is present in other."""
        if not self.qualifiers:
            return True
        if not other.qualifiers:
            return False
        other_by_prop: Dict[str, List[Snak]] = {}
        for s in other.qualifiers:
            other_by_prop.setdefault(s.property, []).append(s)
        return all(
            any(c.value == s.value for c in other_by_prop.get(s.property, []))
            for s in self.qualifiers
        )

    def _merge_references_into(self, target: Statement) -> None:
        """Merge self's references into target's reference set."""
        if not self.references:
            return
        if target.references is None:
            target.references = References()
        for ref_snaks in self.references._items:
            by_prop: Dict[str, List[Snak]] = {}
            for s in ref_snaks:
                by_prop.setdefault(s.property, []).append(s)
            target.references.include(by_prop)

    def upsert(self, existing: List[Statement]) -> Optional[Statement]:
        """Find a matching statement in existing and merge references into it.

        A match requires equal mainsnak values (or equal snaktype for novalue/somevalue)
        and all of self's qualifiers being present in the candidate.

        Returns the matched statement from existing (update), or None (insert).
        """
        for candidate in existing:
            if self.mainsnak.snaktype != 'value':
                if candidate.mainsnak.snaktype != self.mainsnak.snaktype:
                    continue
            elif (candidate.mainsnak.snaktype != 'value' or
                  candidate.mainsnak.value != self.mainsnak.value):
                continue
            if self._qualifiers_present_in(candidate):
                self._merge_references_into(candidate)
                if self.rank is not None:
                    candidate.rank = self.rank
                return candidate
        return None

    @staticmethod
    def deduplicate_authors(statements: List[Statement],
                            ordinal_property: str) -> List[Statement]:
        """Deduplicate author statements sharing the same ordinal qualifier.

        Groups statements by the value of ordinal_property (e.g. P1545).
        Statements without that qualifier are ignored.  For each ordinal that
        has more than one author statement the inferior one is selected for
        deletion using these rules (applied in order):

        - P50 (linked item) always beats P2093 (string name).
        - Two P50 statements linking to *different* items are both kept.
        - Among same-type duplicates the statement with the longer display
          name wins; for P50 the display name is the P1932 qualifier value.

        Returns the list of redundant statements that should be deleted.
        """
        def _display_name(s: Statement) -> str:
            if s.mainsnak.property == 'P2093':
                return s.mainsnak.value[0] if s.mainsnak.value else ''
            for q in (s.qualifiers or []):
                if q.property == 'P1932' and q.value:
                    return q.value[0]
            return ''

        to_delete: List[Statement] = []
        best: Dict[str, Statement] = {}

        for s in statements:
            ordinal = next(
                (q.value[0] for q in (s.qualifiers or [])
                 if q.property == ordinal_property and q.value),
                None,
            )
            if ordinal is None:
                continue

            if ordinal not in best:
                best[ordinal] = s
                continue

            incumbent = best[ordinal]

            if s.mainsnak.property == 'P50':
                if incumbent.mainsnak.property == 'P2093':
                    best[ordinal], s = s, incumbent          # P50 beats P2093
                elif (s.mainsnak.value and incumbent.mainsnak.value
                      and s.mainsnak.value[0] != incumbent.mainsnak.value[0]):
                    continue                                  # different items, keep both
                elif len(_display_name(s)) > len(_display_name(incumbent)):
                    best[ordinal], s = s, incumbent          # longer name wins
            elif incumbent.mainsnak.property == 'P2093':    # both P2093
                if len(_display_name(s)) > len(_display_name(incumbent)):
                    best[ordinal], s = s, incumbent

            to_delete.append(s)

        return to_delete

    def write(self, summary: str) -> Optional[str]:
        """Persist statement via wbsetclaim."""
        response = api_write('wbsetclaim', claim=self.json(), summary=summary)
        if response and isinstance(response.get('claim'), dict):
            return response['claim'].get('id')
        return None

    @staticmethod
    def select_outdated(statements: List[Statement],
                        group_by: Optional[str] = None) -> List[Statement]:
        """Return statements to delete, keeping only the most recently sourced per group.

        Statements are grouped by the value of the group_by qualifier (or treated as
        one group when group_by is None).  Within each group the keeper is the
        novalue/somevalue statement if one exists, otherwise the statement with the
        highest reference publication date.  Statements lacking the group_by qualifier
        are always returned for deletion.
        """
        _MISSING = object()

        def _group(s: Statement):
            if not group_by:
                return None
            for q in (s.qualifiers or []):
                if q.property == group_by and q.value:
                    return q.value[0]
            return _MISSING

        # First pass: find the best ref date per group (None = novalue/somevalue wins).
        best: Dict[Any, Optional[str]] = {}
        for s in statements:
            if (g := _group(s)) is _MISSING:
                continue
            if s.mainsnak.snaktype != 'value':
                best[g] = None
            elif g not in best:
                best[g] = (s.references and s.references.publication_date) or '00000000'
            elif best[g] is not None:
                d = (s.references and s.references.publication_date) or '00000000'
                if d > best[g]:
                    best[g] = d

        # Second pass: keep exactly one per group, queue the rest for deletion.
        kept: set = set()
        to_delete: List[Statement] = []
        for s in statements:
            if (g := _group(s)) is _MISSING:
                to_delete.append(s)
                continue
            if best.get(g) is None:          # novalue group
                if s.mainsnak.snaktype == 'value' or g in kept:
                    to_delete.append(s)
                else:
                    kept.add(g)
            elif g in kept or ((s.references and s.references.publication_date) or '00000000') < best[g]:
                to_delete.append(s)
            else:
                kept.add(g)
        return to_delete

    @staticmethod
    def rank_by_recency(statements: List[Statement]) -> None:
        """Assign deprecated rank to all but the most recently sourced statement.

        Statements carrying a P2241 (reason for deprecation) qualifier are left
        unchanged.  If any statement already has preferred rank, nothing is modified.
        The one statement with the highest reference publication date keeps normal
        rank; all others receive deprecated rank.
        """
        for s in statements:
            if s.rank == 'preferred':
                return

        best_date = '00000000'
        for s in statements:
            if not any(q.property == 'P2241' for q in (s.qualifiers or [])):
                if (d := (s.references and s.references.publication_date) or '00000000') > best_date:
                    best_date = d

        remaining = 1
        for s in statements:
            if not any(q.property == 'P2241' for q in (s.qualifiers or [])):
                if remaining > 0 and ((s.references and s.references.publication_date) or '00000000') == best_date:
                    s.rank = 'normal'
                    remaining -= 1
                else:
                    s.rank = 'deprecated'