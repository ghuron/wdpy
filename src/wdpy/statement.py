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

    def save(self, summary: str) -> Optional[Dict[str, Any]]:
        """Persist statement via wbsetclaim."""
        return api_write('wbsetclaim', claim=self.json(), summary=summary)