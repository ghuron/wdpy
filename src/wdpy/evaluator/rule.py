from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Optional, Protocol, runtime_checkable

from wdpy import Item, Statement


@dataclass
class Violation:
    rule: str
    property_id: str
    statement_id: Optional[str]
    detail: str

    def __str__(self) -> str:
        stmt = f' [{self.statement_id}]' if self.statement_id else ''
        return f'[{self.rule}] {self.property_id}{stmt}: {self.detail}'


@runtime_checkable
class Rule(Protocol):
    def check(
        self,
        post_write_item: Item,
        pre_sync_claims: Dict[str, List[Statement]],
    ) -> List[Violation]:
        ...
