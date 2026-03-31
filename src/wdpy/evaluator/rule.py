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
    qid: Optional[str] = None

    @property
    def url(self) -> Optional[str]:
        if not self.qid:
            return None
        if self.statement_id:
            return f'https://www.wikidata.org/wiki/{self.qid}#{self.statement_id}'
        return f'https://www.wikidata.org/wiki/{self.qid}#{self.property_id}'

    def __str__(self) -> str:
        link = f' {self.url}' if self.url else ''
        return f'[{self.rule}]{link}: {self.detail}'


@runtime_checkable
class Rule(Protocol):
    def check(
        self,
        post_write_item: Item,
        pre_sync_claims: Dict[str, List[Statement]],
    ) -> List[Violation]:
        ...
