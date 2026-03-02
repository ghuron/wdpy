from __future__ import annotations
from typing import Optional
from wdpy.connectors import AstroItem
from wdpy import Snak, Statement
import urllib.parse


class ExoplanetItem(AstroItem):    
    @staticmethod
    def _extract_catalog_id(url: Optional[str]) -> Optional[str]:
        if not isinstance(url, str):
            return None
        parts = [
            part for part in urllib.parse.urlparse(url).path.split('/') if part
        ]
        if len(parts) < 2 or parts[-2] != 'catalog':
            return None
        return parts[-1]

    @classmethod
    def update_ident(cls, ident: Statement, url: str) -> Statement:
        target = cls._extract_catalog_id(url)
        if not target:
            raise ValueError(f'Cannot extract exoplanet id from {url}')
        snak = ident.mainsnak if isinstance(ident, Statement) else None
        if not isinstance(snak, Snak):
            raise TypeError('Invalid statement mainsnak for redirect update')
        return Statement(
            Snak(snak.property, (target,), snak.snaktype),
            id=ident.id,
            rank=ident.rank,
            qualifiers=ident.qualifiers,
            references=ident.references,
        )

