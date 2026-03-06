from __future__ import annotations
from typing import Optional
from wdpy.connectors import AstroItem
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

