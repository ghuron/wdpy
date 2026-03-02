from __future__ import annotations
import http.client
import json
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional
import urllib.error
from urllib.request import Request, build_opener


from wdpy import haswbstatement, Snak, Statement
from wdpy.core import build_request


def request(url: str,
            params: Optional[Dict[str, Any]] = None,
            headers: Optional[Dict[str, str]] = None,
            timeout: float = 120) -> Optional[http.client.HTTPResponse]:
    req = build_request(url, params, headers)
    try:
        return build_opener().open(req, timeout=timeout)
    except urllib.error.HTTPError as e:
        return e
    except Exception as e:
        logging.error('Request failed for %s: %s', url, e)
    return None

@dataclass
class SourceItem:
    patch: Optional[List[Statement]] = None

    def __init_subclass__(cls, **kwargs:Any):
        super().__init_subclass__(**kwargs)
        cls._config = getattr(cls, '_config', {}).copy()
        cls._config.update(cls._load_config(cls.__module__))
    
    @staticmethod
    def _load_config(module_name: str) -> Dict[str, Any]:
        config_path = Path(str(sys.modules[module_name].__file__)).with_suffix('.json')
        if config_path.exists():
            with open(config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}

    @classmethod
    def get_properties(cls) -> List[str]:
        return cls._config['properties'].keys()

    @classmethod
    def get_db_ref(cls) -> Optional[str]:
        return cls._config.get('source_item')

    @staticmethod
    def lookup (property_id:str, external_id:Any) -> Optional[str]:
        if result := haswbstatement(property_id, external_id):
            if len(result) > 1:  # Todo: warning ONLY if it was pre-loaded
                logging.warning(f'{len(result)} instances {property_id}="{external_id}", using {result[0]}')
            return result[0]
    
    @classmethod
    def make_request(cls, ident: Statement) -> Optional[Request]:
        if url := cls._config["properties"].get(ident.mainsnak.property):
            return build_request(url.format(ident.mainsnak.value[0]))

    @classmethod
    def update_ident(cls, ident: Statement, url: str) -> Statement:
        raise NotImplementedError('Subclasses must implement update_ident')

    def add_claim(self, property_id: str, *value: str) -> Statement:
        s = Statement(Snak(property_id, value or None))
        if self.patch is None:
            self.patch = []
        self.patch.append(s)
        return s

    def obtain(self, source: dict, properties: dict, translate: dict = {}) -> None:
        for key, prop in properties.items():
            if (val := source.get(key)) is None or not prop:
                continue
            if isinstance(val, list):
                for v in val:
                    self.add_claim(prop, translate.get(v, v))
            elif isinstance(prop, str):
                self.add_claim(prop, translate.get(val, val))
            elif isinstance(prop, dict):
                self.obtain(val, prop, translate)

    def add_author(self, full_name: str, orcid: Optional[str] = None) -> Statement:
        full_name = full_name.strip()
        if orcid and (qid := self.lookup('P496', orcid)):
            s = self.add_claim('P50', qid)
            s.set_qualifier('P1932', full_name)
        else:
            s = self.add_claim('P2093', full_name)
        self._author_num = getattr(self, '_author_num', 0) + 1
        s.set_qualifier('P1545', str(self._author_num))
        return s

    def parse(self, text: str) -> None:
        raise NotImplementedError('Subclasses must implement parse')

    @classmethod
    def extract(cls, ident: Statement) -> Optional[SourceItem]:
        if not (req := cls.make_request(ident)):
            return None
        url = req.full_url
        if not (resp := request(url, timeout=30)):
            return None
        handled = cls._config.get('extract', [])
        with resp:
            if 404 in handled and resp.getcode() == 404:
                return cls(patch=[])
            destination = resp.geturl()
            text = resp.read().decode('utf-8')
        destination_url = destination or url
        redirected = 301 in handled and destination_url.lower() != url.lower()
        if redirected:
            patch_ident = cls.update_ident(ident, destination_url)
        else:
            patch_ident = ident
        item = cls(patch=[patch_ident])
        item.parse(text)
        return item
