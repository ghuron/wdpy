from __future__ import annotations
import http.client
import importlib
import json
import logging
import pkgutil
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, ClassVar, Dict, List, Optional
import urllib.error
from urllib.request import Request, build_opener


_CLASS_HANDLER_CACHE: Dict[type, Dict[str, str]] = {}


def obtain_handler(key: str) -> Callable:
    """Decorator: register this method to intercept `key` during obtain() traversal."""
    def decorator(fn: Callable) -> Callable:
        fn._obtain_key = key
        return fn
    return decorator


def _get_class_handlers(cls: type) -> Dict[str, str]:
    if cls not in _CLASS_HANDLER_CACHE:
        handlers: Dict[str, str] = {}
        for name in dir(cls):
            m = getattr(cls, name, None)
            if callable(m) and (k := getattr(m, '_obtain_key', None)):
                handlers[k] = name
        _CLASS_HANDLER_CACHE[cls] = handlers
    return _CLASS_HANDLER_CACHE[cls]


from wdpy import haswbstatement, References, Snak, Statement
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
    proposed_label: Optional[str] = None
    _lookup_cache: ClassVar[Dict[str, Dict[Any, Optional[str]]]] = {}
    _registry: ClassVar[List[type]] = []
    _connectors_loaded: ClassVar[bool] = False

    def __init_subclass__(cls, **kwargs: Any):
        super().__init_subclass__(**kwargs)
        cls._config = getattr(cls, '_config', {}).copy()
        cls._config.update(cls._load_config(cls.__module__))
        if cls._config.get('source'):
            SourceItem._registry.append(cls)

    @classmethod
    def get_extractors(cls) -> List[type]:
        """Return all SourceItem subclasses found in wdpy.connectors."""
        if not cls._connectors_loaded:
            cls._connectors_loaded = True
            import wdpy.connectors
            for _, name, _ in pkgutil.iter_modules(wdpy.connectors.__path__):
                try:
                    importlib.import_module(f'wdpy.connectors.{name}')
                except Exception as e:
                    logging.warning('Failed to load connector %s: %s', name, e)
        return cls._registry
    
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
        return cls._config.get('source')

    @classmethod
    def get_primary_property(cls) -> Optional[str]:
        return next(iter(cls._config.get('properties', {})), None)

    @staticmethod
    def lookup(property_id: str, external_id: Any) -> Optional[str]:
        by_prop = SourceItem._lookup_cache.setdefault(property_id, {})
        if external_id not in by_prop:
            by_prop[external_id] = None
            if result := haswbstatement(property_id, external_id):
                if len(result) > 1:  # Todo: warning ONLY if it was pre-loaded
                    logging.warning(f'{len(result)} instances {property_id}="{external_id}", using {result[0]}')
                by_prop[external_id] = result[0]
        return by_prop[external_id]
    
    @classmethod
    def make_request(cls, ident: Statement) -> Optional[Request]:
        if url := cls._config["properties"].get(ident.mainsnak.property):
            return build_request(url.format(ident.mainsnak.value[0]))

    @classmethod
    def update_ident(cls, ident: Statement, url: str) -> Statement:
        raise NotImplementedError('Subclasses must implement update_ident')

    def add_claim(self, property_id: str, *value: str) -> Optional[Statement]:
        normalized = value[0] if len(value) == 1 else (value or None)
        if not (snak := Snak.create(property_id, normalized)):
            return None
        s = Statement(snak)
        if self.patch is None:
            self.patch = []
        self.patch.append(s)
        if property_id == 'P1476' and not self.proposed_label and value:
            self.proposed_label = value[0]
        return s

    def deprecate_ident(self, ident: Statement, reason: str = 'Q21441764') -> None:
        """Append ident as a deprecated statement to patch."""
        s = Statement(ident.mainsnak, ident.id, 'deprecated', ident.qualifiers)
        s.set_qualifier('P2241', reason)
        if self.patch is None:
            self.patch = []
        self.patch.append(s)

    def obtain(self, source: dict, properties: dict, translate: Optional[dict] = None) -> None:
        if translate is None:
            translate = self._config.get('translate', {})
        handlers = _get_class_handlers(type(self))
        for key, method_name in handlers.items():
            if (val := source.get(key)) is not None:
                getattr(self, method_name)(val)
        for key, prop in properties.items():
            if key in handlers or (val := source.get(key)) is None or not prop:
                continue
            if isinstance(val, list):
                for v in val:
                    self.add_claim(prop, translate.get(v, v))
            elif isinstance(prop, str):
                self.add_claim(prop, translate.get(val, val))
            elif isinstance(prop, dict):
                self.obtain(val, prop, translate)

    def add_author(self, full_name: str, orcid: Optional[str] = None) -> Optional[Statement]:
        full_name = full_name.strip()
        qid = self.lookup('P496', orcid) if orcid else None
        if orcid and qid is None:
            from wdpy.item import Item
            if person := Item.get_by_id('P496', orcid):
                person.labels.setdefault('mul', full_name)
                if new_qid := person.write():
                    SourceItem._lookup_cache.setdefault('P496', {})[orcid] = new_qid
                    qid = new_qid
        if qid:
            s = self.add_claim('P50', qid)
            if s:
                s.set_qualifier('P1932', full_name)
        else:
            s = self.add_claim('P2093', full_name)
        if s:
            self._author_num = getattr(self, '_author_num', 0) + 1
            s.set_qualifier('P1545', str(self._author_num))
        return s

    @staticmethod
    def register_new_item(statements: List[Statement]) -> None:
        for s in statements:
            if Snak.type_of(s.mainsnak.property) != 'external-id':
                continue
            if not s.id or not s.mainsnak.value:
                continue
            property_id = s.mainsnak.property
            value = s.mainsnak.value[0]
            qid = s.id.split('$')[0]
            if property_id not in SourceItem._lookup_cache:
                logging.error('No lookup performed for %s', property_id)
                SourceItem._lookup_cache[property_id] = {}
            elif value not in SourceItem._lookup_cache[property_id]:
                logging.error('No lookup performed for %s:%s', property_id, value)
            elif SourceItem._lookup_cache[property_id][value] is not None:
                logging.error('Duplicate discovered for %s:%s %s+%s',
                              property_id, value, qid, SourceItem._lookup_cache[property_id][value])
            SourceItem._lookup_cache[property_id][value] = qid

    def parse(self, text: str, ident: Statement) -> None:
        raise NotImplementedError('Subclasses must implement parse')

    @classmethod
    def extract(cls, ident: Statement) -> SourceItem:
        """Fetch and parse the record for `ident`. Always returns a SourceItem.

        patch None: unrecognised property (make_request returned None).

        patch with only the non-deprecated ident: network error — the ident
        is preserved as-is so it is not inadvertently removed from the item.

        patch with a deprecated statement: ident confirmed gone or redirected;
        patch may also carry additional claims.

        Only normal statements in patch: ident unchanged; patch carries new
        claims to merge.
        """
        if not (req := cls.make_request(ident)):
            return cls()
        url = req.full_url
        try:
            resp = build_opener().open(req, timeout=120)
        except Exception as e:
            logging.error('Request failed for %s: %s', url, e)
            return cls(patch = [ident])
        handled = cls._config.get('extract', [])
        primary_prop = cls.get_primary_property()
        with resp:
            if ((code := resp.getcode()) == 404) and (404 in handled):
                if ident.mainsnak.property == primary_prop:
                    item = cls()
                    item.deprecate_ident(ident)
                    return item
                return cls()
            destination = resp.geturl()
            text = resp.read().decode('utf-8')
            if code != 200:
                logging.error(f'Returned code {code} for {url}, message: {text}')
        destination_url = destination or url
        redirected = 301 in handled and destination_url.lower() != url.lower()
        item = cls()
        if redirected:
            new_ident = cls.update_ident(ident, destination_url)
            item.patch = [new_ident]
            item.deprecate_ident(ident)
        item.parse(text, ident)

        item._apply_references()
        return item

    def get_ref_snaks(self) -> Dict[str, List[Snak]]:
        """Return the reference snaks for this source item without modifying anything.

        Always includes P248 (stated in <db_ref>). Also includes the primary
        identifier snak when a non-deprecated statement with that property exists
        in self.patch, so callers can trace the exact external ID that was synced.
        """
        db_ref = self.get_db_ref()
        if not db_ref:
            return {}
        result: Dict[str, List[Snak]] = {}
        if p248 := Snak.create('P248', db_ref):
            result['P248'] = [p248]
        primary_prop = self.get_primary_property()
        if primary_prop and self.patch:
            if ident := next(
                (s for s in self.patch
                 if s.rank != 'deprecated' and s.mainsnak.property == primary_prop),
                None
            ):
                result[ident.mainsnak.property] = [ident.mainsnak]
        return result

    def _apply_references(self) -> None:
        """Attach a source reference to every statement in self.patch."""
        if not self.patch:
            return
        if ref_snaks := self.get_ref_snaks():
            for s in self.patch:
                r = References()
                r.upsert(ref_snaks)
                s.references = r
