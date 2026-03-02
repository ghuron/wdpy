from __future__ import annotations
import json
from typing import Any, Dict, Iterable, List, Optional

from wdpy import Snak, get_entities

_REDIRECTS: Dict[str, str] = {}
_PUB_DATES: Dict[str, int] = {}


class References:
    _items: List[List[Snak]]
    _confirmed: List[bool]

    def __init__(self, wikibase_refs: Optional[Iterable[Any]] = None):
        self._items, self._confirmed = [], []
        for ref in (wikibase_refs or []):
            payload = ref.get('snaks', {}) if isinstance(ref, dict) else {}
            self._items.append([s if isinstance(s, Snak) else Snak.parse(s)
                                for val in payload.values() for s in (val or [])])
            self._confirmed.append(False)

    def __bool__(self) -> bool:
        return bool(self._items)

    def json(self) -> str:
        res = []
        for snaks in self._items:
            payload = {}
            for s in snaks:
                payload.setdefault(s.property, []).append(json.loads(s.json()))
            res.append({'snaks': payload})
        return json.dumps(res, sort_keys=True)

    def include(self, snaks_dict: Dict[str, List[Snak]]) -> None:
        new_item = [s for val in snaks_dict.values() for s in (val or [])]
        items = [new_item] + self._items
        _preload(items)
        Snak.resolve_redirect(items, _REDIRECTS)
        for i, item in enumerate(self._items):
            if Snak.try_to_merge_references(new_item, item):
                self._confirmed[i] = True
                return
        self._items.append(new_item)
        self._confirmed.append(True)

    def compress(self, qid: str) -> None:
        target = (qid or '').strip()
        if not target: return
        _preload(self._items)
        Snak.resolve_redirect(self._items, _REDIRECTS)

        def has(item: List[Snak], prop: str) -> bool:
            return any(s.property == prop and s.value and s.value[0] == target
                       for s in item)

        confirmed_found = any(c and has(item, 'P12132')
                              for item, c in zip(self._items, self._confirmed))
        new_items, new_confirmed = [], []
        for item, confirmed in zip(self._items, self._confirmed):
            if confirmed or not (has(item, 'P248') or has(item, 'P12132')):
                if confirmed_found and has(item, 'P248'):
                    item = [s for s in item if not (s.property == 'P248' and
                            s.value and s.value[0] == target)]
                if item:
                    new_items.append(item)
                    new_confirmed.append(confirmed)
        self._items, self._confirmed = new_items, new_confirmed

    @property
    def publication_date(self) -> Optional[str]:
        if not self._items: return None
        _preload(self._items)
        dates = [
            _PUB_DATES.get(s.value[0]) for item in self._items for s in item
            if s.property == 'P248' and s.value and _PUB_DATES.get(s.value[0])
        ]
        return f"{max(dates):08d}" if dates else None


def _preload(items: Iterable[List[Snak]]) -> None:
    if not items: return
    cached = set(_REDIRECTS) | set(_PUB_DATES)
    if missing := Snak.get_p248_qids(items) - cached:
        if entities := get_entities(missing):
            _update_redirect(entities)
            _update_pub_date(entities)


def _update_redirect(entities: Dict[str, Dict[str, Any]]) -> None:
    for key, raw in (entities or {}).items():
        data = raw if isinstance(raw, dict) else {}
        canonical = data.get('id') or key
        if canonical:
            _REDIRECTS.setdefault(canonical, canonical)
            _REDIRECTS.setdefault(key, canonical)
        redirects = data.get('redirects') if isinstance(data, dict) else None
        if isinstance(redirects, dict):
            source = redirects.get('from')
            target = redirects.get('to') or canonical
            if source and target:
                _REDIRECTS[source] = target
                _REDIRECTS.setdefault(target, target)


def _update_pub_date(entities: Dict[str, Dict[str, Any]]) -> None:
    def get_stamp(rows: Any) -> Optional[int]:
        for r in (rows or []):
            v = r.get('mainsnak', {}).get('datavalue', {}).get('value', {}).get('time')
            if isinstance(v, str) and len(v) >= 11:
                try:
                    return int(v[1:5] + v[6:8] + v[9:11])
                except ValueError:
                    pass
        return None

    for key, raw in (entities or {}).items():
        data = raw if isinstance(raw, dict) else {}
        if (stamp := get_stamp(data.get('claims', {}).get('P577'))) is None:
            continue
        target = data.get('id') or key
        _PUB_DATES.update({target: stamp, key: stamp})
        if isinstance(redir := data.get('redirects'), dict) and (src := redir.get('from')):
            _PUB_DATES[src] = stamp
