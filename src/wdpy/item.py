from __future__ import annotations
import json
import logging
import uuid
from typing import Any, Dict, List, Optional, Tuple

from wdpy import Snak, Statement, SourceItem, get_entities, api_write


class Item:
    def __init__(self, qid: Optional[str] = None) -> None:
        self.qid = qid
        self.labels: Dict[str, str] = {}
        self.claims: Dict[str, List[Statement]] = {}
        self._loaded = False
        self._removals: List[Tuple[str, str]] = []

    def sync(self) -> Optional[Item]:
        """Fetch data from all applicable sources and transform them into this item.

        Iterates external-id claims to discover applicable source models,
        transforming each in turn.  If a QID is discovered mid-sync for a
        previously-unknown item, the item is reloaded and all prior transforms
        are re-applied from scratch.

        Returns self when a P31 claim is present after all transforms, else None.
        """
        transformed: List[SourceItem] = []

        while model := self._extract_new(transformed):
            if self.qid is None:
                if not self.labels and model.proposed_label:
                    self.labels['mul'] = model.proposed_label
                for stmt in (model.patch or []):
                    if Snak.type_of(stmt.mainsnak.property) == 'external-id' and stmt.mainsnak.value:
                        if qid := SourceItem.lookup(stmt.mainsnak.property, stmt.mainsnak.value[0]):
                            self.qid, self._loaded = qid, False
                            self.claims, self.labels = {}, {}
                            to_reapply, transformed = transformed, []
                            for m in to_reapply:
                                self.transform(m)
                            break
            self.transform(model)
            transformed.append(model)

        return self if self.claims.get('P31') else None

    def _extract_new(self, transformed: List[SourceItem]) -> Optional[SourceItem]:
        """Return the next untransformed source model, or None when exhausted."""
        self._ensure_loaded()
        deprecated = {
            (prop, stmt.mainsnak.value[0])
            for prop, stmts in self.claims.items()
            if Snak.type_of(prop) == 'external-id'
            for stmt in stmts
            if stmt.rank == 'deprecated' and stmt.mainsnak.value
        }
        for prop, stmts in self.claims.items():
            if Snak.type_of(prop) != 'external-id':
                continue
            for stmt in stmts:
                if stmt.rank == 'deprecated' or not stmt.mainsnak.value:
                    continue
                for model_type in SourceItem.get_extractors():
                    if any(
                        type(m) is model_type
                        and any(s.mainsnak == stmt.mainsnak for s in (m.patch or []))
                        for m in transformed
                    ):
                        continue
                    model = model_type.extract(stmt)
                    if not model.patch:
                        continue
                    return model
        return None

    @classmethod
    def get_by_id(cls, property_id: str, external_id: Any) -> Optional[Item]:
        """Return Item with found qid or create/load with all updates"""
        if property_id and external_id:
            if _id := SourceItem.lookup(property_id, external_id):
                return cls(_id)
            new_item = cls()
            new_item.merge(Statement(Snak(property_id, (str(external_id),))))
            return new_item.sync()

    def _ensure_loaded(self) -> None:
        if self._loaded:
            return
        if not (self.qid and self.qid.strip()):
            self._loaded = True
            return
        data = get_entities([self.qid])
        entity = data.get(self.qid) if data else None
        if isinstance(entity, dict):
            self.qid = entity.get('id', self.qid)
            labels = entity.get('labels', {})
            if isinstance(labels, dict):
                self.labels = {
                    lang: item.get('value')
                    for lang, item in labels.items()
                    if isinstance(item, dict) and isinstance(item.get('value'), str)
                }
            claims: Dict[str, List[Statement]] = {}
            raw_claims = entity.get('claims', {})
            if isinstance(raw_claims, dict):
                for prop, rows in raw_claims.items():
                    bucket: List[Statement] = []
                    if isinstance(rows, list):
                        for row in rows:
                            if isinstance(row, dict):
                                bucket.append(Statement.parse(row))
                    claims[prop] = bucket
            self.claims = claims
        self._loaded = True

    def transform(self, model: SourceItem) -> None:
        if not isinstance(model, SourceItem):
            return
        if not model.patch:
            return
        self._ensure_loaded()
        ref_snaks = model.get_ref_snaks()

        affected: set = set()
        for stmt in model.patch:
            affected.add(self.merge(stmt).mainsnak.property)

        for prop in affected:
            to_delete = []
            for stmt in self.claims.get(prop) or []:
                if stmt.references and ref_snaks:
                    if stmt.references.compress(ref_snaks):
                        to_delete.append(stmt)
            for stmt in to_delete:
                self.delete_claim(stmt)

    def merge(self, statement: Statement) -> Statement:
        """Upsert a statement into this item's claims.

        If an existing statement with the same mainsnak value and compatible
        qualifiers is found, references are merged into it and it is returned.
        Otherwise the statement is appended as a new claim (with an id assigned
        when the item already has a qid) and returned.
        """
        self._ensure_loaded()
        bucket = self.claims.setdefault(statement.mainsnak.property, [])
        if (existing := statement.upsert(bucket)) is None:
            if self.qid:
                statement.id = f'{self.qid}${uuid.uuid4()}'
            bucket.append(statement)
            return statement
        return existing

    def delete_claim(self, stmt: Statement) -> None:
        """Remove a statement from this item; if it was server-saved, queue it for deletion on the next write()."""
        prop = stmt.mainsnak.property
        if prop in self.claims:
            self.claims[prop] = [s for s in self.claims[prop] if s is not stmt]
        if stmt.mainsnak.hash is not None and stmt.id is not None:
            self._removals.append((prop, stmt.id))

    def json(self) -> str:
        data: Dict[str, Any] = {}
        if self.qid:
            data['id'] = self.qid
        if self.labels:
            data['labels'] = {
                lang: {'language': lang, 'value': text}
                for lang, text in self.labels.items()
            }
        if self.claims or self._removals:
            claims: Dict[str, List[Any]] = {}
            for prop, statements in self.claims.items():
                items: List[Any] = []
                for stmt in statements or []:
                    try:
                        items.append(json.loads(stmt.json()))
                    except json.JSONDecodeError:
                        logging.error('Invalid statement JSON for %s', prop)
                claims[prop] = items
            for prop, stmt_id in self._removals:
                claims.setdefault(prop, []).append({'id': stmt_id, 'remove': ''})
            data['claims'] = claims
        return json.dumps(data, ensure_ascii=False)

    def postprocess(self) -> None:
        author_stmts = (self.claims.get('P50') or []) + (self.claims.get('P2093') or [])
        for stmt in Statement.deduplicate_authors(author_stmts, 'P1545'):
            self.delete_claim(stmt)

        for prop, stmts in self.claims.items():
            if Snak.type_of(prop) != 'external-id':
                continue
            if sum(1 for s in stmts if s.rank != 'deprecated') != 1:
                continue
            for all_stmts in self.claims.values():
                for stmt in all_stmts:
                    if stmt.references:
                        for ref in stmt.references._items:
                            ref[:] = [s for s in ref if s.property != prop]
                        stmt.references._items = [r for r in stmt.references._items if r]

    def write(self, summary: str) -> Optional[str]:
        self.postprocess()
        payload: Dict[str, Any] = {'data': self.json(), 'summary': summary}
        if self.qid:
            payload['id'] = self.qid
        else:
            payload['new'] = 'item'
        if (response := api_write('wbeditentity', **payload)):
            entity = response.get('entity')
            if isinstance(entity, dict):
                new_qid = entity.get('id')
                if isinstance(new_qid, str):
                    self.qid = new_qid
            self._removals.clear()
            return self.qid
        return None