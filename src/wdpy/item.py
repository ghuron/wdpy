from __future__ import annotations
import json
import logging
import uuid
from typing import Any, Dict, List, Optional

from wdpy import Statement, SourceItem, get_entities, api_write


class Item:
    def __init__(self, qid: Optional[str] = None) -> None:
        self.qid = qid
        self.labels: Dict[str, str] = {}
        self.claims: Dict[str, List[Statement]] = {}
        self._loaded = False

    @classmethod
    def get_by_id(cls, property_id: str, external_id: Any) -> Optional[Item]:
        """Return Item with found qid or create/load with all updates"""
        if property_id and external_id:
            if _id := SourceItem.lookup(property_id, external_id):
                return cls(_id)

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
        if not isinstance(model, SourceItem) or model.patch is None:
            return
        self._ensure_loaded()
        source = model.get_db_ref()

        def compress(statement: Statement) -> None:
            if not isinstance(statement, Statement) or not source:
                return
            if statement.references:
                statement.references.compress(source)
                if not statement.references:
                    statement.references = None

        if not model.patch:
            for prop in model.get_properties():
                for stmt in self.claims.get(prop) or []:
                    compress(stmt)
            return
        grouped: Dict[str, List[Statement]] = {}
        for stmt in model.patch:
            compress(stmt)
            grouped.setdefault(stmt.mainsnak.property, []).append(stmt)
        for prop, items in grouped.items():
            self.claims[prop] = items

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

    def json(self) -> str:
        data: Dict[str, Any] = {}
        if self.qid:
            data['id'] = self.qid
        if self.labels:
            data['labels'] = {
                lang: {'language': lang, 'value': text}
                for lang, text in self.labels.items()
            }
        if self.claims:
            claims: Dict[str, List[Any]] = {}
            for prop, statements in self.claims.items():
                items: List[Any] = []
                for stmt in statements or []:
                    try:
                        items.append(json.loads(stmt.json()))
                    except json.JSONDecodeError:
                        logging.error('Invalid statement JSON for %s', prop)
                claims[prop] = items
            data['claims'] = claims
        return json.dumps(data, ensure_ascii=False)

    def write(self) -> Optional[str]:
        payload: Dict[str, Any] = {'data': self.json()}
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
            return self.qid
        return None