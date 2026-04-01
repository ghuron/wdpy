import json
from typing import Optional
from urllib.request import Request
from wdpy import SourceItem, Statement


class ORCID(SourceItem):
    @classmethod
    def make_request(cls, ident: Statement) -> Optional[Request]:
        if req := super().make_request(ident):
            req.add_header('Accept', 'application/json')
        return req

    def parse(self, text: str, ident: Statement) -> None:
        data = json.loads(text)
        if 'path' not in data:
            self.deprecate_ident(ident)
            return
        self.add_claim('P496', data['path'].split('/')[1])
        self.add_claim('P31', 'Q5')
        self.add_claim('P106', 'Q1650915')
        name = data.get('name') or {}
        given = (name.get('given-names') or {}).get('value', '')
        family = (name.get('family-name') or {}).get('value', '')
        if full := f'{given} {family}'.strip():
            self.proposed_label = full
        for i in data.get('external-identifiers', {}).get('external-identifier', []):
            prop = self._config.get('fields', {}).get(i.get('external-id-type'))
            if prop:
                self.add_claim(prop, i.get('external-id-value'))
