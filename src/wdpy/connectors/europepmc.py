import json
from wdpy import obtain_handler, SourceItem, Statement


class EuropePMC(SourceItem):
    @obtain_handler('title')
    def _handle_title(self, val: str) -> None:
        self.add_claim('P1476', val, 'en')

    @obtain_handler('authorList')
    def _handle_authors(self, val: dict) -> None:
        for a in val.get('author', []):
            orcid = a.get('authorId', {}).get('value') if a.get('authorId', {}).get('type') == 'ORCID' else None
            self.add_author(f"{a.get('firstName', '')} {a.get('lastName', '')}".strip(), orcid)

    def parse(self, text: str, ident: Statement) -> None:
        results = json.loads(text).get('resultList', {}).get('result', [])
        if len(results) != 1:
            if ident.mainsnak.property == 'P698':
                self.deprecate_ident(ident)
            return
        self.obtain(results[0], self._config.get('fields', {}))
