import json
from wdpy import SourceItem, Statement


class EuropePMC(SourceItem):
    def parse(self, text: str, ident: Statement) -> None:
        results = json.loads(text).get('resultList', {}).get('result', [])
        if len(results) != 1:
            if ident.mainsnak.property == 'P698':
                self.prior_ident = ident
            return
        d = results[0]
        fields = self._config.get('fields', {})
        translate = self._config.get('translate', {})
        self.obtain(d, {k: v for k, v in fields.items() if k != 'title'}, translate)
        if title := d.get('title'):
            self.add_claim('P1476', title, 'en')
        for a in d.get('authorList', {}).get('author', []):
            orcid = a['authorId']['value'] if a.get('authorId', {}).get('type') == 'ORCID' else None
            self.add_author(a.get('firstName', '') + ' ' + a.get('lastName', ''), orcid)
