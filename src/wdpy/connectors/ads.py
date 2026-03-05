import json
import logging
from typing import Optional
from urllib.request import Request
from wdpy import SourceItem, Statement, build_request


class ADS(SourceItem):
    @classmethod
    def make_request(cls, ident: Statement) -> Optional[Request]:
        if url := cls._config["properties"].get(ident.mainsnak.property):
            return build_request(
                url.format((ident.mainsnak.value or ('',))[0]) +
                f'&fl={",".join(ADS._config["fields"])}',
                headers={'Authorization': 'Bearer ogOoi0uDxIebyeseB3tAbf5mBTJxXQQWQqE5TW40'}
            )

    def parse(self, text: str, ident: Statement) -> None:
        docs = json.loads(text).get('response', {}).get('docs', [])
        if len(docs) != 1:
            if ident.mainsnak.property == 'P819':
                logging.warning('Got %d results for %s', len(docs), ident.mainsnak.value)
                if len(docs) == 0:
                    self.prior_ident = ident
            return
        d = docs[0]
        if 'page' in d:
            p = d['page'][0] if isinstance(d['page'], list) else d['page']
            if not str(p).isalnum():
                del d['page']
            elif 'page_count' in d:
                try:
                    d['page'] = f'{p} - {int(p) + d["page_count"] - 1}'
                except (ValueError, KeyError):
                    pass
        self.obtain(d, self._config.get('fields', {}), self._config.get('translate', {}))
        for i, name in enumerate(d.get('author', []), 1):
            self.add_author(name, _get_orcid(d, i - 1))
        for identifier in d.get('identifier', []):
            if identifier.startswith('arXiv:'):
                self.add_claim('P818', identifier[6:])
            elif identifier.startswith('10.') and 'ARXIV' not in identifier.upper():
                self.add_claim('P356', identifier)
        self.add_claim('P31', 'Q13442814')


def _get_orcid(d: dict, idx: int) -> Optional[str]:
    for key, orcids in d.items():
        if key.startswith('orcid_') and isinstance(orcids, list) and idx < len(orcids):
            if len(orcids[idx]) > 10:
                return orcids[idx].replace('orcid:', '')
    return None
