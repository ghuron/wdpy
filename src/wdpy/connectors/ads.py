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

    def parse(self, text: str) -> bool:
        docs = json.loads(text).get('response', {}).get('docs', [])
        if len(docs) != 1:
            if self.patch and self.patch[0].mainsnak.property == 'P819':
                logging.warning('Got %d results for %s', len(docs), self.patch[0].mainsnak.value)
                self.patch = []
                return True
            return False
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
        for ident in d.get('identifier', []):
            if ident.startswith('arXiv:'):
                self.add_claim('P818', ident[6:])
            elif ident.startswith('10.') and 'ARXIV' not in ident.upper():
                self.add_claim('P356', ident)
        self.add_claim('P31', 'Q13442814')
        return True


def _get_orcid(d: dict, idx: int) -> Optional[str]:
    for key, orcids in d.items():
        if key.startswith('orcid_') and isinstance(orcids, list) and idx < len(orcids):
            if len(orcids[idx]) > 10:
                return orcids[idx].replace('orcid:', '')
    return None
