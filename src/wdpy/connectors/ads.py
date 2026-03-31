import json
import logging
import re
from pathlib import Path
from typing import Optional
from urllib.request import Request
from wdpy import Snak, SourceItem, Statement, build_request

_TOKEN_PATH = Path(__file__).parents[3] / '.ads'
_TOKEN = _TOKEN_PATH.read_text().strip() if _TOKEN_PATH.exists() else None


class ADS(SourceItem):
    @classmethod
    def make_request(cls, ident: Statement) -> Optional[Request]:
        if _TOKEN is None:
            return None
        if url := cls._config["properties"].get(ident.mainsnak.property):
            return build_request(
                url.format((ident.mainsnak.value or ('',))[0]) +
                f'&fl={",".join(ADS._config["fields"])}',
                headers={'Authorization': f'Bearer {_TOKEN}'}
            )

    def parse(self, text: str, ident: Statement) -> None:
        docs = json.loads(text).get('response', {}).get('docs', [])
        if len(docs) != 1:
            if ident.mainsnak.property == 'P819':
                logging.warning('Got %d results for %s', len(docs), ident.mainsnak.value)
                if len(docs) == 0:
                    self.deprecate_ident(ident)
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
        fields = self._config.get('fields', {})
        translate = self._config.get('translate', {})

        bibcode = d.get('bibcode', '')
        if d.get('pub') == 'The Astrophysical Journal' and (
            'ApJL' in bibcode or re.search(r'ApJ\.+\d+L', bibcode)
        ):
            d['pub'] = 'The Astrophysical Journal Letters'

        redirected = False
        if ident.mainsnak.property == 'P819':
            bibcode = d.get('bibcode')
            if bibcode and bibcode != ident.mainsnak.value[0]:
                self.deprecate_ident(ident)
                redirected = True

        self.obtain(d, fields, translate)
        for i, name in enumerate(d.get('author', []), 1):
            self.add_author(name, _get_orcid(d, i - 1))

        identifiers = d.get('identifier', [])
        arxiv_ids = [i[6:] for i in identifiers if i.startswith('arXiv:')]
        dois = [i for i in identifiers if i.startswith('10.')]
        bibcodes = [i for i in identifiers if not i.startswith('arXiv:') and not i.startswith('10.')]
        if redirected:
            bibcodes = [bc for bc in bibcodes if bc != ident.mainsnak.value[0]]

        for arxiv_id in arxiv_ids:
            self.add_claim('P818', arxiv_id)

        has_journal_doi = any('ARXIV' not in doi.upper() for doi in dois)
        for doi in dois:
            s = self.add_claim('P356', doi)
            if s and has_journal_doi and 'ARXIV' in doi.upper():
                s.rank = 'deprecated'

        has_journal_bibcode = any('arxiv' not in bc.lower() for bc in bibcodes)
        for bc in bibcodes:
            s = self.add_claim('P819', bc)
            if s and has_journal_bibcode and 'arxiv' in bc.lower():
                s.rank = 'deprecated'

        self.add_claim('P31', 'Q13442814')


def _get_orcid(d: dict, idx: int) -> Optional[str]:
    for key, orcids in d.items():
        if key.startswith('orcid_') and isinstance(orcids, list) and idx < len(orcids):
            if len(orcids[idx]) > 10:
                return orcids[idx].replace('orcid:', '')
    return None
