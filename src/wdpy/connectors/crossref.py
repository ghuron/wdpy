import json
from wdpy import obtain_handler, SourceItem, Statement


class Crossref(SourceItem):
    @obtain_handler('published-online')
    def _handle_date(self, val: dict) -> None:
        parts = (val.get('date-parts') or [[]])[0]
        if parts:
            self.add_claim('P577', '-'.join(str(p).zfill(2) for p in parts[:3]))

    @obtain_handler('author')
    def _handle_authors(self, authors: list) -> None:
        for a in authors:
            raw = a.get('ORCID')
            orcid = raw.rsplit('/', 1)[-1] if raw else None
            self.add_author(f"{a.get('given', '')} {a.get('family', '')}".strip(), orcid)

    def parse(self, text: str, ident: Statement) -> None:
        self.obtain(json.loads(text), self._config.get('fields', {}))
