import json
from urllib.request import Request, build_opener
from wdpy import obtain_handler, SourceItem, Statement


class Crossref(SourceItem):
    @obtain_handler('link')
    def _handle_link(self, links: list) -> None:
        for link in links:
            if link.get('content-type') == 'application/pdf' and link.get('intended-application') == 'syndication':
                if url := link.get('URL', ''):
                    try:
                        resp = build_opener().open(Request(url, method='HEAD'), timeout=30)
                        if resp.getcode() == 200:
                            self.add_claim('P953', url)
                    except Exception:
                        pass
                return

    @obtain_handler('published-online')
    def _handle_date(self, val: dict) -> None:
        parts = (val.get('date-parts') or [[]])[0]
        if parts:
            self.add_claim('P577', '-'.join(str(p).zfill(2) for p in parts[:3]))

    @obtain_handler('reference')
    def _handle_references(self, refs: list) -> None:
        for ref in refs:
            if (doi := ref.get('DOI')) and (qid := self.lookup('P356', doi.upper())):
                self.add_claim('P2860', qid)

    @obtain_handler('ISSN')
    def _handle_issn(self, issns: list) -> None:
        for issn in issns:
            if qid := self.lookup('P236', issn):
                self.add_claim('P1433', qid)
                return

    @obtain_handler('author')
    def _handle_authors(self, authors: list) -> None:
        for a in authors:
            raw = a.get('ORCID')
            orcid = raw.rsplit('/', 1)[-1] if raw else None
            self.add_author(f"{a.get('given', '')} {a.get('family', '')}".strip(), orcid)

    def parse(self, text: str, ident: Statement) -> None:
        self.obtain(json.loads(text), self._config.get('fields', {}))
