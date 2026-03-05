import json
from wdpy import SourceItem, Statement


class Crossref(SourceItem):
    def parse(self, text: str, ident: Statement) -> None:
        self.obtain(json.loads(text), self._config.get('fields', {}), self._config.get('translate', {}))
