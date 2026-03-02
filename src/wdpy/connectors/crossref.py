import json
from wdpy import SourceItem


class Crossref(SourceItem):
    def parse(self, text: str) -> None:
        self.obtain(json.loads(text), self._config.get('fields', {}), self._config.get('translate', {}))
