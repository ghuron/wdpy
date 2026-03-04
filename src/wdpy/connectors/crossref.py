import json
from wdpy import SourceItem


class Crossref(SourceItem):
    def parse(self, text: str) -> bool:
        self.obtain(json.loads(text), self._config.get('fields', {}), self._config.get('translate', {}))
        return True
