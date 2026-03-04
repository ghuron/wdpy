from typing import Any, Dict, List, Optional, Union
from wdpy import build_request, parse_csv, SourceItem, Statement

class AstroItem(SourceItem):
    @classmethod
    def exec(cls, adql: str, table1: Optional[List[str]] = None) -> Optional[Dict[str, Union[Dict[str, str], List[Dict[str, str]]]]]:
        """Execute ADQL query against DALI-sync endpoint."""

        payload: Dict[str, Any] = {"request": "doQuery", "lang": "ADQL",
                                    "query": adql, "format": "csv",
                                    "maxrec": "-1"}
        if table1: payload.update({"upload": "table1,param:csv", "csv": table1})
        headers = {"Content-Type": "multipart/form-data"} if table1 else None
        return parse_csv(build_request(cls._config['tap_endpoint'], params=payload, headers=headers))

    @staticmethod
    def parse_url(url: str) -> Optional[str]:
        """Try to find qid of the reference based on the url provided"""
        import re, urllib.parse
        if url and url.strip() and (url := url.split()[0]):  # get text before first whitespace and strip
            for item in AstroItem._config['transform']:
                result = re.subn(item['pattern'], item['repl'], url, flags=re.S)
                if result[1] == 1:
                    return AstroItem.lookup(item['property'], urllib.parse.unquote(result[0]))

    @staticmethod
    def get_canonical_name(name: str) -> Optional[str]:
        """Return SIMBAD main identifier for a given object name."""

        from wdpy.connectors import SimbadItem

        query = (
            "SELECT main_id FROM basic JOIN ident ON oid=oidref "
            "WHERE id = '{}'".format(name.replace("'", "''"))
        )
        if (rows := SimbadItem.exec(query)) and (main_id := list(rows.keys())[0]):
            return main_id
        return None

    @classmethod
    def extract(cls, ident: Statement) -> Optional[SourceItem]:
        return None # Temporary until ADQL extractor is implemented