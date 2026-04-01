import logging
import time
from wdpy import Item, logon, exec
from wdpy.evaluator import Evaluator

_SCHOLARLY = 'https://query-scholarly.wikidata.org/sparql'
_QUERY = 'SELECT ?item { ?item p:P1433/ps:P1433 wd:Q3470990; p:P1433/ps:P1433 wd:Q598789 } LIMIT 100'

if logon('Ghuron@Ghuron', '1t0ev93e72p8neccgquoo87jllr49mli'):
    logging.basicConfig(level=logging.INFO)
    for qid in (exec(_QUERY, _SCHOLARLY) or {}).keys():
        item = Item(qid)
        if synced := item.sync():
            if qid := synced.write():
                time.sleep(10)
                violations = Evaluator().check(item, qid)
                for v in violations:
                    print(v)
