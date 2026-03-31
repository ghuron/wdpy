import logging
from wdpy import Item, logon
from wdpy.evaluator import Evaluator

if logon('Ghuron@Ghuron', '1t0ev93e72p8neccgquoo87jllr49mli'):
    logging.basicConfig(level=logging.INFO)
    item = Item('Q21955283')
    if synced := item.sync():
        qid = synced.write()
        violations = Evaluator().check(item, qid)
        for v in violations:
            print(v)
