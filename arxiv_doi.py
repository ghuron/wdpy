import logging
from wdpy import Item, logon
from wdpy.evaluator import Evaluator

if logon('Ghuron@Ghuron', '1t0ev93e72p8neccgquoo87jllr49mli'):
    logging.basicConfig(level=logging.INFO)
    for qid in ["Q130502719", "Q130421003", "Q130413383", "Q130413336", "Q130413332", "Q130413256", "Q130412856", "Q130404246", "Q130404236"]:
        item = Item(qid)
        if synced := item.sync():
            qid = synced.write()
            violations = Evaluator().check(item, qid)
            for v in violations:
                print(v)
        pass
