import logging
import time
from wdpy import logon, exec, Statement, Snak, connectors


if logon('Ghuron@Ghuron', '1t0ev93e72p8neccgquoo87jllr49mli'):
    logging.basicConfig(level=logging.INFO)
    ADQL = '''SELECT col1, main_id 
            FROM TAP_UPLOAD.table1
            LEFT JOIN ident ON col1 = id
            LEFT JOIN basic ON oid = oidref'''
    offset = -(size := 5000) + 1000000
    while (offset := offset + size) >= 0:
        if chunk := exec(f'SELECT ?i ?s {{?s ps:P3083 ?i}} LIMIT {size} OFFSET {offset}'):
            if redirect := connectors.SimbadItem.exec(ADQL, list(chunk.keys())):
                for old_id in chunk:
                    if old_id not in redirect:
                        logging.info(f'Did not receive main_id for {old_id}')
                    else:
                        statement_id = chunk[old_id]['s'].replace('-', '$', 1).replace('statement/', '')
                        if not (new_id := redirect[old_id].get('main_id')):
                            p3083 = Statement(Snak('P3083', (old_id,)), statement_id)
                            p3083.set_rank('deprecated', 'Q21441764')
                            p3083.save('deprecated due to its absence in [[Q654724]]')
                            time.sleep(1)
                        elif old_id != redirect[old_id].get('main_id'):
                            p3083 = Statement(Snak('P3083', (redirect[old_id]['main_id'],)), statement_id)
                            p3083.save(f'was {old_id}')
                            time.sleep(1)
        elif chunk is not None and len(chunk) == 0:
            logging.info(f'0 results for offset {offset}')
            break


