from json import dumps
from sys import argv

from adql import ADQL

ADQL.logon(argv[1], argv[2])
QUERY = 'SELECT id, main_id from ident JOIN basic ON oidref = oid WHERE id IN ({})'
offset, size = 0, 10000
while True:
    if len(chunk := ADQL.query('SELECT ?i ?s {{?s ps:P3083 ?i}} LIMIT {} OFFSET {}'.format(size, offset))) == 0:
        quit()
    ident = ADQL.tap_query('https://simbad.u-strasbg.fr/simbad/sim-tap',
                           QUERY.format('\'{}\''.format('\',\''.join([i.replace('\'', '\'\'') for i in chunk.keys()]))))
    for qid, statement in chunk.items():
        statement = statement.replace('-', '$', 1).replace('statement/', '')
        if qid not in ident:
            ADQL(qid).edit({'claim': statement, 'summary': 'no longer in [[Q654724]]'}, 'wbremoveclaims')
        elif qid != ident[qid][0]['main_id']:
            item = ADQL(ident[qid][0]['main_id'])
            (p3083 := item.obtain_claim(ADQL.create_snak('P3083', ident[qid][0]['main_id'])))['id'] = statement
            item.edit({'claim': dumps(p3083), 'summary': 'was ' + qid}, 'wbsetclaim')
    offset += size
