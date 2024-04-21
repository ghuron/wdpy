#!/usr/bin/python3
from sys import argv

from adql import Model
from wd import Wikidata, Claim

TAP = 'https://simbad.u-strasbg.fr/simbad/sim-tap'
QUERY = 'SELECT id, main_id from ident JOIN basic ON oidref = oid WHERE main_id != id AND id IN (\'{}\')'

Wikidata.logon(argv[1], argv[2])
offset = -(size := 10000)
while (offset := offset + size) >= 0:
    if chunk := Wikidata.query('SELECT ?i ?s {{?s ps:P3083 ?i}} LIMIT {} OFFSET {}'.format(size, offset)):
        if ident := Model.tap_query(TAP, QUERY.format('\',\''.join([i.replace('\'', '\'\'') for i in chunk.keys()]))):
            for old_id in ident:
                if ((new_id := ident[old_id][0]['main_id']) != old_id) and (old_id in chunk):
                    statement_id = chunk[old_id].replace('-', '$', 1).replace('statement/', '')
                    Claim.construct(Model.create_snak('P3083', new_id), statement_id).save('was ' + old_id)
    elif chunk is not None and len(chunk) == 0:
        break
