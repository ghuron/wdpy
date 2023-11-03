from json import dumps
from sys import argv

from adql import ADQL
from wd import Wikidata

TAP = 'https://simbad.u-strasbg.fr/simbad/sim-tap'
QUERY = 'SELECT id, main_id from ident JOIN basic ON oidref = oid WHERE main_id != id AND id IN (\'{}\')'

Wikidata.logon(argv[1], argv[2])
offset = -(size := 10000)
while (offset := offset + size) >= 0:
    if chunk := Wikidata.query('SELECT ?i ?s {{?s ps:P3083 ?i}} LIMIT {} OFFSET {}'.format(size, offset)):
        if ident := ADQL.tap_query(TAP, QUERY.format('\',\''.join([i.replace('\'', '\'\'') for i in chunk.keys()]))):
            for simbad_id in ident:
                if (new_id := ident[simbad_id][0]['main_id']) != simbad_id:
                    p3083 = (item := ADQL(new_id)).obtain_claim(ADQL.create_snak('P3083', new_id))
                    p3083['id'] = chunk[simbad_id].replace('-', '$', 1).replace('statement/', '')
                    Wikidata.edit(data={'summary': 'was ' + simbad_id, 'claim': dumps(p3083)}, method='wbsetclaim')
    elif chunk is not None and len(chunk) == 0:
        break
