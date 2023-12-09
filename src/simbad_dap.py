#!/usr/bin/python3
from adql import ADQL
from wd import Wikidata


class SimbadDAP(ADQL):
    db_property, db_ref = 'P3083', 'Q654724'

    @staticmethod
    def get_next_chunk(offset):
        if isinstance(offset, str):
            query = 'SELECT oidref FROM ident WHERE id=\'{}\''.format(offset)
            if len(ident := SimbadDAP.tap_query(SimbadDAP.config('endpoint'), query)) == 1:
                SimbadDAP.load('id = \'{}\''.format(list(ident.keys())[0]))
            return [], None
        elif len(ADQL.dataset) > 0:  # TODO: sliding window
            return [], None
        SimbadDAP.load('id BETWEEN {} AND {}'.format(0, 10000))
        return SimbadDAP.dataset.keys(), None

    __var_types = None

    @classmethod
    def construct_snak(cls, row, col, new_col=None):
        if (new_col := col) == 'p397':
            new_col = 'p361' if row['parent_type'] in SimbadDAP.config("groups") else new_col
        elif col == 'p215':
            row[col] = row[col].replace(' ', '')
        elif col == 'p881':
            if not SimbadDAP.__var_types:
                SimbadDAP.__var_types = Wikidata.query(
                    'SELECT ?c ?i {?i wdt:P279+ wd:Q6243; p:P528[ps:P528 ?c; pq:P972 wd:Q222662]}')
            if (gcvs := row[col].upper().strip(':')) in SimbadDAP.__var_types:
                row[col] = SimbadDAP.__var_types[gcvs]
            else:
                return
        elif col == 'p2216' and row['p2216t'] != 'v':
            return
        return super().construct_snak(row, col, new_col)

    @classmethod
    def enrich_qualifier(cls, snak, value):
        if (snak := super().enrich_qualifier(snak, value)) and (snak['property'].upper() == 'P528'):
            snak['datavalue']['value'] = value[3:] if value.startswith('V* ') else value
        return snak

    @staticmethod
    def get_id_by_name(name: str):
        q = 'SELECT main_id FROM ident JOIN basic ON oid = oidref WHERE id=\'{}\''.format(name.replace('\'', '\'\''))
        if (row := ADQL.tap_query(SimbadDAP.config('endpoint'), q)) and (len(row) == 1):
            return list(row.keys())[0]


if SimbadDAP.initialize(__file__):  # if not imported
    wd_items = SimbadDAP.get_all_items('SELECT DISTINCT ?id ?item {?item wdt:P3083 ?id; ^wdt:P397 []}')
    for simbad_id in wd_items:
        # simbad_id = '* 51 Eri b'
        SimbadDAP(simbad_id, wd_items[simbad_id]).update(SimbadDAP.prepare_data(simbad_id))
