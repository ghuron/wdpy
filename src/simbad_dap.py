#!/usr/bin/python3
from os.path import basename
from sys import argv

from adql import ADQL


class SimbadDAP(ADQL):
    config = ADQL.load_config(__file__)
    db_property, db_ref = 'P3083', 'Q654724'

    @staticmethod
    def get_next_chunk(offset):
        if isinstance(offset, str):
            query = 'SELECT oidref FROM ident WHERE id=\'{}\''.format(offset)
            if len(ident := SimbadDAP.tap_query(SimbadDAP.config['endpoint'], query)) == 1:
                SimbadDAP.load('id = \'{}\''.format(list(ident.keys())[0]))
            return [], None
        elif len(ADQL.cache) > 0:  # TODO: sliding window
            return [], None
        SimbadDAP.load('id BETWEEN {} AND {}'.format(0, 10000))
        return SimbadDAP.cache.keys(), None

    def construct_snak(self, row, col, new_col=None):
        if (new_col := col) == 'p397':
            new_col = 'p361' if row['parent_type'] in SimbadDAP.config["groups"] else new_col
        elif col == 'p215':
            row[col] = row[col].replace(' ', '')
        elif col == 'p2216' and row['p2216t'] != 'v':
            return

        if snak := super().construct_snak(row, col, new_col):
            if 'filter' in row and row['filter'] in SimbadDAP.config['band']:
                snak['qualifiers'] = {'P1227': SimbadDAP.config['band'][row['filter']]}
        return snak

    @staticmethod
    def get_by_any_id(ident: str) -> str:
        q = 'SELECT main_id FROM ident JOIN basic ON oid = oidref WHERE id=\'{}\''.format(ident.replace('\'', '\'\''))
        if len(row := SimbadDAP.tap_query(SimbadDAP.config['endpoint'], q)) == 1:
            return SimbadDAP.get_by_id(list(row.keys())[0])


if argv[0].endswith(basename(__file__)):  # if not imported
    SimbadDAP.logon(argv[1], argv[2])
    wd_items = SimbadDAP.get_all_items('SELECT DISTINCT ?id ?item {?item wdt:P3083 ?id; ^wdt:P397 []}')
    for simbad_id in wd_items:
        # simbad_id = '* 51 Eri b'
        item = SimbadDAP(simbad_id, wd_items[simbad_id])
        item.prepare_data()
        item.update()
