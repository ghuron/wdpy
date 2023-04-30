#!/usr/bin/python3
from os.path import basename
from sys import argv

from adql import ADQL


class SimbadDAP(ADQL):
    ADQL.load_config(__file__)
    db_property, db_ref = 'P3083', 'Q654724'

    @staticmethod
    def get_next_chunk(offset):
        if len(ADQL.cache) > 0:
            return [], None
        SimbadDAP.load()
        return ADQL.cache.keys(), None

    @staticmethod
    def load(condition=None):
        if condition:
            query = 'SELECT oidref FROM ident WHERE id=\'{}\''.format(condition)
            if len(ident := SimbadDAP.tap_query('https://simbad.u-strasbg.fr/simbad/sim-tap', query)) == 1:
                return ADQL.load('id={}'.format(list(ident.keys())[0]))
        else:
            return ADQL.load('id BETWEEN {} AND {}'.format(0, 10000))

    @staticmethod
    def construct_snak(row, col, new_col=None):
        if (new_col := col) == 'p397':
            if parent_id := SimbadDAP.get_by_id(row[col]):
                row[col] = parent_id
                new_col = 'p361' if row['parent_type'] in SimbadDAP.config["groups"] else new_col
        elif col == 'p215':
            row[col] = row[col].replace(' ', '')
        elif col == 'p2216' and row['p2216t'] != 'v':
            return

        if snak := ADQL.construct_snak(row, col, new_col):
            if 'filter' in row and row['filter'] in SimbadDAP.config['band']:
                snak['qualifiers'] = {'P1227': SimbadDAP.config['band'][row['filter']]}
        return snak


if argv[0].endswith(basename(__file__)):  # if not imported
    SimbadDAP.logon(argv[1], argv[2])
    wd_items = SimbadDAP.get_all_items('SELECT DISTINCT ?id ?item {?item wdt:P3083 ?id; ^wdt:P397 []}')
    for simbad_id in wd_items:
        # simbad_id = '* 51 Eri b'
        item = SimbadDAP(simbad_id, wd_items[simbad_id])
        item.prepare_data()
        item.update()
