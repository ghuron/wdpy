#!/usr/bin/python3
import logging

from adql import ADQL
from wd import Wikidata


class SimbadDAP(ADQL):
    db_property, db_ref = 'P3083', 'Q654724'

    @staticmethod
    def get_next_chunk(offset):
        if isinstance(offset, str):
            query = 'SELECT oidref FROM ident WHERE id=\'{}\''.format(offset)
            if len(ident := SimbadDAP.tap_query(SimbadDAP.config['endpoint'], query)) == 1:
                SimbadDAP.load('id = \'{}\''.format(list(ident.keys())[0]))
            return [], None
        elif len(ADQL.dataset) > 0:  # TODO: sliding window
            return [], None
        SimbadDAP.load('id BETWEEN {} AND {}'.format(0, 10000))
        return SimbadDAP.dataset.keys(), None

    @classmethod
    def construct_snak(cls, row, col, new_col=None):
        if (new_col := col) == 'p397':
            new_col = 'p361' if row['parent_type'] in SimbadDAP.config["groups"] else new_col
        elif col == 'p215':
            row[col] = row[col].replace(' ', '')
        elif col == 'p2216' and row['p2216t'] != 'v':
            return
        return super().construct_snak(row, col, new_col)

    @classmethod
    def enrich_qualifier(cls, snak, value):
        if (snak := super().enrich_qualifier(snak, value)) and (snak['property'].upper() == 'P528'):
            snak['datavalue']['value'] = value[3:] if value.startswith('V* ') else value
        return snak

    _cache = None

    @staticmethod
    def get_parent_object(ident: str):
        if SimbadDAP._cache is None:
            SimbadDAP._cache = Wikidata.query('SELECT DISTINCT ?c ?i { ?i ^ps:P397 []; wdt:P528 ?c }',
                                              lambda row, _: (row[0].lower(), row[1]))
        if ident.lower() in SimbadDAP._cache:
            return SimbadDAP._cache[ident.lower()]
        q = 'SELECT main_id FROM ident JOIN basic ON oid = oidref WHERE id=\'{}\''.format(ident.replace('\'', '\'\''))
        if ident and (row := SimbadDAP.tap_query(SimbadDAP.config['endpoint'], q)):
            if len(row) == 1:
                if (main_id := list(row.keys())[0]).lower() not in SimbadDAP._cache:
                    if (qid := SimbadDAP.get_by_id(main_id)) is None:
                        return
                    SimbadDAP._cache[main_id.lower()] = qid
                SimbadDAP._cache[ident.lower()] = SimbadDAP._cache[main_id.lower()]
                logging.info('Cache miss: "{}" for {}'.format(ident, SimbadDAP._cache[ident.lower()]))
                return SimbadDAP._cache[ident.lower()]


if SimbadDAP.initialize(__file__):  # if not imported
    wd_items = SimbadDAP.get_all_items('SELECT DISTINCT ?id ?item {?item wdt:P3083 ?id; ^wdt:P397 []}')
    for simbad_id in wd_items:
        # simbad_id = '* 51 Eri b'
        SimbadDAP(simbad_id, wd_items[simbad_id]).update(SimbadDAP.prepare_data(simbad_id))
