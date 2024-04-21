#!/usr/bin/python3
import math

import adql
import wd


class Model(adql.Model):
    property, __var_types = 'P3083', None

    @staticmethod
    def get_next_chunk(offset):
        if isinstance(offset, str):
            query = 'SELECT oidref FROM ident WHERE id=\'{}\''.format(offset)
            if (ident := Model.tap_query(Model.config('endpoint'), query)) and (len(ident) == 1):
                Model.load('id = \'{}\''.format(list(ident.keys())[0]))
            return [], None
        elif len(Model.dataset) > 0:  # TODO: sliding window
            return [], None
        Model.load('id BETWEEN {} AND {}'.format(0, 10000))
        return Model.dataset.keys(), None

    @classmethod
    def construct_snak(cls, row, col, new_col=None):
        if (new_col := col) == 'p397':
            new_col = 'p361' if row['parent_type'] in Model.config("groups") else new_col
        elif col == 'p215':
            row[col] = row[col].replace(' ', '')
        elif col == 'p7015':
            row['p7015'] = math.pow(10, (n := float(row['p7015'])))
            row['p7015p'] = p if (p := int(row['p7015p'])) > -round(n) else -round(n)
            while ((c := round(row['p7015'], row['p7015p'] - 1)) > 0) and (round(math.log10(c), p) == n):
                row['p7015p'] = row['p7015p'] - 1
            row['p7015'] = round(row['p7015'], row['p7015p'])
        elif col == 'p881':
            if not Model.__var_types:
                Model.__var_types = wd.Wikidata.query(
                    'SELECT ?c ?i {?i wdt:P279+ wd:Q6243; p:P528[ps:P528 ?c; pq:P972 wd:Q222662]}')
            if (gcvs := row[col].upper().strip(':')) in Model.__var_types:
                row[col] = Model.__var_types[gcvs]
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
        if (row := Model.tap_query(Model.config('endpoint'), q)) and (len(row) == 1):
            return list(row.keys())[0]


class Element(adql.Element):
    _model, _claim = Model, type('Claim', (wd.Claim,), {'db_ref': 'Q654724'})


if Model.initialize(__file__):  # if not imported
    wd_items = Model.get_all_items('SELECT DISTINCT ?id ?item {?item wdt:P3083 ?id; ^wdt:P397 []}')
    for simbad_id in wd_items:
        # simbad_id = '* 51 Eri b'
        Element.run(simbad_id, wd_items[simbad_id])
