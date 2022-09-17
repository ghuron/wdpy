#!/usr/bin/python3
import csv
import json
import re
import sys
from contextlib import closing
from decimal import InvalidOperation
import os.path
import requests
from astropy import coordinates as coord

from wikidata import WikiData


class SimbadDAP(WikiData):
    def __init__(self, login, password):
        super().__init__(login, password)
        self.db_ref = 'Q654724'
        self.db_property = 'P3083'
        self.constellations = self.query('SELECT DISTINCT ?n ?i {?i wdt:P31/wdt:P279* wd:Q8928; wdt:P1813 ?n}')
        self.ads_articles = self.query('SELECT ?id ?item {?item wdt:P819 ?id}')
        self.simbad = {}

    def get_next_chunk(self):
        if len(self.simbad) > 0:
            return []
        wd.load('''otype IN ('Pl', 'Pl?')''')
        return self.simbad.keys()

    def obtain_claim(self, entity, snak):
        min_pos = self.get_min_position(entity, snak['property'])
        claim = super().obtain_claim(entity, snak)
        if snak is not None and snak['property'] in ['P6257', 'P6258']:
            if snak['property'] in entity['claims']:
                for candidate in entity['claims'][snak['property']]:
                    if claim != candidate:
                        candidate['remove'] = 1
            epoch = self.obtain_claim(entity, self.create_snak('P6259', 'Q1264450'))  # J2000
            epoch['references'] = []
            self.add_refs(epoch, [self.db_ref])
        if 'rank' not in claim and 'mespos' in snak and int(snak['mespos']) > min_pos:
            claim['rank'] = 'deprecated'
        return claim

    def load(self, condition):
        for query in [  # p: precision, h: +error, l: -error, u: unit, r: reference
            '''SELECT main_id, otype AS P31, morph_type AS P223, morph_bibcode AS P223r, 
                    ra AS P6257, ra_prec AS P6257p, 'Q28390' AS P6257u, coo_bibcode AS P6257r, 
                    dec AS P6258, dec_prec AS P6258p, 'Q28390' AS P6258u, coo_bibcode AS P6258r, 
                    sp_type AS P215, sp_bibcode AS P215r,
                    plx_value AS P2214, plx_prec AS P2214p, 'Q21500224' AS P2214u, plx_err_prec AS P2214hp, 
                    plx_err AS P2214h, plx_err AS P2214l, plx_err_prec AS P2214lp, plx_bibcode AS P2214r,
                    pmra AS P10752, pmra_prec AS P10752p, pm_err_maj AS P10752h, pm_err_maj AS P10752l, 
                    pm_err_maj_prec AS P10752hp, pm_err_maj_prec AS P10752lp, 'Q22137107' AS P10752u, 
                    pm_bibcode AS P10752r,
                    pmdec AS P10751, pmdec_prec AS P10751p, pm_err_min AS P10751h, pm_err_min AS P10751l, 
                    pm_err_min_prec AS P10751hp, pm_err_min_prec AS P10751lp, 'Q22137107' AS P10751u, 
                    pm_bibcode AS P10751r,
                    rvz_radvel AS P2216, rvz_radvel_prec AS P2216p, rvz_err AS P2216h, rvz_err AS P2216l, 
                    rvz_err_prec AS P2216hp, rvz_err_prec AS P2216lp, 'Q3674704' AS P2216u, 
                    rvz_bibcode AS P2216r, rvz_type AS P2216t, 0 AS mespos
                FROM basic WHERE {} ''',
            '''SELECT id, main_id AS P397, link_bibcode AS P397r, otype AS parent_type, 0 AS mespos
                FROM (SELECT main_id AS id, oid FROM basic WHERE {} ORDER BY oid) b
                JOIN h_link ON h_link.child = b.oid JOIN basic s ON h_link.parent = s.oid''',
            '''SELECT id, pmra AS P10752, pmra_prec AS P10752p, pmra_err AS P10752h, pmra_err AS P10752l, 
                    pmra_err_prec AS P10752hp, pmra_err_prec AS P10752lp, 'Q22137107' AS P10752u, 
                    pmde AS P10751, pmde_prec AS P10751p, pmde_err AS P10751h, pmde_err_prec AS P10751hp, 
                    pmde_err AS P10751l, pmde_err_prec AS P10751lp, 'Q22137107' AS P10751u, 
                    bibcode AS P10751r, bibcode AS P10752r, mespos
                FROM (SELECT main_id AS id, oid FROM basic WHERE otype='Pl' ORDER BY oid) b
                JOIN mesPM ON oidref = oid
                WHERE coosystem='ICRS' ''',
            '''SELECT id, otype AS P31, origin AS P31r, 1 AS mespos
                FROM (SELECT main_id AS id, oid FROM basic WHERE {} ORDER BY oid) b
                JOIN otypes ON oidref = oid''',
            '''SELECT id, sptype AS P215, bibcode AS P215r, mespos
                FROM (SELECT main_id AS id, oid FROM basic WHERE {} ORDER BY oid) b
                JOIN mesSpT ON oidref = oid''',
            '''SELECT id, plx AS P2214, plx_prec AS P2214p, plx_err AS P2214h, plx_err AS P2214l, 
                    'Q21500224' AS P2214u, bibcode AS P2214r, mespos
                FROM (SELECT main_id AS id, oid FROM basic WHERE {} ORDER BY oid) b
                JOIN mesPlx ON oidref = oid'''
        ]:
            self.tap_query('https://simbad.u-strasbg.fr/simbad/sim-tap', query.format(condition), self.simbad)

    def get_snaks(self, identifier):
        if identifier not in self.simbad:
            self.load('main_id = \'' + identifier + '\'')  # attempt to load this specific object
            if identifier not in self.simbad:
                return None
        return self.parse_page(self.simbad[identifier])

    def post_process(self, entity):
        super().post_process(entity)
        if 'P59' not in entity['claims'] and 'P6257' in entity['claims'] and 'P6258' in entity['claims']:
            p = coord.SkyCoord(entity['claims']['P6257'][0]['mainsnak']['datavalue']['value']['amount'],
                               entity['claims']['P6258'][0]['mainsnak']['datavalue']['value']['amount'], frame='icrs',
                               unit='deg')
            const = self.constellations[p.get_constellation(short_name=True)]
            self.obtain_claim(entity, self.create_snak('P59', const if isinstance(const, str) else const[0]))
        for property_id in ['P2214', 'P2215', 'P10751', 'P10752']:
            if property_id in entity['claims']:
                for claim in entity['claims'][property_id]:
                    try:
                        for ref in claim['references']:
                            if 'P248' in ref['snaks']:
                                if ref['snaks']['P248'][0]['datavalue']['value']['id'] == self.db_ref:
                                    raise StopIteration
                        claim['remove'] = 1
                    except StopIteration:
                        pass

    @staticmethod
    def tap_query(url, sql, result=None):
        result = {} if result is None else result
        with closing(requests.post(url + '/sync', params={'request': 'doQuery', 'lang': 'adql', 'format': 'csv',
                                                          'maxrec': -1, 'query': sql, }, stream=True)) as r:
            reader = csv.reader(r.iter_lines(decode_unicode='utf-8'), delimiter=',', quotechar='"')
            header = next(reader)
            for line in reader:
                if len(line) > 0:
                    row = {}
                    for i in range(1, len(line)):
                        row[header[i]] = ' '.join(line[i].split()) if isinstance(line[i], str) else line[i]
                    object_id = ' '.join(line[0].split())
                    if object_id in result:
                        result[object_id].append(row)
                    else:
                        result[object_id] = [row]
        return result

    @staticmethod
    def format_figure(row, field):
        return SimbadDAP.format_float(row[field],
                                      row[field + 'p'] if field + 'p' in row and row[field + 'p'] != '' else -1)

    def parse_page(self, rows):
        mapping = {'?': 6999, 'ev': 2680861, 'Rad': 1931185, 'mR': 67201491, 'cm': 67201524, 'mm': 67201561,
                   'smm': 67201574, 'HI': 67201586, 'rB': 15809070, 'Mas': 1341811, 'IR': 67206691, 'FIR': 67206701,
                   'NIR': 67206785, 'red': 71797619, 'ERO': 71797766, 'blu': 71798532, 'UV': 71798788, 'X': 2154519,
                   'UX?': 2154519, 'ULX': 129686, 'gam': 71962386, 'gB': 22247, 'grv': 71962637, 'Le?': 71962637,
                   'gLe': 185243, 'GWE': 24748034, '..?': 72053253, 'G?': 72053617, 'SC?': 72054258, 'C?G': 72054258,
                   'Gr?': 72533545, '**?': 72534196, 'EB?': 72534536, 'Sy?': 72672560, 'CV?': 72704237, 'No?': 72705413,
                   'XB?': 2154519, 'LX?': 2154519, 'HX?': 2154519, 'Pec?': 72802810, 'Y*?': 72802977, 'pr?': 523,
                   'TT?': 523, 'C*?': 523, 'S*?': 523, 'OH?': 523, 'CH?': 523, 'WR?': 523, 'Be?': 523, 'Ae?': 523,
                   'HB?': 523, 'RR?': 523, 'Ce?': 523, 'RB?': 72802727, 'sg?': 523, 's?r': 523, 's?y': 523, 's?b': 523,
                   'AB?': 523, 'LP?': 523, 'Mi?': 523, 'sv?': 523, 'pA?': 523, 'BS?': 523, 'HS?': 523, 'WD?': 523,
                   'N*?': 523, 'BH?': 523, 'SN?': 523, 'LM?': 523, 'BD?': 3132741, 'vid': 845371, 'SCG': 27521,
                   'ClG': 204107, 'GrG': 1491746, 'CGG': 71963409, 'PaG': 28738741, 'IG': 644507, 'Gl?': 72803708,
                   'Cl*': 168845, 'GlC': 11276, 'OpC': 11387, 'As*': 9262, 'St*': 935337, 'MGr': 12046080, '**': 13890,
                   'EB*': 1457376, 'Al*': 24452, 'bL*': 830831, 'WU*': 691269, 'EP*': 1457376, 'SB*': 1993624,
                   'El*': 1332364, 'Sy*': 18393176, 'CV*': 1059564, 'DQ*': 1586249, 'AM*': 294562, 'NL*': 9283100,
                   'No*': 6458, 'DN*': 244264, 'XB*': 5961, 'LXB': 1407562, 'HXB': 845169, 'ISM': 41872, 'Cld': 1054444,
                   'GNe': 1054444, 'BNe': 1054444, 'DNe': 204194, 'RNe': 203958, 'MoC': 272447, 'glb': 213936,
                   'cor': 97570336, 'SFR': 27150479, 'HVC': 1621824, 'HII': 11282, 'PN': 13632, 'SNR': 207436,
                   'cir': 41872, 'of?': 41872, 'out': 12053157, 'HH': 50048, '*': 523, '*iC': 523, '*iN': 523,
                   '*iA': 523, '*i*': 523, 'V*?': 66521853, 'Pe*': 1142192, 'HB*': 72803426, 'Y*O': 497654,
                   'Ae*': 1044693, 'Em*': 72803622, 'Be*': 812800, 'BS*': 5848, 'RG*': 66619666, 'AB*': 523,
                   'C*': 130019, 'S*': 1153392, 'sg*': 193599, 's*r': 5898, 's*y': 1142197, 's*b': 1048372,
                   'HS*': 54231557, 'pA*': 66619774, 'WD*': 5871, 'ZZ*': 136562, 'LM*': 12795622, 'BD*': 101600,
                   'N*': 4202, 'OH*': 2007502, 'CH*': 1142192, 'pr*': 1062509, 'TT*': 6232, 'WR*': 6251, 'PM*': 2247863,
                   'HV*': 1036344, 'V*': 6243, 'Ir*': 1141054, 'Or*': 1352333, 'RI*': 71965844, 'Er*': 1362543,
                   'Fl*': 285400, 'FU*': 957044, 'RC*': 920941, 'RC?': 1362543, 'Ro*': 2168098, 'a2*': 1141942,
                   'Psr': 4360, 'BY*': 797219, 'RS*': 1392913, 'Pu*': 353834, 'RR*': 726242, 'Ce*': 188593,
                   'dS*': 836976, 'RV*': 727379, 'WV*': 936076, 'bC*': 764463, 'cC*': 10451997, 'gD*': 1493194,
                   'SX*': 24319, 'LP*': 1153690, 'Mi*': 744691, 'sr*': 1054411, 'SN*': 3937, 'su*': 3132741,
                   'Pl?': 18611609, 'Pl': 44559, 'G': 318, 'GiC': 318, 'BiC': 1151284, 'GiG': 318, 'GiP': 318,
                   'HzG': 318, 'ALS': 318, 'LyA': 318, 'DLA': 5212927, 'mAL': 318, 'LLS': 318, 'BAL': 318, 'rG': 217012,
                   'H2G': 318, 'LSB': 115518, 'AG?': 318, 'Q?': 318, 'Bz?': 318, 'BL?': 318, 'EmG': 72802508,
                   'SBG': 726611, 'bCG': 318, 'AGN': 46587, 'LIN': 2557101, 'SyG': 213930, 'Sy1': 71965429,
                   'Sy2': 71965638, 'Bla': 221221, 'BLL': 195385, 'OVV': 7073158, 'QSO': 83373}
        result = []
        for row in rows:
            for column in row:
                if re.search('p\\d+$', column) and row[column] != '':
                    snak = None
                    if row[column] in mapping:
                        snak = self.create_snak(column.upper(), 'Q' + str(mapping[row[column]]))
                    elif column == 'p397':
                        if (parent_id := self.api_search('haswbstatement:"P3083=' + row[column] + '"')) is None:
                            if (parent_id := self.sync(row[column])) == '':
                                continue

                        if row['parent_type'] in ['As*', 'Cl*', 'ClG', 'Cld', 'DNe', 'G', 'HII',
                                                  'LSB', 'MGr', 'MoC', 'OpC', 'PaG', 'PN']:
                            snak = self.create_snak('P361', parent_id)
                        else:
                            snak = self.create_snak('P397', parent_id)
                    elif column == 'p215':
                        snak = self.create_snak('P215', row[column].replace(' ', ''))
                    else:
                        if column == 'p2216' and row['p2216t'] != 'v':
                            continue
                        if column + 'h' not in row:
                            try:
                                snak = self.create_snak(column.upper(), SimbadDAP.format_figure(row, column))
                            except InvalidOperation:
                                continue
                        else:
                            try:
                                high = SimbadDAP.format_figure(row, column + 'h')
                                low = SimbadDAP.format_figure(row, column + 'l')
                                figure = SimbadDAP.format_figure(row, column)
                                snak = self.create_snak(column.upper(), figure, low, high)
                            except InvalidOperation:
                                continue

                    if snak is not None:
                        if column + 'u' in row:
                            snak['datavalue']['value']['unit'] = 'http://www.wikidata.org/entity/' + row[column + 'u']
                        if column + 'r' in row and row[column + 'r'] != '':
                            if ads_bibcode := re.search('bibcode=(\\d{4}[\\dA-Za-z.&]+)', row[column + 'r']):
                                row[column + 'r'] = ads_bibcode.group(1)
                            if row[column + 'r'] in self.ads_articles:
                                snak['source'] = [self.ads_articles[row[column + 'r']]]
                        snak['mespos'] = row['mespos']
                        result.append(snak)
        return result

    def get_min_position(self, entity, property_id):
        result = 999
        if property_id in entity['claims']:
            for claim in entity['claims'][property_id]:
                if 'rank' not in claim or claim['rank'] == 'normal':
                    result = 0
                elif claim['rank'] == 'deprecated' and 'mespos' in claim['mainsnak']:
                    if result > int(claim['mainsnak']['mespos']):
                        result = int(claim['mainsnak']['mespos'])
        return result


if sys.argv[0].endswith(os.path.basename(__file__)):  # if not imported
    wd = SimbadDAP(sys.argv[1], sys.argv[2])
    wd_items = wd.get_all_items('SELECT DISTINCT ?id ?item {?item wdt:P3083 ?id; wdt:P31/wdt:P279* wd:Q44559}')
    # wd_items= {}
    # wd_items['SDSS J003906.37+250601.3'] = None
    for simbad_id in wd_items:
        # simbad_id = 'HD 89744b'
        if wd_items[simbad_id] is not None:
            info = json.loads(wd.api_call('wbgetentities', {'props': 'claims|info|labels', 'ids': wd_items[simbad_id]}))
            if 'entities' not in info:
                continue
            item = info['entities'][wd_items[simbad_id]]
        else:
            # continue  # uncomment if we do not want to create new items
            item = {}
        wd.sync(simbad_id, item)
