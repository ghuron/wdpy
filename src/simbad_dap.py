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
        # coo_err_angle, coo_err_maj, coo_err_maj_prec, coo_err_min, coo_err_min_prec, coo_qual, coo_wavelength,
        # hpx, morph_qual, nbref, oid, plx_qual, pm_err_angle, pm_qual, sp_qual, update_date,
        # rvz_bibcode, rvz_err, rvz_err_prec, rvz_nature, rvz_qual, rvz_radvel, rvz_radvel_prec,
        # rvz_redshift, rvz_redshift_prec, rvz_type,
        # vlsr, vlsr_bibcode, vlsr_max, vlsr_min, vlsr_wavelength
        self.simbad = [  # p: precision, h: +error, l: -error, u: unit, r: reference
            {'query': '''SELECT main_id, otype AS P31, morph_type AS P223, morph_bibcode AS P223r, 
    ra AS P6257, ra_prec AS P6257p, 'Q28390' AS P6257u, coo_bibcode AS P6257r, sp_type AS P215, sp_bibcode AS P215r,
    dec AS P6258, dec_prec AS P6258p, 'Q28390' AS P6258u, coo_bibcode AS P6258r, 
    plx_value AS P2214, plx_prec AS P2214p, plx_err AS P2214h, plx_err_prec AS P2214hp, 
    plx_err AS P2214l, plx_err_prec AS P2214lp, 'Q21500224' AS P2214u, plx_bibcode AS P2214r,
    pmra AS P10752, pmra_prec AS P10752p, pm_err_maj AS P10752h, pm_err_maj_prec AS P10752hp,
    pm_err_maj AS P10752l, pm_err_maj_prec AS P10752lp, 'Q22137107' AS P10752u, pm_bibcode AS P10752r,
    pmdec AS P10751, pmdec_prec AS P10751p, pm_err_min AS P10751h, pm_err_min_prec AS P10751hp,
    pm_err_min AS P10751l, pm_err_min_prec AS P10751lp, 'Q22137107' AS P10751u, pm_bibcode AS P10751r
                           FROM basic WHERE {}''', 'cache': {}},
            {'query': '''SELECT id, main_id AS P397, link_bibcode AS P397r, otype AS parent_type
                         FROM (SELECT main_id AS id, oid FROM basic WHERE {} ORDER BY oid) b
                         JOIN h_link ON h_link.child = b.oid JOIN basic s ON h_link.parent = s.oid''', 'cache': {}},
            {'query': '''SELECT id, otype AS P31, origin 
                         FROM (SELECT main_id AS id, oid FROM basic WHERE {} ORDER BY oid) b
                         JOIN otypes ON oidref = oid''', 'cache': {}}
        ]

    def get_summary(self, entity):
        return 'batch import from [[Q654724|SIMBAD]] for object ' + \
               entity['claims']['P3083'][0]['mainsnak']['datavalue']['value']

    def get_chunk_from_search(self, offset):
        return self.simbad[0] if offset == 0 else []  # .keys()

    def obtain_claim(self, entity, snak):
        claim = super().obtain_claim(entity, snak)
        if snak is not None and snak['property'] in ['P6257', 'P6258']:
            if snak['property'] in entity['claims']:
                for candidate in entity['claims'][snak['property']]:
                    if claim != candidate:
                        candidate['remove'] = 1
            epoch = self.obtain_claim(entity, self.create_snak('P6259', 'Q1264450'))  # J2000
            epoch['references'] = []
            self.add_refs(epoch, [self.db_ref])
        if 'default_rank' in snak and 'rank' not in claim:
            claim['rank'] = snak['default_rank']
        return claim

    def load(self, condition):
        for table in self.simbad:
            table['cache'] = table['cache'] | self.tap_query('https://simbad.u-strasbg.fr/simbad/sim-tap',
                                                             table['query'].format(condition), lambda n, r: r + [n])

    def transform_to_snak(self, identifier):
        if identifier not in self.simbad[0]['cache']:
            self.load('main_id = \'' + identifier + '\'')  # attempt to load this specific object
            if identifier not in self.simbad[0]['cache']:
                return None
        result = []
        for table in self.simbad:
            if identifier in table['cache']:
                result = result + self.parse_page(table['cache'][identifier])
        return result

    def post_process(self, entity):
        super().post_process(entity)
        if 'P59' not in entity['claims'] and 'P6257' in entity['claims'] and 'P6258' in entity['claims']:
            p = coord.SkyCoord(entity['claims']['P6257'][0]['mainsnak']['datavalue']['value']['amount'],
                               entity['claims']['P6258'][0]['mainsnak']['datavalue']['value']['amount'], frame='icrs',
                               unit='deg')
            const = self.constellations[p.get_constellation(short_name=True)]
            self.obtain_claim(entity, self.create_snak('P59', const if isinstance(const, str) else const[0]))
        for property_id in ['P10751', 'P10752']:
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
    def tap_query(url, sql, process=lambda new, existing: new):
        result = {}
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
                    result[object_id] = process(row, result[object_id] if object_id in result else [])
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
                            parent = {}
                            if parent_data := self.transform_to_snak(row[column]):
                                self.sync(parent, parent_data, row[column])
                                parent_id = self.save(parent)
                            else:
                                continue

                        if row['parent_type'] in ['As*', 'Cl*', 'ClG', 'Cld', 'DNe', 'G', 'HII',
                                                  'LSB', 'MGr', 'MoC', 'OpC', 'PaG', 'PN']:
                            snak = self.create_snak('P361', parent_id)
                        else:
                            snak = self.create_snak('P397', parent_id)
                    elif column == 'p215':
                        snak = self.create_snak('P215', row[column].replace(' ', ''))
                    else:
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
                        if column.upper() == 'P31':
                            if 'origin' in row:  # from otypes table
                                if ads_bibcode := re.search('bibcode=(\\d{4}[\\dA-Za-z.&]+)', row['origin']):
                                    if ads_bibcode.group(1) in self.ads_articles:
                                        snak['source'] = [self.ads_articles[ads_bibcode.group(1)]]
                                snak['default_rank'] = 'deprecated'
                            else:  # from basic
                                snak['default_rank'] = 'normal'
                        if column + 'r' in row and row[column + 'r'] != '':
                            if row[column + 'r'] in self.ads_articles:
                                snak['source'] = [self.ads_articles[row[column + 'r']]]
                        result.append(snak)
        return result


if sys.argv[0].endswith(os.path.basename(__file__)):  # if not imported
    wd = SimbadDAP(sys.argv[1], sys.argv[2])
    wd.load('''otype IN ('Pl', 'Pl?')''')
    wd_items = wd.get_all_items('SELECT DISTINCT ?id ?item {?item wdt:P3083 ?id; wdt:P31/wdt:P279* wd:Q44559}')
    for simbad_id in wd_items:
        # simbad_id = 'TOI-1259b'
        item = {}
        if wd_items[simbad_id] is not None:
            info = json.loads(wd.api_call('wbgetentities', {'props': 'claims|info|labels', 'ids': wd_items[simbad_id]}))
            if 'entities' not in info:
                continue
            item = info['entities'][wd_items[simbad_id]]
        # else:
        #     continue  # uncomment if we do not want to create new items

        if data := wd.transform_to_snak(simbad_id):
            wd.sync(item, data, simbad_id)
            wd.save(item)
        else:
            wd.trace(item, 'was not updated because corresponding simbad page was not parsed')
