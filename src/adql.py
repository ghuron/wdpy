import csv
import logging
import re
from contextlib import closing
from decimal import InvalidOperation
from urllib.parse import unquote

from astropy import coordinates

from wd import Wikidata, Model, Element


class ADQL(Element):
    def obtain_claim(self, snak):
        if claim := super().obtain_claim(snak):
            if 'mespos' in snak and ('mespos' not in claim or int(claim['mespos']) > int(snak['mespos'])):
                claim['mespos'] = snak['mespos']
            if snak['property'] == 'P1215':
                if 'qualifiers' in snak and 'P1227' in snak['qualifiers'] and snak['qualifiers']['P1227'] == 'Q4892529':
                    claim['rank'] = 'preferred'  # V-magnitude is always preferred
        return claim

    dataset = {}

    @classmethod
    def load(cls, condition=None):
        for lines in cls.config('queries'):
            query = ''.join(lines)
            if condition:
                query = 'SELECT * FROM ({}) a WHERE {}'.format(query, condition)  # condition uses "final" column names
            ADQL.tap_query(cls.config('endpoint'), query, ADQL.dataset)

    @classmethod
    def prepare_data(cls, external_id) -> []:
        input_snaks = super().prepare_data(external_id)
        if (external_id not in cls.dataset) and cls.config('endpoint'):
            cls.get_next_chunk(external_id)  # attempt to load this specific object
        if external_id in cls.dataset:
            for row in cls.dataset[external_id]:
                for col in row:
                    if row[col] and re.search('\\d+$', col) and (snak := cls.construct_snak(row, col)):
                        input_snaks.append(snak)
        else:
            logging.warning('{}:"{}"\tcould not be extracted'.format(cls.db_property, external_id))
            input_snaks = None
        return input_snaks

    def remove_all_but_one(self, property_id):
        if property_id not in ['P528']:
            super().remove_all_but_one(property_id)

    __const = None

    def post_process(self):
        super().post_process()
        if 'P6257' in self.entity['claims'] and 'datavalue' in self.entity['claims']['P6257'][0]['mainsnak']:
            if 'P6258' in self.entity['claims'] and 'datavalue' in self.entity['claims']['P6258'][0]['mainsnak']:
                self.obtain_claim(self.create_snak('P6259', 'Q1264450'))  # J2000
                if 'P59' not in self.entity['claims']:
                    ra = self.entity['claims']['P6257'][0]['mainsnak']['datavalue']['value']['amount']
                    dec = self.entity['claims']['P6258'][0]['mainsnak']['datavalue']['value']['amount']
                    tla = coordinates.SkyCoord(ra, dec, frame='icrs', unit='deg').get_constellation(short_name=True)
                    if ADQL.__const is None:
                        ADQL.__const = Wikidata.query(
                            'SELECT DISTINCT ?n ?i {?i wdt:P31/wdt:P279* wd:Q8928; wdt:P1813 ?n}')
                    self.obtain_claim(self.create_snak('P59', ADQL.__const[tla]))

    @staticmethod
    def tap_query(url, sql, result=None):
        if response := Wikidata.request(url + '/sync', data={'request': 'doQuery', 'lang': 'adql', 'format': 'csv',
                                                             'maxrec': -1, 'query': sql}, stream=True):
            with closing(response) as r:
                reader = csv.reader(r.iter_lines(decode_unicode='utf-8'), delimiter=',', quotechar='"')
                header = next(reader)
                result = {} if result is None else result
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
    def format_figure(row, col):
        return Model.format_float(row[col], int(row[col + 'p']) if col + 'p' in row and row[col + 'p'] != '' else -1)

    _parents = None

    @staticmethod
    def get_parent_object(name: str):
        if ADQL._parents is None:
            ADQL._parents = Wikidata.query('SELECT DISTINCT ?c ?i { ?i ^ps:P397 []; wdt:P528 ?c }',
                                           lambda row, _: (row[0].lower(), row[1]))

        name = name[:-1] if re.search('OGLE.+L$', name) else name  # In SIMBAD OGLE names are w/o trailing 'L'
        if name.lower() in ADQL._parents:
            return ADQL._parents[name.lower()]

        from simbad_dap import SimbadDAP
        if simbad_id := SimbadDAP.get_id_by_name(name):
            if simbad_id.lower() not in ADQL._parents:
                if (qid := SimbadDAP.get_by_id(simbad_id)) is None:
                    return
                ADQL._parents[simbad_id.lower()] = qid
            ADQL._parents[name.lower()] = ADQL._parents[simbad_id.lower()]
            logging.info('Cache miss: "{}" for {}'.format(name, ADQL._parents[name.lower()]))
            return ADQL._parents[name.lower()]

    @classmethod
    def construct_snak(cls, row, col, new_col=None):
        new_col = (new_col if new_col else col).upper()
        if Wikidata.type_of(new_col) != 'quantity':
            if col == 'p397' and (qid := ADQL.get_parent_object(row[col])):
                row[col] = qid
            result = cls.create_snak(new_col, row[col])
        elif col + 'h' not in row or row[col + 'h'] == '':
            result = cls.create_snak(new_col, cls.format_figure(row, col))
        else:
            try:
                high = cls.format_figure(row, col + 'h')
                low = cls.format_figure(row, col + 'l')
                figure = cls.format_figure(row, col)
                result = cls.create_snak(new_col, figure, low, high)
            except InvalidOperation:
                return

        if result is not None:
            if 'mespos' in row:
                result['mespos'] = row['mespos']
            if col + 'u' in row and (unit := cls.lut(row[col + 'u'])):
                result['datavalue']['value']['unit'] = 'http://www.wikidata.org/entity/' + unit
            reference = row[col + 'r'] if col + 'r' in row and row[col + 'r'] else None
            reference = row['reference'] if 'reference' in row and row['reference'] else reference
            if reference and (ref_id := ADQL.parse_url(re.sub('.*(http\\S+).*', '\\g<1>', reference))):
                result['source'] = [ref_id] if 'source' not in result else result['source'] + [ref_id]
        return cls.enrich_qualifier(result, row['qualifier'] if 'qualifier' in row else row[col])

    @classmethod
    def enrich_qualifier(cls, snak, value):
        if (not snak) or (not cls.config(snak['property'].upper(), 'id')):
            return snak
        for pattern in (config := cls.config(snak['property'].upper()))['translate']:
            if value.startswith(pattern):
                return {**snak, 'qualifiers': {config['id']: config['translate'][pattern]}}

    @staticmethod
    def parse_url(url: str) -> str:
        from ads import ADS
        from arxiv import ArXiv

        """Try to find qid of the reference based on the url provided"""
        if url and url.strip() and (url := url.split()[0]):  # get text before first whitespace and strip
            for pattern, repl in ADQL.config('transform').items():
                if (query := unquote(re.sub(pattern, repl, url, flags=re.S))).startswith('P'):
                    if query.startswith('P818=') and (qid := ArXiv.get_by_id(query.replace('P818=', ''))):
                        return qid
                    if query.startswith('P819=') and (qid := ADS.get_by_id(query.replace('P819=', ''))):
                        return qid
                    try:  # fallback
                        return Wikidata.search('haswbstatement:' + query)
                    except ValueError as e:
                        logging.warning('Found {} instances of {}'.format(e.args[0], query))


ADQL.initialize(__file__)
