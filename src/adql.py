import csv
import logging
import re
from abc import ABC
from contextlib import closing
from decimal import InvalidOperation
from urllib.parse import unquote

from astropy import coordinates

from wd import Wikidata, Element


class ADQL(Element, ABC):
    def obtain_claim(self, snak):
        if claim := super().obtain_claim(snak):
            if 'mespos' in snak:
                claim['mespos'] = snak['mespos']
            if snak['property'] == 'P1215':
                if 'qualifiers' in snak and 'P1227' in snak['qualifiers'] and snak['qualifiers']['P1227'] == 'Q4892529':
                    claim['rank'] = 'preferred'  # V-magnitude is always preferred
        return claim

    dataset = {}

    @classmethod
    def load(cls, condition=None):
        for lines in cls.config['queries']:
            query = ''.join(lines)
            if condition:
                query = 'SELECT * FROM ({}) a WHERE {}'.format(query, condition)  # condition uses "final" column names
            ADQL.tap_query(cls.config['endpoint'], query, ADQL.dataset)

    def prepare_data(self):
        input_snaks = super().prepare_data()
        if self.external_id not in self.dataset and 'endpoint' in self.config:
            self.get_next_chunk(self.external_id)  # attempt to load this specific object
        if self.external_id in self.dataset:
            for row in self.dataset[self.external_id]:
                for col in row:
                    if row[col] and re.search('\\d+$', col) and (snak := self.construct_snak(row, col)):
                        input_snaks.append(snak)
        else:
            self.trace('"{}"\tcould not be loaded, skipping update'.format(self.external_id), 30)
            input_snaks = None
        return input_snaks

    __const = None

    def post_process(self):
        super().post_process()
        for property_id in self.entity['claims']:
            if property_id in ADQL.config['normalize']:
                ADQL.normalize(self.entity['claims'][property_id])
            elif property_id in ['P6257', 'P6258']:
                self.entity['claims'][property_id] = ADQL.get_best_value(self.entity['claims'][property_id])
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
                    self.obtain_claim(Element.create_snak('P59', ADQL.__const[tla]))

    __pub_dates, __redirects = {'Q66617668': 19240101, 'Q4026990': 99999999}, {}

    @staticmethod
    def get_latest_publication_date(claim: dict):
        latest, p248 = 0, []
        if 'references' in claim:
            for ref in list(claim['references']):
                if 'P248' in ref['snaks']:
                    if (ref_id := ref['snaks']['P248'][0]['datavalue']['value']['id']) in ADQL.__redirects:
                        ref_id = ADQL.__redirects[ref_id]
                    if ref_id not in ADQL.__pub_dates:
                        p577 = None
                        if (item := Wikidata.load([ref_id])) and ref_id in item:
                            if 'redirects' in (entity := item[ref_id]):
                                ADQL.__redirects[ref_id] = entity['redirects']['to']
                                ref_id = entity['redirects']['to']
                            if 'claims' in entity and 'P577' in entity['claims']:
                                p577 = entity['claims']['P577'][0]['mainsnak']['datavalue']['value']
                        ADQL.__pub_dates[ref_id] = int(Element.serialize(p577)) if p577 else None
                    if ref_id in p248:
                        claim['references'].remove(ref)  # remove duplicates
                    else:
                        p248.append(ref_id)
                        if ADQL.__pub_dates[ref_id] and ADQL.__pub_dates[ref_id] > latest:
                            latest = ADQL.__pub_dates[ref_id]
        return latest

    @staticmethod
    def get_best_value(statements):
        latest = 0
        remaining_normal = 1  # only one statement supported by latest sources should remain existing
        for statement in statements:
            if 'datavalue' not in statement['mainsnak']:
                remaining_normal = 0
                break
            elif (current := ADQL.get_latest_publication_date(statement)) > latest:
                latest = current
        result = []
        for statement in statements:
            if 'datavalue' not in statement['mainsnak']:
                result.append(statement)
            elif latest == ADQL.get_latest_publication_date(statement) and remaining_normal == 1:
                remaining_normal = 0
                result.append(statement)
            elif 'hash' in statement['mainsnak']:
                statement['remove'] = 1
                result.append(statement)
        return result

    @staticmethod
    def deprecate_less_precise_values(statements):
        for claim1 in statements:
            if 'rank' not in claim1 or claim1['rank'] == 'normal':
                for claim2 in statements:
                    if claim1 != claim2 and ('rank' not in claim2 or claim2['rank'] == 'normal'):
                        if 'datavalue' in claim1['mainsnak'] and 'datavalue' in claim2['mainsnak']:  # novalue
                            val1 = claim1['mainsnak']['datavalue']['value']
                            val2 = claim2['mainsnak']['datavalue']['value']
                            if ADQL.serialize(val2, val1) == ADQL.serialize(val1):
                                claim1['rank'] = 'deprecated'
                                claim1['qualifiers'] = {} if 'qualifiers' not in claim1 else claim1['qualifiers']
                                claim1['qualifiers']['P2241'] = [ADQL.create_snak('P2241', 'Q42727519')]

    @staticmethod
    def normalize(statements):
        minimal = 999999
        for statement in statements:
            if 'rank' in statement and statement['rank'] == 'preferred':
                return  # do not change any ranks

            if 'mespos' in statement and minimal > int(statement['mespos']):
                minimal = int(statement['mespos'])

        for statement in statements:
            if 'mespos' in statement:  # normal for statements with minimal mespos, deprecated for the rest
                if int(statement['mespos']) == minimal:
                    statement['rank'] = 'normal'
                elif 'hash' not in statement['mainsnak'] and 'rank' not in statement:
                    statement['rank'] = 'deprecated'

        ADQL.deprecate_less_precise_values(statements)

        latest = 0
        for statement in statements:
            if 'rank' not in statement or statement['rank'] == 'normal':
                if (current := ADQL.get_latest_publication_date(statement)) > latest:
                    latest = current

        remaining_normal = 1  # only one statement supported by latest sources should remain normal
        for statement in statements:
            if 'rank' not in statement or statement['rank'] == 'normal':
                if remaining_normal == 0 or latest > ADQL.get_latest_publication_date(statement):
                    statement['rank'] = 'deprecated'
                else:
                    remaining_normal -= 1

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
        return Element.format_float(row[col], int(row[col + 'p']) if col + 'p' in row and row[col + 'p'] != '' else -1)

    def construct_snak(self, row, col, new_col=None):
        from simbad_dap import SimbadDAP

        new_col = (new_col if new_col else col).upper()
        if Wikidata.type_of(new_col) != 'quantity':
            if col == 'p397' and (qid := SimbadDAP.get_by_any_id(row[col])):
                row[col] = qid
            result = self.create_snak(new_col, row[col])
        elif col + 'h' not in row or row[col + 'h'] == '':
            result = self.create_snak(new_col, ADQL.format_figure(row, col))
        else:
            try:
                high = ADQL.format_figure(row, col + 'h')
                low = ADQL.format_figure(row, col + 'l')
                figure = ADQL.format_figure(row, col)
                result = self.create_snak(new_col, figure, low, high)
            except InvalidOperation:
                return

        if result is not None:
            if 'mespos' in row:
                result['mespos'] = row['mespos']
            if col + 'u' in row and (unit := self.convert_to_qid(row[col + 'u'])):
                result['datavalue']['value']['unit'] = 'http://www.wikidata.org/entity/' + unit
            reference = row[col + 'r'] if col + 'r' in row and row[col + 'r'] else None
            reference = row['reference'] if 'reference' in row and row['reference'] else reference
            if reference and (ref_id := ADQL.parse_url(re.sub('.*(http\\S+).*', '\\g<1>', reference))):
                result['source'] = [ref_id] if 'source' not in result else result['source'] + [ref_id]
        return self.enrich_qualifier(result, row['qualifier'] if 'qualifier' in row else row[col])

    @classmethod
    def enrich_qualifier(cls, snak, value):
        if not snak or snak['property'].upper() not in cls.config:
            return snak
        for pattern in (config := cls.config[snak['property'].upper()])['translate']:
            if value.startswith(pattern):
                snak['qualifiers'] = {config['id']: config['translate'][pattern]}
                return snak

    @staticmethod
    def parse_url(url: str) -> str:
        from ads import ADS
        from arxiv import ArXiv

        """Try to find qid of the reference based on the url provided"""
        if url and url.strip() and (url := url.split()[0]):  # get text before first whitespace and strip
            for pattern, repl in ADQL.config['transform'].items():
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
