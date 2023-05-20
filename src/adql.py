import csv
import logging
import re
from abc import ABC
from contextlib import closing
from decimal import InvalidOperation
from urllib.parse import unquote

import requests
from astropy import coordinates

from ads import ADS
from arxiv import ArXiv
from wikidata import WikiData


class ADQL(WikiData, ABC):
    config = WikiData.load_config(__file__)

    def obtain_claim(self, snak):
        claim = super().obtain_claim(snak)
        if claim and 'mespos' in snak:
            claim['mespos'] = snak['mespos']
        return claim

    cache = {}

    @classmethod
    def load(cls, condition=None):
        for lines in cls.config['queries']:
            query = ''.join(lines)
            if condition:
                query = 'SELECT * FROM ({}) a WHERE {}'.format(query, condition)  # condition uses "final" column names
            ADQL.tap_query(cls.config['endpoint'], query, ADQL.cache)

    def prepare_data(self, source=None):
        super().prepare_data()
        if self.external_id not in self.cache and 'endpoint' in self.config:
            self.get_next_chunk(self.external_id)  # attempt to load this specific object
        if self.external_id in self.cache:
            for row in self.cache[self.external_id]:
                for col in row:
                    if row[col] and re.search('\\d+$', col) and (snak := self.construct_snak(row, col)):
                        self.input_snaks.append(snak)

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
                        ADQL.__const = ADQL.query('SELECT DISTINCT ?n ?i {?i wdt:P31/wdt:P279* wd:Q8928; wdt:P1813 ?n}')
                    self.obtain_claim(WikiData.create_snak('P59', ADQL.__const[tla]))

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
                        if (item := WikiData.load_items([ref_id])) and ref_id in item:
                            if 'redirects' in (entity := item[ref_id]):
                                ADQL.__redirects[ref_id] = entity['redirects']['to']
                                ref_id = entity['redirects']['to']
                            if 'claims' in entity and 'P577' in entity['claims']:
                                p577 = entity['claims']['P577'][0]['mainsnak']['datavalue']['value']
                        ADQL.__pub_dates[ref_id] = int(WikiData.serialize_value(p577)) if p577 else None
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
                        val1 = claim1['mainsnak']['datavalue']['value']
                        val2 = claim2['mainsnak']['datavalue']['value']
                        if ADQL.serialize_value(val2, val1) == ADQL.serialize_value(val1):
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
        result = {} if result is None else result
        try:
            with closing(requests.post(url + '/sync', data={'request': 'doQuery', 'lang': 'adql', 'format': 'csv',
                                                            'maxrec': -1, 'query': sql}, stream=True)) as r:
                if r.status_code >= 500:
                    logging.error('Connecting {} error {}'.format(url, r.status_code))
                    return result
                elif r.status_code != 200:
                    logging.error('Query {}'.format(sql))
                    return result
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
        except requests.exceptions.ConnectionError:
            logging.error('Error while retrieving results of the query: ' + sql)
        return result

    @staticmethod
    def format_figure(row, col):
        return WikiData.format_float(row[col], int(row[col + 'p']) if col + 'p' in row and row[col + 'p'] != '' else -1)

    def construct_snak(self, row, col, new_col=None):
        from simbad_dap import SimbadDAP

        new_col = (new_col if new_col else col).upper()
        if WikiData.get_type(new_col) != 'quantity':
            if col == 'p397' and (qid := SimbadDAP.get_by_any_id(row[col])):
                row[col] = qid
            return self.create_snak(new_col, row[col])
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
            if col + 'u' in row:
                result['datavalue']['value']['unit'] = 'http://www.wikidata.org/entity/' + row[col + 'u']
            reference = row[col + 'r'] if col + 'r' in row and row[col + 'r'] else None
            reference = row['reference'] if 'reference' in row and row['reference'] else reference
            if reference and (ref_id := ADQL.parse_url(re.sub('.*(http\\S+).*', '\\g<1>', reference))):
                result['source'] = [ref_id] if 'source' not in result else result['source'] + [ref_id]

        return result

    @staticmethod
    def parse_url(url: str) -> str:
        """Try to find qid of the reference based on the url provided"""
        if url and url.strip() and (url := url.split()[0]):  # get text before first whitespace and strip
            for pattern, repl in ADQL.config['transform'].items():
                if (query := unquote(re.sub(pattern, repl, url, flags=re.S))).startswith('P'):
                    if query.startswith('P818=') and (qid := ArXiv.get_by_id(query.replace('P818=', ''))):
                        return qid
                    if query.startswith('P819=') and (qid := ADS.get_by_id(query.replace('P819=', ''))):
                        return qid
                    try:  # fallback
                        return WikiData.api_search('haswbstatement:' + query)
                    except ValueError as e:
                        logging.warning('Found {} instances of {}'.format(e.args[0], query))
