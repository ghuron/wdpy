import csv
import logging
import re
from abc import ABC
from contextlib import closing
from decimal import InvalidOperation
from urllib.parse import unquote

import dateutil.parser
import requests
from astropy import coordinates

from ads import ADS
from arxiv import ArXiv
from wikidata import WikiData


class ADQL(WikiData, ABC):
    WikiData.load_config(__file__)
    constellations = None
    ads_articles = None
    publication_dates = None

    def obtain_claim(self, snak):
        claim = super().obtain_claim(snak)
        if claim and 'mespos' in snak:
            claim['mespos'] = snak['mespos']
        return claim

    def post_process(self):
        super().post_process()
        if 'P59' not in self.entity['claims'] and 'P6257' in self.entity['claims'] and 'P6258' in self.entity['claims']:
            ra = self.entity['claims']['P6257'][0]['mainsnak']['datavalue']['value']['amount']
            dec = self.entity['claims']['P6258'][0]['mainsnak']['datavalue']['value']['amount']
            tla = coordinates.SkyCoord(ra, dec, frame='icrs', unit='deg').get_constellation(short_name=True)
            if ADQL.constellations is None:
                ADQL.constellations = WikiData.query(
                    'SELECT DISTINCT ?n ?i {?i wdt:P31/wdt:P279* wd:Q8928; wdt:P1813 ?n}')
            self.obtain_claim(WikiData.create_snak('P59', ADQL.constellations[tla]))
        for property_id in self.entity['claims']:
            if property_id not in ADQL.config['noranking'] and WikiData.get_type(property_id) == 'quantity':
                ADQL.normalize(self.entity['claims'][property_id])

    @staticmethod
    def get_latest_publication_date(claim):
        if not ADQL.publication_dates:
            ADQL.publication_dates = WikiData.query(
                'select ?item ?date { ?item wdt:P819 []; OPTIONAL { ?item wdt:P577 ?date }}')
            ADQL.publication_dates['Q66617668'] = '1924-01-01T00:00:00Z'

        latest = dateutil.parser.parse('1800-01-01T00:00:00Z')
        if 'references' in claim:
            for ref in claim['references']:
                if 'P248' not in ref['snaks']:
                    continue
                if ref['snaks']['P248'][0]['datavalue']['value']['id'] in ADQL.publication_dates:
                    text = ADQL.publication_dates[ref['snaks']['P248'][0]['datavalue']['value']['id']]
                    if text and dateutil.parser.parse(text) > latest:
                        latest = dateutil.parser.parse(text)
                else:
                    if ref['snaks']['P248'][0]['datavalue']['value']['id'] == 'Q654724':
                        latest = dateutil.parser.parse('1800-01-02T00:00:00Z')
        return latest

    @staticmethod
    def normalize(statements):
        minimal = 999999
        for statement in statements:
            if 'rank' in statement and statement['rank'] == 'preferred':
                return  # do not change any ranks

            if 'mespos' in statement and minimal > int(statement['mespos']):
                minimal = int(statement['mespos'])

        latest = dateutil.parser.parse('1800-01-01T00:00:00Z')
        for statement in statements:
            if 'mespos' in statement:  # normal for new statements with minimal mespos, deprecated for the rest
                if int(statement['mespos']) == minimal:
                    statement['rank'] = 'normal'
                elif 'hash' not in statement['mainsnak']:  # newly created statement
                    statement['rank'] = 'deprecated'

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
        except requests.exceptions.ConnectionError:
            logging.error('Error while retrieving results of the query: ' + sql)
        return result

    @staticmethod
    def format_figure(row, col):
        return WikiData.format_float(row[col], int(row[col + 'p']) if col + 'p' in row and row[col + 'p'] != '' else -1)

    @staticmethod
    def construct_snak(row, col):
        if WikiData.get_type(col.upper()) != 'quantity':
            result = WikiData.create_snak(col.upper(), row[col])
        elif col + 'h' not in row or row[col + 'h'] == '':
            result = WikiData.create_snak(col.upper(), ADQL.format_figure(row, col))
        else:
            try:
                high = ADQL.format_figure(row, col + 'h')
                low = ADQL.format_figure(row, col + 'l')
                figure = ADQL.format_figure(row, col)
                result = WikiData.create_snak(col.upper(), figure, low, high)
            except InvalidOperation:
                return

        if result is not None:
            result['mespos'] = row['mespos']
            if col + 'u' in row:
                result['datavalue']['value']['unit'] = 'http://www.wikidata.org/entity/' + row[col + 'u']
            if col + 'r' in row and row[col + 'r'] != '':
                if ref_id := ADQL.parse_url(re.sub('.*(http\\S+).*', '\\g<1>', row[col + 'r'])):
                    result['source'] = [] if 'source' not in result else result['source']
                    result['source'].append(ref_id)

        return result

    @staticmethod
    def parse_url(url: str) -> str:
        """Try to find qid of the reference based on the url provided"""
        if url and url.strip() and (url := url.split()[0]):  # get text before first whitespace and strip
            for pattern, repl in ADQL.config['transform'].items():
                if (query := unquote(re.sub(pattern, repl, url, flags=re.S))).startswith('P'):
                    if query.startswith('P818='):
                        if qid := ArXiv.get_by_id(query.replace('P818=', '')):
                            return qid
                    if query.startswith('P819='):
                        if qid := ADS.get_by_id(query.replace('P819=', '')):
                            return qid
                    elif qid := WikiData.api_search('haswbstatement:' + query):  # fallback
                        return qid
