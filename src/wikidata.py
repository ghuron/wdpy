#!/usr/bin/python3
import csv
import json
import logging
import math
import os
import re
import sys
import time
import uuid
from abc import ABC, abstractmethod
from contextlib import closing
from decimal import Decimal, DecimalException
from typing import Tuple

import requests
from requests.structures import CaseInsensitiveDict


class WikiData(ABC):
    USER_AGENT = 'automated import by https://www.wikidata.org/wiki/User:Ghuron'
    api = requests.Session()
    api.headers.update({'User-Agent': USER_AGENT})
    db_property, db_ref = None, None
    login, password, token = '', '', 'bad'
    __types: dict[str, str] = None
    config = {}
    logging.basicConfig(format="%(asctime)s: %(levelname)s - %(message)s", stream=sys.stdout,
                        level=os.environ.get('LOGLEVEL', 'INFO').upper())

    @staticmethod
    def load_config(file_name: str):
        try:
            with open(os.path.splitext(file_name)[0] + '.json') as file:
                WikiData.config = {**WikiData.config, **json.load(file)}
        except OSError:
            return

    @staticmethod
    def api_call(action: str, params: dict[str, str]) -> dict:
        """Wikidata API call with JSON format, see https://wikidata.org/w/api.php"""
        WD_API = 'https://www.wikidata.org/w/api.php'
        try:
            return WikiData.api.post(WD_API, data={**params, 'format': 'json', 'action': action}).json()
        except json.decoder.JSONDecodeError:
            logging.error('Cannot decode {} response for {}'.format(action, params))
        except (requests.exceptions.ConnectionError, requests.exceptions.RequestException):
            logging.error('Connection error while calling {} for {}'.format(action, params))

    @staticmethod
    def logon(login: str = None, password: str = None):
        """Wikidata logon, see https://wikidata.org/w/api.php?action=help&modules=login
        and store credentials for future use. Performs wikidata re-logon if called subsequently without parameters.
        All further API calls will be performed on behalf on logged user"""
        WikiData.login = login if login else WikiData.login
        WikiData.password = password if password else WikiData.password
        token = WikiData.api_call('query', {'meta': 'tokens', 'type': 'login'})['query']['tokens']['logintoken']
        WikiData.api_call('login', {'lgtoken': token, 'lgname': WikiData.login, 'lgpassword': WikiData.password})

    @staticmethod
    def load_items(ids: list[str]):
        """Load up to 50 wikidata entities, returns None in case of error"""
        if len(ids) > 0:
            result = WikiData.api_call('wbgetentities', {'props': 'claims|info|labels', 'ids': '|'.join(ids)})
            return result['entities'] if result is not None and 'entities' in result else None

    @staticmethod
    def api_search(query: str):
        """CirrusSearch query, returns only if one element found, warns otherwise"""
        if (response := WikiData.api_call('query', {'list': 'search', 'srsearch': query})) and 'query' in response:
            if len(response['query']['search']) > 1:
                logging.warning(query + ' returned ' + str(len(response['query']['search'])) + ' results')
            elif len(response['query']['search']) == 0:
                logging.info(query + ' not found')
            else:
                return response['query']['search'][0]['title']

    @staticmethod
    def query(query: str, process=lambda new, existing: new[0]):
        result = CaseInsensitiveDict()
        with requests.Session() as session:
            session.headers.update({'Accept': 'text/csv', 'User-Agent': WikiData.USER_AGENT})
            with closing(session.post('https://query.wikidata.org/sparql', params={'query': query}, stream=True)) as r:
                try:
                    reader = csv.reader(r.iter_lines(decode_unicode='utf-8'), delimiter=',', quotechar='"')
                    next(reader)
                    for line in reader:
                        line = [item.replace('http://www.wikidata.org/entity/', '') for item in line]
                        if len(line) > 1:
                            result[line[0]] = process(line[1:], result[line[0]] if line[0] in result else [])
                except requests.exceptions.ChunkedEncodingError:
                    logging.error('Error while executing ' + query)
        return result

    @staticmethod
    def get_next_chunk(offset: any) -> Tuple[list[str], any]:
        """Fetch array of external identifiers starting from specified offset"""
        return [], None

    @classmethod
    def get_all_items(cls, sparql: str, process=lambda new, existing: new[0]):
        results = WikiData.query(sparql, process)
        offset = None
        while True:
            chunk, offset = cls.get_next_chunk(offset)
            if len(chunk) == 0:
                break
            for external_id in chunk:
                if external_id not in results:
                    results[external_id] = None
        return results

    @staticmethod
    def format_float(figure, digits: int = -1):
        """Raises: DecimalException"""
        formatter = '{:f}'
        if 0 <= digits < 24:
            if math.fabs(Decimal(figure)) >= 1:  # adding number of digits before .
                digits += 1 + int(math.log10(math.fabs(Decimal(figure))))
            formatter = '{:.' + str(digits) + '}'
        return formatter.format(Decimal(figure).normalize())

    @staticmethod
    def fix_error(figure: str) -> str:
        if re.search('999+\\d$', figure):
            n = Decimal(999999999999999999999999)
            return WikiData.format_float(n - Decimal(WikiData.fix_error(WikiData.format_float(n - Decimal(figure)))))
        else:
            return WikiData.format_float(re.sub('^000+\\d$', '', figure))

    @staticmethod
    def get_type(property_id: str) -> str:
        """Lazy load and read-only access to type of the property"""
        if WikiData.__types is None:
            WikiData.__types = WikiData.query('SELECT ?prop ?type { ?prop wikibase:propertyType ?type }')
            for prop in WikiData.__types:
                WikiData.__types[prop] = WikiData.__types[prop].replace('http://wikiba.se/ontology#', ''). \
                    replace('WikibaseItem', 'wikibase-item').replace('ExternalId', 'external-id').lower()
        if property_id in WikiData.__types:
            return WikiData.__types[property_id]

    @staticmethod
    def create_snak(property_id: str, value, lower: str = None, upper: str = None):
        """Create snak based on provided id of the property and string value"""
        if not WikiData.get_type(property_id) or not value or value == 'NaN':
            return None

        snak = {'datatype': WikiData.get_type(property_id), 'property': property_id, 'snaktype': 'value',
                'datavalue': {'value': value, 'type': WikiData.get_type(property_id)}}
        if snak['datatype'] == 'quantity':
            try:
                snak['datavalue']['value'] = {'amount': WikiData.format_float(value), 'unit': '1'}
                if upper is not None and lower is not None:
                    min_bound = Decimal(WikiData.fix_error(lower))
                    if min_bound < 0:
                        min_bound = -min_bound
                    amount = Decimal(value)
                    max_bound = Decimal(WikiData.fix_error(upper))

                    if min_bound > 0 or max_bound > 0:  # +/- 0 can be skipped
                        if max_bound != Decimal('Infinity'):
                            snak['datavalue']['value']['lowerBound'] = WikiData.format_float(amount - min_bound)
                            snak['datavalue']['value']['upperBound'] = WikiData.format_float(amount + max_bound)
            except (ValueError, DecimalException, KeyError):
                return None
        elif snak['datatype'] == 'wikibase-item':
            text = snak['datavalue']['value']
            if WikiData.config and 'translate' in WikiData.config and text in WikiData.config['translate']:
                text = WikiData.config['translate'][text]
            if not re.search('Q\\d+$', text):
                return
            snak['datavalue'] = {'type': 'wikibase-entityid', 'value': {'entity-type': 'item', 'id': text}}
        elif snak['datatype'] == 'time':
            if not (value := WikiData.parse_date(snak['datavalue']['value'])):
                return
            snak['datavalue']['value'] = value
        elif snak['datatype'] == 'external-id':
            snak['datavalue']['type'] = 'string'
        return snak

    @staticmethod
    def parse_date(i: str):
        for p in ['(?P<d>\\d\\d?)/(?P<m>\\d\\d?)/(?P<y>\\d{4})', '(?P<y>\\d{4})-?(?P<m>\\d\\d?)?-?(?P<d>\\d\\d?)?']:
            if (m := re.search(p, i)) and (g := m.groupdict('0')):
                return {'before': 0, 'after': 0, 'calendarmodel': 'http://www.wikidata.org/entity/Q1985727',
                        'time': '+{}-{:02d}-{:02d}T00:00:00Z'.format(int(g['y']), int(g['m']), int(g['d'])),
                        'precision': 9 if g['m'] == '0' else 10 if g['d'] == '0' else 11, 'timezone': 0}

    @staticmethod
    def find_claim(value: dict, claims: list):
        for candidate_claim in claims:
            if 'datavalue' not in candidate_claim['mainsnak']:
                continue
            candidate = candidate_claim['mainsnak']['datavalue']['value']
            if isinstance(value, str):
                if candidate == value:
                    return candidate_claim
            elif 'id' in value:
                if candidate['id'] == value['id']:
                    return candidate_claim
            elif 'time' in value:
                if candidate['precision'] == value['precision']:
                    if candidate['time'] == value['time']:
                        return candidate_claim
            elif 'amount' in value and Decimal(candidate['amount']) == Decimal(value['amount']):  # ToDo: units + tests
                if 'lowerBound' not in candidate and 'lowerBound' not in value:
                    return candidate_claim
                if 'lowerBound' in candidate and 'lowerBound' in value:
                    if Decimal(candidate['lowerBound']) == Decimal(value['lowerBound']):
                        if Decimal(candidate['upperBound']) == Decimal(value['upperBound']):
                            return candidate_claim

    def __init__(self, external_id: str, qid: str = None):
        self.external_id = external_id
        self.qid = qid
        self.entity, self.input_snaks = None, None

    @abstractmethod
    def prepare_data(self, source=None) -> None:
        """Load self.entity using self.qid and prepare self.input_snaks by parsing source using self.external_id"""
        if self.qid and (result := WikiData.load_items([self.qid])):
            self.entity = result[self.qid]
        self.input_snaks = [WikiData.create_snak(self.db_property, self.external_id)]

    def obtain_claim(self, snak: dict):
        """Find or create claim, corresponding to the provided snak"""
        if snak is None:
            return
        if isinstance(snak['datavalue']['value'], dict) and 'id' in snak['datavalue']['value']:
            if self.qid == snak['datavalue']['value']['id']:
                return

        self.entity = {} if self.entity is None else self.entity
        self.entity['claims'] = {} if 'claims' not in self.entity else self.entity['claims']
        if snak['property'] not in self.entity['claims']:
            self.entity['claims'][snak['property']] = []

        if claim := WikiData.find_claim(snak['datavalue']['value'], self.entity['claims'][snak['property']]):
            if 'rank' in claim and claim['rank'] == 'deprecated':
                if 'qualifiers' in claim and 'P2241' in claim['qualifiers']:
                    return  # someone explicitly states that this is a bad statement, do not touch it
        else:
            claim = {'type': 'statement', 'mainsnak': snak}
            if 'id' in self.entity:
                claim['id'] = self.entity['id'] + '$' + str(uuid.uuid4())
            self.entity['claims'][snak['property']].append(claim)

        if 'qualifiers' in snak:
            claim['qualifiers'] = {} if 'qualifiers' not in claim else claim['qualifiers']
            for property_id in snak['qualifiers']:
                claim['qualifiers'][property_id] = [self.create_snak(property_id, snak['qualifiers'][property_id])]
        return claim

    def process_own_reference(self, only_default_sources: bool, ref: dict = None) -> dict:
        if ref is None:
            ref = {'snaks': {'P248': [self.create_snak('P248', self.db_ref)]}}
        if self.db_property in ref['snaks']:
            if self.db_property in self.entity['claims'] or not only_default_sources:
                ref['snaks'].pop(self.db_property, 0)
        elif self.db_property not in self.entity['claims'] and only_default_sources:
            ref['snaks'][self.db_property] = [self.create_snak(self.db_property, self.external_id)]
        # if 'P813' in ref['snaks']:  # update "retrieved" timestamp if one already exists
        #     ref['snaks']['P813'] = [self.create_snak('P813', datetime.now().strftime('%d/%m/%Y'))]
        return ref

    def add_refs(self, claim: dict, references: list = None):
        references = [] if references is None else references
        if 'references' not in claim:
            claim['references'] = []
        default_ref_exists = False
        only_default_sources = (len(references) == 0)
        for ref in list(claim['references']):
            if 'P248' in ref['snaks']:
                if ref['snaks']['P248'][0]['datavalue']['value']['id'] == self.db_ref:
                    default_ref_exists = True
                    self.process_own_reference(only_default_sources, ref)
                elif ref['snaks']['P248'][0]['datavalue']['value']['id'] in references:
                    references.remove(ref['snaks']['P248'][0]['datavalue']['value']['id'])
            ref['snaks'].pop('P143', 0)  # Doesn't make sense to keep "imported from" if real source exists
            ref['snaks'].pop('P4656', 0)
            if len(ref['snaks']) == 0:
                claim['references'].remove(ref)

        if not default_ref_exists:
            claim['references'].append(self.process_own_reference(only_default_sources))
        for ref in references:
            claim['references'].append({'snaks': {'P248': [WikiData.create_snak('P248', ref)]}})

    def filter_by_ref(self, unfiltered: list):
        filtered = []
        for statement in unfiltered:
            if 'references' in statement:
                for ref in statement['references']:
                    if 'P248' in ref['snaks'] and ref['snaks']['P248'][0]['datavalue']['value']['id'] == self.db_ref:
                        filtered.append(statement)
                        break
        return filtered

    def post_process(self):
        """Changes in self.entity that does not depend on specific input"""
        self.entity['labels'] = {} if 'labels' not in self.entity else self.entity['labels']
        if 'en' not in self.entity['labels']:
            self.entity['labels']['en'] = {'value': self.external_id, 'language': 'en'}

    def trace(self, message: str, level=20):
        # CRITICAL: 50, ERROR: 40, WARNING: 30, INFO: 20, DEBUG: 10
        logging.log(level, 'https://www.wikidata.org/wiki/' + self.qid + '\t' + message if self.qid else message)

    def get_summary(self):
        return 'batch import from [[' + self.db_ref + ']] for object ' + self.external_id

    def save(self):
        data = {'maxlag': '15', 'data': json.dumps(self.entity), 'summary': self.get_summary()}
        if 'id' in self.entity:
            data['id'] = self.entity['id']
            data['baserevid'] = self.entity['lastrevid']
        else:
            data['new'] = 'item'

        for retries in range(1, 3):
            data['token'] = WikiData.token
            if response := WikiData.api_call('wbeditentity', data):
                if 'error' not in response:
                    time.sleep(0.5)
                    if 'nochange' not in response['entity']:
                        self.entity = response['entity']
                        self.trace('modified' if 'id' in data else 'created')
                        return self.entity['id']
                    return  # no change
                if response['error']['code'] == 'badtoken':
                    WikiData.token = WikiData.api_call('query', {'meta': 'tokens'})['query']['tokens']['csrftoken']
                    continue
                self.trace('error while saving: ' + response['error']['info'], 40)
            time.sleep(10)
            self.logon()  # just in case - re-authenticate
        return ''

    def update(self):
        if self.input_snaks is None:
            return

        self.entity = {} if self.entity is None else self.entity
        original = json.dumps(self.entity)
        self.entity['claims'] = {} if 'claims' not in self.entity else self.entity['claims']

        affected_statements = {}
        for snak in self.input_snaks:
            claim = self.obtain_claim(snak)
            if claim:
                if snak['property'] not in affected_statements and snak['property'] in self.entity['claims']:
                    affected_statements[snak['property']] = self.filter_by_ref(self.entity['claims'][snak['property']])
                if claim in affected_statements[snak['property']]:
                    affected_statements[snak['property']].remove(claim)
                if claim['mainsnak']['datatype'] != 'external-id':
                    self.add_refs(claim, snak['source'] if 'source' in snak else None)

        for property_id in affected_statements:
            for claim in affected_statements[property_id]:
                if len(claim['references']) > 1:
                    for ref in claim['references']:
                        if 'P248' in ref['snaks']:
                            if ref['snaks']['P248'][0]['datavalue']['value']['id'] == self.db_ref:
                                claim['references'].remove(ref)
                                continue
                else:  # there is always P248:db_ref, so if there are no other references -> delete statement
                    claim['remove'] = 1

        self.post_process()
        if json.dumps(self.entity) != original:
            return self.save()

    @classmethod
    def get_by_id(cls, external_id: str, create=True):
        """Attempt to find qid by external_id or create it"""
        if qid := WikiData.api_search('haswbstatement:"{}={}"'.format(cls.db_property, external_id)):
            return qid
        if create:
            instance = cls(external_id)
            instance.prepare_data()
            return instance.update()
