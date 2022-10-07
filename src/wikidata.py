#!/usr/bin/python3
import csv
import json
import re
import time
import uuid
from _datetime import datetime
from abc import ABC, abstractmethod
from contextlib import closing
from decimal import Decimal, DecimalException

import requests
from requests.structures import CaseInsensitiveDict


class WikiData(ABC):
    USER_AGENT = 'automated import by https://www.wikidata.org/wiki/User:Ghuron)'
    api = requests.Session()
    api.headers.update({'User-Agent': USER_AGENT})
    db_property, db_ref = None, None
    login, password, token = '', '', 'bad'
    types: dict[str, str] = None

    @staticmethod
    def api_call(action: str, params: dict[str, str]) -> dict:
        """Wikidata API call with JSON format, see https://wikidata.org/w/api.php"""
        WD_API = 'https://www.wikidata.org/w/api.php'
        try:
            return WikiData.api.post(WD_API, data={**params, 'format': 'json', 'action': action}).json()
        except json.decoder.JSONDecodeError:
            print('Cannot decode {} response for {}'.format(action, params))
        except requests.exceptions.ConnectionError:
            print('Connection error while calling {} for {}'.format(action, params))

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
    def api_search(query: str) -> str | None:
        """CirrusSearch query, returns first found element, warns if zero or more than one found"""
        if (response := WikiData.api_call('query', {'list': 'search', 'srsearch': query})) and 'query' in response:
            if len(response['query']['search']) != 1:
                print(query + ' returned ' + str(len(response['query']['search'])) + ' results')
            if len(response['query']['search']) > 0:
                return response['query']['search'][0]['title']

    @staticmethod
    def query(query: str, process=lambda new, existing: new[0]):
        result = CaseInsensitiveDict()
        with requests.Session() as session:
            session.headers.update({'Accept': 'text/csv', 'User-Agent': WikiData.USER_AGENT})
            with closing(session.post('https://query.wikidata.org/sparql', params={'query': query}, stream=True)) as r:
                reader = csv.reader(r.iter_lines(decode_unicode='utf-8'), delimiter=',', quotechar='"')
                next(reader)
                for line in reader:
                    line = [item.replace('http://www.wikidata.org/entity/', '') for item in line]
                    if len(line) > 1:
                        result[line[0]] = process(line[1:], result[line[0]] if line[0] in result else [])
        return result

    @staticmethod
    def get_next_chunk(offset):
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
        if 0 <= digits < 20:
            return ('{0:.' + str(digits) + 'f}').format(Decimal(figure))
        if amount := re.search('(?P<mantissa>\\d\\.\\d+)e-(?P<exponent>\\d+)', str(figure)):
            return WikiData.format_float(figure, len(amount.group('mantissa')) + int(amount.group('exponent')) - 2)
        return str(Decimal(figure))

    @staticmethod
    def fix_error(figure: str) -> str:
        if re.search('999+\\d$', figure):
            n = Decimal(999999999999999999999999)
            return WikiData.format_float(n - Decimal(WikiData.fix_error(WikiData.format_float(n - Decimal(figure)))))
        else:
            return WikiData.format_float(re.sub('^000+\\d$', '', figure))

    @staticmethod
    def create_snak(property_id: str, value, lower=None, upper=None):
        if WikiData.types is None:
            WikiData.types = WikiData.query('SELECT ?prop ?type { ?prop wikibase:propertyType ?type }')
            for prop in WikiData.types:
                WikiData.types[prop] = WikiData.types[prop].replace('http://wikiba.se/ontology#', ''). \
                    replace('WikibaseItem', 'wikibase-item').replace('ExternalId', 'external-id').lower()

        if property_id not in WikiData.types or value == '' or value == 'NaN':
            return None

        snak = {'datatype': WikiData.types[property_id], 'property': property_id, 'snaktype': 'value',
                'datavalue': {'value': value, 'type': WikiData.types[property_id]}}
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
            if not re.search('^Q\\d+$', snak['datavalue']['value']):
                return None
            snak['datavalue']['type'] = 'wikibase-entityid'
            snak['datavalue']['value'] = {'entity-type': 'item', 'id': snak['datavalue']['value']}
        elif snak['datatype'] == 'time':
            snak['datavalue']['type'] = 'time'
            if len(snak['datavalue']['value']) == 4:  # year only
                snak['datavalue']['value'] = {'time': '+' + snak['datavalue']['value'] + '-00-00T00:00:00Z',
                                              'precision': 9, 'timezone': 0, 'before': 0, 'after': 0,
                                              'calendarmodel': 'http://www.wikidata.org/entity/Q1985727'}
            else:
                try:  # trying to parse date
                    snak['datavalue']['value'] = {
                        'time': datetime.strptime(snak['datavalue']['value'], '%d/%m/%Y').strftime(
                            '+%Y-%m-%dT00:00:00Z'), 'precision': 11, 'timezone': 0, 'before': 0, 'after': 0,
                        'calendarmodel': 'http://www.wikidata.org/entity/Q1985727'}
                except ValueError:
                    return None
        elif snak['datatype'] == 'external-id':
            snak['datavalue']['type'] = 'string'
        return snak

    def __init__(self, external_id: str):
        self.external_id = external_id
        self.entity = None
        self.input_snaks = None

    @staticmethod
    def load_items(ids: list) -> dict[str, dict]:
        if len(ids) == 0:
            return {}
        result = WikiData.api_call('wbgetentities', {'props': 'claims|info|labels', 'ids': '|'.join(ids)})
        return result['entities'] if result is not None and 'entities' in result else None

    @abstractmethod
    def parse_input(self, source=None):
        self.input_snaks = [self.create_snak(self.db_property, self.external_id)]

    def obtain_claim(self, snak: dict):
        if snak is None:
            return

        self.entity = {} if self.entity is None else self.entity
        self.entity['claims'] = {} if 'claims' not in self.entity else self.entity['claims']

        if snak['property'] in self.entity['claims']:
            for candidate in self.entity['claims'][snak['property']]:
                if 'datavalue' not in candidate['mainsnak']:
                    continue
                if isinstance(snak['datavalue']['value'], str):
                    if candidate['mainsnak']['datavalue']['value'] == snak['datavalue']['value']:
                        return candidate
                elif snak['datavalue']['value'] == candidate['mainsnak']['datavalue']['value']:
                    return candidate
                elif 'id' in snak['datavalue']['value']:
                    if candidate['mainsnak']['datavalue']['value']['id'] == snak['datavalue']['value']['id']:
                        return candidate
                elif 'time' in snak['datavalue']['value']:
                    value1 = candidate['mainsnak']['datavalue']['value']
                    value2 = snak['datavalue']['value']
                    if value1['precision'] == value2['precision']:
                        if value1['precision'] == 9 and value1['time'][0:5] == value2['time'][0:5]:
                            return candidate
                        if value1['time'] == value2['time'] and value1['precision'] == value2['precision']:
                            return candidate
                elif 'amount' in snak['datavalue']['value']:
                    source = candidate['mainsnak']['datavalue']['value']
                    if float(source['amount']) == float(snak['datavalue']['value']['amount']):
                        if 'lowerBound' in source and 'lowerBound' in snak['datavalue']['value']:
                            if float(source['lowerBound']) == float(snak['datavalue']['value']['lowerBound']):
                                return candidate
                        if 'lowerBound' not in snak['datavalue']['value']:
                            return candidate

        new_claim = {'type': 'statement', 'mainsnak': snak}
        if 'id' in self.entity:
            new_claim['id'] = self.entity['id'] + '$' + str(uuid.uuid4())
        if 'qualifiers' in snak:
            new_claim['qualifiers'] = {}
            for property_id in snak['qualifiers']:
                new_claim['qualifiers'][property_id] = [self.create_snak(property_id, snak['qualifiers'][property_id])]

        if snak['property'] not in self.entity['claims']:
            self.entity['claims'][snak['property']] = []
        self.entity['claims'][snak['property']].append(new_claim)
        return new_claim

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
            claim['references'].append({'snaks': {'P248': [self.create_snak('P248', ref)]}})

    def filter_by_ref(self, unfiltered: list):
        filtered = []
        for statement in unfiltered:
            if 'references' in statement:
                for ref in statement['references']:
                    if 'P248' in ref['snaks']:
                        if ref['snaks']['P248'][0]['datavalue']['value']['id'] == self.db_ref:
                            filtered.append(statement)
                            break
        return filtered

    def post_process(self):
        if 'labels' not in self.entity:
            self.entity['labels'] = {}
        if 'en' not in self.entity['labels']:
            self.entity['labels']['en'] = {'value': self.external_id, 'language': 'en'}

    def trace(self, message: str):
        if self.entity is not None and 'id' in self.entity:
            message = 'https://www.wikidata.org/wiki/' + self.entity['id'] + '\t' + message
        print(message)

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
            try:
                data['token'] = WikiData.token
                response = WikiData.api_call('wbeditentity', data)
                if 'error' in response:
                    if response['error']['code'] == 'badtoken':
                        WikiData.token = WikiData.api_call('query', {'meta': 'tokens'})['query']['tokens']['csrftoken']
                        continue
                    self.trace('error while saving: ' + response['error']['info'])
                else:
                    time.sleep(0.5)
                    if 'nochange' not in response['entity']:
                        self.trace('modified' if 'id' in data else 'created')
                        self.entity = response['entity']
                        return self.entity['id']
                    return
            except requests.exceptions.RequestException:
                pass
            time.sleep(10)
            self.logon()  # just in case - re-authenticate
        return ''

    def update(self):
        if self.input_snaks is None:
            self.trace('error while retrieving/parsing {}:"{}"'.format(self.db_property, self.external_id))
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
    def get_by_id(cls, external_id, create=True):
        instance = cls(external_id)
        if qid := WikiData.api_search('haswbstatement:"{}={}"'.format(instance.db_property, external_id)):
            return qid
        if create:
            instance.parse_input()
            return instance.update()
