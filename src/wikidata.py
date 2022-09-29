#!/usr/bin/python3
from abc import ABC
import csv
import json
import re
import sys
import time
import uuid
from contextlib import closing
from _datetime import datetime
from decimal import Decimal, DecimalException

import requests
from requests.structures import CaseInsensitiveDict


class WikiData(ABC):
    USER_AGENT = 'automated import by https://www.wikidata.org/wiki/User:Ghuron)'
    api = requests.Session()
    api.headers.update({'User-Agent': USER_AGENT})
    login = None
    password = None
    token = 'bad token'
    types = None

    @staticmethod
    def api_call(action, params):
        return WikiData.api.post('https://www.wikidata.org/w/api.php',
                                 data={**params, 'format': 'json', 'action': action}).content.decode('utf-8')

    @staticmethod
    def logon(login=None, password=None):
        WikiData.login = login if login is not None else WikiData.login
        WikiData.password = password if password is not None else WikiData.password
        t = json.loads(WikiData.api_call('query', {'meta': 'tokens', 'type': 'login'}))['query']['tokens']['logintoken']
        WikiData.api_call('login', {'lgname': WikiData.login, 'lgpassword': WikiData.password, 'lgtoken': t})

    @staticmethod
    def api_search(query):
        response = json.loads(WikiData.api_call('query', {'list': 'search', 'srsearch': query}))
        if 'query' in response:
            if len(response['query']['search']) != 1:
                print(query + ' returned ' + str(len(response['query']['search'])) + ' results')
            if len(response['query']['search']) > 0:
                return response['query']['search'][0]['title']

    @staticmethod
    def query(query, process=lambda new, existing: new[0]):
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
    def get_all_items(cls, sparql, process=lambda new, existing: new[0]):
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
    def format_float(figure, digits=-1):
        if 0 <= digits < 20:
            return ('{0:.' + str(digits) + 'f}').format(Decimal(figure))
        if amount := re.search('(?P<mantissa>\\d\\.\\d+)e-(?P<exponent>\\d+)', str(figure)):
            return WikiData.format_float(figure, len(amount.group('mantissa')) + int(amount.group('exponent')) - 2)
        return str(Decimal(figure))

    @staticmethod
    def fix_error(figure):
        if re.search('999+\\d$', figure):
            n = Decimal(999999999999999999999999)
            return WikiData.format_float(n - Decimal(WikiData.fix_error(WikiData.format_float(n - Decimal(figure)))))
        else:
            return WikiData.format_float(re.sub('^000+\\d$', '', figure))

    @staticmethod
    def create_snak(property_id, value, lower=None, upper=None):
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

    def __init__(self, external_id, property_id, ref_id):
        self.external_id = external_id
        self.entity = None
        self.db_property = property_id
        self.db_ref = ref_id

    @staticmethod
    def load_items(ids):
        if len(ids) == 0:
            return []
        try:
            response = WikiData.api_call('wbgetentities', {'props': 'claims|info|labels', 'ids': '|'.join(ids)})
            return json.loads(response)['entities']
        except json.decoder.JSONDecodeError:
            print('Cannot decode wbgetentities response for entities ' + '|'.join(ids))
        except requests.exceptions.ConnectionError:
            print('Connection error while calling wbgetentities for entities ' + '|'.join(ids))

    def get_item(self, qid):
        self.entity = WikiData.load_items([qid])[qid]
        return self.entity

    def get_snaks(self):
        return [self.create_snak(self.db_property, self.external_id)]

    def obtain_claim(self, snak):
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
        if snak['property'] not in self.entity['claims']:
            self.entity['claims'][snak['property']] = []
        self.entity['claims'][snak['property']].append(new_claim)
        return new_claim

    def add_refs(self, claim, references=None):
        references = [] if references is None else references
        if 'references' not in claim:
            claim['references'] = []
        default_ref_exists = False
        for ref in list(claim['references']):
            if 'P248' in ref['snaks']:
                if ref['snaks']['P248'][0]['datavalue']['value']['id'] == self.db_ref:
                    # if 'P813' in ref['snaks']:  # update "retrieved" timestamp if one already exists
                    #     ref['snaks']['P813'] = [self.create_snak('P813', datetime.now().strftime('%d/%m/%Y'))]
                    if self.db_property not in self.entity['claims']:
                        ref['snaks'][self.db_property] = [self.create_snak(self.db_property, self.external_id)]
                    default_ref_exists = True
                elif ref['snaks']['P248'][0]['datavalue']['value']['id'] in references:
                    references.remove(ref['snaks']['P248'][0]['datavalue']['value']['id'])
            elif 'P143' in ref['snaks'] or 'P4656' in ref['snaks']:
                # Doesn't make sense to keep "imported from" if real source exists
                claim['references'].remove(ref)

        if not default_ref_exists:
            ref = {'snaks': {'P248': [self.create_snak('P248', self.db_ref)]}}
            if self.db_property not in self.entity['claims']:
                ref['snaks'][self.db_property] = [self.create_snak(self.db_property, self.external_id)]
            claim['references'].append(ref)
        for ref in references:
            claim['references'].append({'snaks': {'P248': [self.create_snak('P248', ref)]}})

    def filter_by_ref(self, unfiltered):
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
            self.entity['labels']['en'] = {
                'value': self.entity['claims'][self.db_property][0]['mainsnak']['datavalue']['value'], 'language': 'en'}

    def trace(self, message):
        print('https://www.wikidata.org/wiki/' + self.entity['id'] + '\t' + message)

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
                data['token'] = self.token
                response = self.api_call('wbeditentity', data)
                if 'error' not in response.lower() or 'editconflict' in response.lower():  # succesful save
                    time.sleep(0.5)
                    if 'nochange' not in response.lower():
                        self.entity = json.loads(response)['entity']
                        self.trace('modified' if 'id' in self.entity else 'created')
                        return json.loads(response)['entity']['id']
                    return None

                if 'badtoken' in response.lower():
                    self.token = json.loads(self.api_call('query', {'meta': 'tokens'}))['query']['tokens']['csrftoken']
                    continue

                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ':' + response, file=sys.stderr)
            except requests.exceptions.RequestException:
                pass
            time.sleep(10)
            self.logon()  # just in case - re-authenticate
        return ''

    def update(self, input_snaks):
        if input_snaks is None:
            self.trace('error while retrieving/parsing {}:"{}"'.format(self.db_property, self.external_id))
            return

        self.entity = {} if self.entity is None else self.entity
        original = json.dumps(self.entity)
        self.entity['claims'] = {} if 'claims' not in self.entity else self.entity['claims']

        affected_statements = {}
        for snak in input_snaks:
            claim = self.obtain_claim(snak)
            if claim:
                if snak['property'] not in affected_statements and snak['property'] in self.entity['claims']:
                    affected_statements[snak['property']] = self.filter_by_ref(self.entity['claims'][snak['property']])
                if claim in affected_statements[snak['property']]:
                    affected_statements[snak['property']].remove(claim)
                # noinspection PyTypeChecker
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

    def sync(self, qid=None):
        self.get_item(qid)
        return self.update(self.get_snaks())
