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
    db_ref = None
    db_property = None

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

    def __init__(self, login, password):
        self.api = requests.Session()
        self.api.headers.update({'User-Agent': WikiData.USER_AGENT})

        self.login = login
        self.password = password
        if login != '':
            self.logon()

        self.token = 'bad token'
        self.types = WikiData.query('SELECT ?prop ?type { ?prop wikibase:propertyType ?type }')
        for prop in self.types:
            self.types[prop] = self.types[prop].replace('http://wikiba.se/ontology#', ''). \
                replace('WikibaseItem', 'wikibase-item').replace('ExternalId', 'external-id').lower()

    def api_call(self, action, params):
        return self.api.post('https://www.wikidata.org/w/api.php',
                             data={**params, 'format': 'json', 'action': action}).content.decode('utf-8')

    def logon(self):
        token = json.loads(self.api_call('query', {'meta': 'tokens', 'type': 'login'}))['query']['tokens']['logintoken']
        self.api_call('login', {'lgname': self.login, 'lgpassword': self.password, 'lgtoken': token})

    def api_search(self, query):
        response = json.loads(self.api_call('query', {'list': 'search', 'srsearch': query}))
        if 'query' in response:
            if len(response['query']['search']) != 1:
                print(query + ' returned ' + str(len(response['query']['search'])) + ' results')
            if len(response['query']['search']) > 0:
                return response['query']['search'][0]['title']

    def create_snak(self, property_id, value, lower=None, upper=None):
        if property_id not in self.types or value == '' or value == 'NaN':
            return None

        snak = {'datatype': self.types[property_id], 'property': property_id, 'snaktype': 'value',
                'datavalue': {'value': value, 'type': self.types[property_id]}}
        if snak['datatype'] == 'quantity':
            try:
                snak['datavalue']['value'] = {'amount': self.format_float(value), 'unit': '1'}
                if upper is not None and lower is not None:
                    min_bound = Decimal(self.fix_error(lower))
                    if min_bound < 0:
                        min_bound = -min_bound
                    amount = Decimal(value)
                    max_bound = Decimal(self.fix_error(upper))

                    if min_bound > 0 or max_bound > 0:  # +/- 0 can be skipped
                        if max_bound != Decimal('Infinity'):
                            snak['datavalue']['value']['lowerBound'] = self.format_float(amount - min_bound)
                            snak['datavalue']['value']['upperBound'] = self.format_float(amount + max_bound)
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

    def obtain_claim(self, entity, snak):
        if snak is None:
            return

        entity = {} if entity is None else entity
        entity['claims'] = {} if 'claims' not in entity else entity['claims']

        if snak['property'] in entity['claims']:
            for candidate in entity['claims'][snak['property']]:
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
        if 'id' in entity:
            new_claim['id'] = entity['id'] + '$' + str(uuid.uuid4())
        if snak['property'] not in entity['claims']:
            entity['claims'][snak['property']] = []
        entity['claims'][snak['property']].append(new_claim)
        return new_claim

    def add_refs(self, claim, references=None, foreign_id=None):
        references = [] if references is None else references
        if 'references' not in claim:
            claim['references'] = []
        default_ref_exists = False
        for ref in list(claim['references']):
            if 'P248' in ref['snaks']:
                if ref['snaks']['P248'][0]['datavalue']['value']['id'] == self.db_ref:
                    # if 'P813' in ref['snaks']:  # update "retrieved" timestamp if one already exists
                    #     ref['snaks']['P813'] = [self.create_snak('P813', datetime.now().strftime('%d/%m/%Y'))]
                    if foreign_id is not None:
                        ref['snaks'][self.db_property] = [self.create_snak(self.db_property, foreign_id)]
                    default_ref_exists = True
                elif ref['snaks']['P248'][0]['datavalue']['value']['id'] in references:
                    references.remove(ref['snaks']['P248'][0]['datavalue']['value']['id'])
            elif 'P143' in ref['snaks'] or 'P4656' in ref['snaks']:
                # Doesn't make sense to keep "imported from" if real source exists
                claim['references'].remove(ref)

        if not default_ref_exists:
            ref = {'snaks': {'P248': [self.create_snak('P248', self.db_ref)]}}
            if foreign_id is not None:
                ref['snaks'][self.db_property] = [self.create_snak(self.db_property, foreign_id)]
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

    def update(self, input_snaks, entity=None):
        if input_snaks is None:
            external_id = entity['claims'][self.db_property][0]['mainsnak']['datavalue']['value']
            self.trace(entity, 'error while retrieving/parsing {}:"{}"'.format(self.db_property, external_id))
            return

        entity = {} if entity is None else entity
        original = json.dumps(entity)
        entity['claims'] = {} if 'claims' not in entity else entity['claims']

        affected_statements = {}
        for snak in input_snaks:
            claim = self.obtain_claim(entity, snak)
            if claim:
                if snak['property'] not in affected_statements and snak['property'] in entity['claims']:
                    affected_statements[snak['property']] = self.filter_by_ref(entity['claims'][snak['property']])
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
        self.post_process(entity)
        if json.dumps(entity) != original:
            return self.save(entity)

    def post_process(self, entity):
        if 'labels' not in entity:
            entity['labels'] = {}
        if 'en' not in entity['labels']:
            entity['labels']['en'] = {'value': entity['claims'][self.db_property][0]['mainsnak']['datavalue']['value'],
                                      'language': 'en'}

    def get_snaks(self, external_id):
        return [self.create_snak(self.db_property, external_id)]

    def sync(self, external_id, qid=None):
        return self.update(self.get_snaks(external_id), self.get_items(qid))

    def get_items(self, qid):
        try:
            if qid is not None:
                info = json.loads(self.api_call('wbgetentities', {'props': 'claims|info|labels', 'ids': qid}))
                if 'entities' in info:
                    return info['entities'][qid]
        except json.decoder.JSONDecodeError:
            print('Cannot decode wbgetentities response')
        except requests.exceptions.ConnectionError:
            print('Connection error while calling wbgetentities')

    def trace(self, entity, message):
        print('https://www.wikidata.org/wiki/' + entity['id'] + '\t' + message)

    def get_summary(self, entity):
        return 'batch import from [[' + self.db_ref + ']] for object ' + \
               entity['claims'][self.db_property][0]['mainsnak']['datavalue']['value']

    def save(self, entity):
        data = {'maxlag': '15', 'data': json.dumps(entity), 'summary': self.get_summary(entity)}
        if 'id' in entity:
            data['id'] = entity['id']
            data['baserevid'] = entity['lastrevid']
        else:
            data['new'] = 'item'

        for retries in range(1, 3):
            try:
                data['token'] = self.token
                response = self.api_call('wbeditentity', data)
                if 'error' not in response.lower() or 'editconflict' in response.lower():  # succesful save
                    time.sleep(0.5)
                    if 'nochange' not in response.lower():
                        self.trace(json.loads(response)['entity'], 'modified' if 'id' in entity else 'created')
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

    def get_next_chunk(self):
        return []

    def get_all_items(self, sparql, process=lambda new, existing: new[0]):
        results = self.query(sparql, process)
        while True:
            chunk = self.get_next_chunk()
            if len(chunk) == 0:
                break
            for external_id in chunk:
                if external_id not in results:
                    results[external_id] = None
        return results
