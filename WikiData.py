#!/usr/bin/python3
import abc
import csv
import json
import re
import sys
import time
import uuid
from contextlib import closing
from datetime import datetime
from decimal import Decimal, DecimalException

import requests
from requests.structures import CaseInsensitiveDict


class WikiData(abc.ABC):
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
                    result[line[0]] = process(line[1:], result[line[0]] if line[0] in result else [])
        return result

    def __init__(self, login, password):
        self.login = login
        self.password = password
        self.logon()
        self.types = WikiData.query('SELECT ?prop ?type { ?prop wikibase:propertyType ?type }')
        for prop in self.types:
            self.types[prop] = self.types[prop]. \
                replace('http://wikiba.se/ontology#', ''). \
                replace('WikibaseItem', 'wikibase-item'). \
                replace('ExternalId', 'external-id'). \
                lower()

    def api_call(self, action, params):
        # noinspection PyCompatibility
        return self.api.post('https://www.wikidata.org/w/api.php',
                             data={**params, 'format': 'json', 'action': action}).content.decode('utf-8')

    def logon(self):
        self.api = requests.Session()
        self.api.headers.update({'User-Agent': WikiData.USER_AGENT})
        token = json.loads(self.api_call('query', {'meta': 'tokens', 'type': 'login'}))['query']['tokens']['logintoken']
        self.api_call('login', {'lgname': self.login, 'lgpassword': self.password, 'lgtoken': token})
        self.token = 'bad token'

    @staticmethod
    def format_float(figure, digits='-1'):
        if 0 <= int(digits) < 20:
            return ('{0:.' + digits + 'f}').format(Decimal(figure))
        else:
            return str(Decimal(figure))

    @staticmethod
    def fix_error(figure):
        if re.search('999+\\d$', figure):
            return WikiData.format_float(
                9999999999 - Decimal(WikiData.fix_error(WikiData.format_float(9999999999 - Decimal(figure)))))
        else:
            return WikiData.format_float(re.sub('000+\\d$', '', figure))

    pubs = None

    def create_snak(self, property_id, value):
        """"Create snak based on value in format: https://wikidata.org/wiki/Help:QuickStatements#Add_simple_statement"""
        if property_id.upper() not in self.types or value == '' or value == 'NaN':
            return None

        snak = {'datatype': self.types[property_id.upper()], 'property': property_id.upper(), 'snaktype': 'value',
                'datavalue': {'value': str(value).strip(), 'type': self.types[property_id.upper()]}}
        if snak['datatype'] == 'quantity':
            parts = snak['datavalue']['value'].split('U')
            bounds = list(filter(None, re.split('[\\[,\\]]', parts[0])))
            snak['datavalue']['value'] = {
                'amount': bounds[0],
                'unit': 'http://www.wikidata.org/entity/Q' + parts[1] if len(parts) == 2 else '1'
            }
            if len(bounds) > 1:
                try:
                    min_bound = Decimal(self.fix_error(bounds[1]))
                    amount = Decimal(bounds[0])
                    max_bound = Decimal(self.fix_error(bounds[2])) if len(bounds) == 3 else min_bound

                    if max_bound != Decimal('Infinity'):
                        if max_bound > 0 and min_bound > 0:
                            if min_bound < amount < max_bound:
                                snak['datavalue']['value']['lowerBound'] = self.format_float(min_bound)
                                snak['datavalue']['value']['upperBound'] = self.format_float(max_bound)
                            else:
                                if amount > max_bound:
                                    snak['datavalue']['value']['lowerBound'] = self.format_float(amount - min_bound)
                                    snak['datavalue']['value']['upperBound'] = self.format_float(amount + max_bound)
                except (ValueError, DecimalException, KeyError):
                    pass  # ignore if nothing/garbage comes in min/max fields
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

    def get_claim(self, entity, snak):
        if snak is None:
            return None

        if snak['property'] in entity['claims']:
            for candidate in entity['claims'][snak['property']]:
                if 'datavalue' not in candidate['mainsnak']:
                    continue
                if isinstance(snak['datavalue']['value'], str):
                    if candidate['mainsnak']['datavalue']['value'] == snak['datavalue']['value']:
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

    def mark_reference(self, claim, db_id=None):
        if 'references' not in claim:
            claim['references'] = []
        already_exists = False
        for ref in list(claim['references']):
            if 'P248' in ref['snaks']:
                if ref['snaks']['P248'][0]['datavalue']['value']['id'] == self.db_ref:
                    already_exists = True  # correct reference already exists
                    if 'P813' in ref['snaks']:  # update "retrieved" timestamp if one already exists
                        ref['snaks']['P813'] = [self.create_snak('P813', datetime.now().strftime('%d/%m/%Y'))]
            elif 'P143' in ref['snaks']:  # Doesn't make sense to keep "imported from" if real source exists
                claim['references'].remove(ref)
                continue
            elif 'P4656' in ref['snaks']:  # Doesn't make sense to keep "imported from" if real source exists
                claim['references'].remove(ref)
                continue

            if self.db_property in ref['snaks']:
                ref_id = ref['snaks'][self.db_property][0]['datavalue']['value']
                if ref_id.replace(' ', '_').lower() == db_id.replace(' ', '_').lower():
                    del ref['snaks'][self.db_property]
        if not already_exists:
            claim['references'].append({'snaks': {'P248': [self.create_snak('P248', self.db_ref)]}})

    def update(self, entity, input_data):
        referenced_statements = {}
        for property_id, value in input_data:
            if re.search('^p\\d+$', property_id, re.IGNORECASE):
                claim = self.get_claim(entity, self.create_snak(property_id, value))
                if claim:
                    if property_id not in referenced_statements:
                        referenced_statements[property_id] = []  # save all claims that are referenced from YV DB
                        if property_id.upper() in entity['claims']:
                            for statement in entity['claims'][property_id.upper()]:
                                if 'references' in statement:
                                    for ref in statement['references']:
                                        if 'P248' in ref['snaks']:
                                            if ref['snaks']['P248'][0]['datavalue']['value']['id'] == self.db_ref:
                                                referenced_statements[property_id].append(statement)
                                                break

                    if claim in referenced_statements[property_id]:
                        referenced_statements[property_id].remove(claim)
                    if claim['mainsnak']['datatype'] != 'external-id':
                        self.mark_reference(claim,
                                            entity['claims'][self.db_property][0]['mainsnak']['datavalue']['value'])

        for property_id in referenced_statements:
            for claim in referenced_statements[property_id]:
                if len(claim['references']) > 1:
                    for ref in claim['references']:
                        if 'P248' in ref['snaks']:
                            if ref['snaks']['P248'][0]['datavalue']['value']['id'] == self.db_ref:
                                claim['references'].remove(ref)
                                continue
                else:
                    claim['remove'] = 1

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
                if 'error' not in response.lower() or 'editconflict' in response.lower():
                    time.sleep(0.5)
                    if 'nochange' not in response.lower():
                        self.trace(json.loads(response)['entity'], 'modified' if 'id' in entity else 'created')
                    return

                if 'badtoken' in response.lower():
                    self.token = json.loads(self.api_call('query', {'meta': 'tokens'}))['query']['tokens']['csrftoken']
                    continue

                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ':' + response, file=sys.stderr)
            except requests.exceptions.RequestException:
                pass
            time.sleep(10)
            self.logon()  # just in case - re-authenticate

    def trace(self, entity, message):
        print('https://www.wikidata.org/wiki/' + entity['id'] + '\t' + message)

    @abc.abstractmethod
    def get_summary(self, entity):
        pass
