import csv
import json
import logging
import math
import os
import re
import sys
import time
import uuid
from contextlib import closing
from datetime import datetime
from decimal import Decimal, DecimalException
from typing import Tuple

import requests


class Wikidata:
    USER_AGENT = 'automated import by https://www.wikidata.org/wiki/User:Ghuron'
    (__api := requests.Session()).headers.update({'User-Agent': USER_AGENT})
    __login, __password, __token = '', '', 'bad'
    __types: dict[str, str] = None
    logging.basicConfig(format="%(asctime)s: %(levelname)s - %(message)s", stream=sys.stdout,
                        level=os.environ.get('LOGLEVEL', 'INFO').upper())

    @staticmethod
    def request(url: str, session=requests.Session(), **kwargs):
        try:
            if len(kwargs):
                if (response := session.post(url, **kwargs)).status_code != 200:
                    logging.error('{} response: {} POST {}'.format(url, response.status_code, json.dumps(kwargs)))
                    return
            elif (response := session.get(url)).status_code != 200:
                logging.error('{} response: {}'.format(url, response.status_code))
                return
            return response
        except requests.exceptions.RequestException as e:
            logging.error('{} exception: {} POST {}'.format(url, e.__str__(), json.dumps(kwargs)))

    @staticmethod
    def call(action: str, params: dict[str, str]) -> dict:
        """Wikidata API call with JSON format, see https://wikidata.org/w/api.php"""
        if result := Wikidata.request('https://www.wikidata.org/w/api.php', Wikidata.__api,
                                      data={**params, 'format': 'json', 'action': action}):
            try:
                return result.json()
            except json.decoder.JSONDecodeError:
                logging.error('Cannot decode {} response for {}'.format(action, params))

    @staticmethod
    def logon(login: str = None, password: str = None):
        """Wikidata logon, see https://wikidata.org/w/api.php?action=help&modules=login
        and store credentials for future use. Performs wikidata re-logon if called subsequently without parameters.
        All further API calls will be performed on behalf on logged user"""
        Wikidata.__login = login if login else Wikidata.__login
        Wikidata.__password = password if password else Wikidata.__password
        token = Wikidata.call('query', {'meta': 'tokens', 'type': 'login'})['query']['tokens']['logintoken']
        Wikidata.call('login', {'lgtoken': token, 'lgname': Wikidata.__login, 'lgpassword': Wikidata.__password})

    @staticmethod
    def load(items: list[str]):
        """Load up to 50 wikidata entities, returns None in case of error"""
        if len(items) > 0:
            result = Wikidata.call('wbgetentities', {'props': 'claims|info|labels|aliases', 'ids': '|'.join(items)})
            return result['entities'] if (result is not None) and ('entities' in result) else None

    @staticmethod
    def search(query: str):
        """CirrusSearch query, :raises ValueError if more than one item found, None if nothing found, otherwise id"""
        if (response := Wikidata.call('query', {'list': 'search', 'srsearch': query})) and 'query' in response:
            if (count := len(response['query']['search'])) > 1:
                raise ValueError(count)
            return response['query']['search'][0]['title'] if count == 1 else None

    @staticmethod
    def edit(data, method):
        for retries in range(1, 3):
            if response := Wikidata.call(method, {**data, 'maxlag': '15', 'token': Wikidata.__token}):
                if 'error' not in response:
                    time.sleep(0.5)
                    return response
                if response['error']['code'] == 'badtoken':
                    Wikidata.__token = Wikidata.call('query', {'meta': 'tokens'})['query']['tokens']['csrftoken']
                    continue
                logging.error('{} response: {}'.format(method, response['error']['info']))
            time.sleep(10)
            if response and (response['error']['code'] != 'maxlag'):
                Wikidata.logon()  # just in case - re-authenticate

    @staticmethod
    def query(sparql: str, process=lambda row, result: (row[0], row[1])):
        result = None
        with requests.Session() as session:
            session.headers.update({'Accept': 'text/csv', 'User-Agent': Wikidata.USER_AGENT})
            if request := Wikidata.request('https://query.wikidata.org/sparql',
                                           session, data={'query': sparql}, stream=True):
                with closing(request) as r:
                    reader = csv.reader(r.iter_lines(decode_unicode='utf-8'), delimiter=',', quotechar='"')
                    next(reader)
                    result = {}
                    for line in reader:
                        if len(line := [item.replace('http://www.wikidata.org/entity/', '') for item in line]) > 1:
                            key, value = process(line, result)
                            result[key] = value
        return result

    @staticmethod
    def type_of(property_id: str) -> str:
        """Lazy load and read-only access to type of the property"""
        if Wikidata.__types is None:
            Wikidata.__types = Wikidata.query('SELECT ?prop ?type { ?prop wikibase:propertyType ?type }')
            for prop in Wikidata.__types:
                Wikidata.__types[prop] = Wikidata.__types[prop].replace('http://wikiba.se/ontology#', ''). \
                    replace('WikibaseItem', 'wikibase-item').replace('ExternalId', 'external-id').lower()
        return Wikidata.__types[property_id] if property_id in Wikidata.__types else None


class Model:
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
            return Model.format_float(n - Decimal(Model.fix_error(Model.format_float(n - Decimal(figure)))))
        else:
            return Model.format_float(re.sub('^000+\\d$', '', figure))

    @staticmethod
    def parse_date(i: str):
        for p in ['(?P<d>\\d\\d?)?/?(?P<m>\\d\\d?)/(?P<y>\\d{4})', '(?P<y>\\d{4})-?(?P<m>\\d\\d?)?-?(?P<d>\\d\\d?)?']:
            if (m := re.search(p, i)) and (g := m.groupdict('0')):
                try:  # validate parsed month and day
                    datetime(int(g['y']), int(g['m']) if int(g['m']) else 1, int(g['d']) if int(g['d']) else 1)
                except ValueError:
                    return
                return {'before': 0, 'after': 0, 'calendarmodel': 'http://www.wikidata.org/entity/Q1985727',
                        'time': '+{}-{:02d}-{:02d}T00:00:00Z'.format(int(g['y']), int(g['m']), int(g['d'])),
                        'precision': 11 if int(g['d']) else 10 if int(g['m']) else 9, 'timezone': 0}

    @staticmethod
    def serialize(value: dict, standard: dict = None):
        if isinstance(value, str):
            return value
        elif 'id' in (standard := standard if standard else value):
            return value['id']  # TODO: implement P279*
        elif 'amount' in standard:
            digits = -Decimal(standard['amount']).normalize().as_tuple().exponent
            result = value['unit']
            if 'lowerBound' in value and 'lowerBound' in standard:
                if digits < (bound := -Decimal(standard['lowerBound']).normalize().as_tuple().exponent):
                    digits = bound
                if digits < (bound := -Decimal(standard['upperBound']).normalize().as_tuple().exponent):
                    digits = bound
                result += '|' + str(round(Decimal(value['amount']) - Decimal(value['lowerBound']), digits))
                result += '|' + str(round(Decimal(value['upperBound']) - Decimal(value['amount']), digits))
            return result + '|' + str(round(Decimal(value['amount']), digits))
        elif 'precision' in standard and int(value['precision']) >= int(standard['precision']):
            if standard['precision'] == 9:
                return value['time'][:5] + '0000'
            elif standard['precision'] == 10:
                return value['time'][:5] + value['time'][6:8] + '00'
            elif standard['precision'] == 11:
                return value['time'][:5] + value['time'][6:8] + value['time'][9:11]
        return float('nan')  # because NaN != NaN

    @staticmethod
    def skip_statement(s: dict) -> bool:
        """ If someone explicitly states that this is a bad statement, do not touch it"""
        return 'rank' in s and s['rank'] == 'deprecated' and 'qualifiers' in s and 'P2241' in s['qualifiers']

    @staticmethod
    def create_snak(property_id: str, value, lower: str = None, upper: str = None):
        """Create snak based on provided id of the property and string value"""
        if not Wikidata.type_of(property_id) or value is None or value == '' or value == 'NaN':
            return None

        snak = {'datatype': Wikidata.type_of(property_id), 'property': property_id, 'snaktype': 'value',
                'datavalue': {'value': value, 'type': Wikidata.type_of(property_id)}}
        if snak['datatype'] == 'quantity':
            try:
                snak['datavalue']['value'] = {'amount': Model.format_float(value), 'unit': '1'}
                if upper is not None and lower is not None:
                    min_bound = Decimal(Model.fix_error(lower))
                    if min_bound < 0:
                        min_bound = -min_bound
                    amount = Decimal(value)
                    max_bound = Decimal(Model.fix_error(upper))

                    if min_bound > 0 or max_bound > 0:  # +/- 0 can be skipped
                        if max_bound != Decimal('Infinity'):
                            snak['datavalue']['value']['lowerBound'] = Model.format_float(amount - min_bound)
                            snak['datavalue']['value']['upperBound'] = Model.format_float(amount + max_bound)
            except (ValueError, DecimalException, KeyError):
                return None
        elif snak['datatype'] == 'wikibase-item':
            if not re.search('Q\\d+$', value):
                return None
            snak['datavalue'] = {'type': 'wikibase-entityid', 'value': {'entity-type': 'item', 'id': value}}
        elif snak['datatype'] == 'time':
            if not (value := Model.parse_date(snak['datavalue']['value'])):
                return
            snak['datavalue']['value'] = value
        elif snak['datatype'] == 'external-id':
            snak['datavalue']['type'] = 'string'
        return snak

    @staticmethod
    def compare(claim: dict, value: dict) -> bool:
        return 'datavalue' in claim and Model.serialize(claim['datavalue']['value']) == Model.serialize(value)

    @staticmethod
    def qualifier_filter(conditions: dict, claim: dict) -> bool:
        for property_id in (conditions['qualifiers'] if 'qualifiers' in conditions else {}):
            if 'qualifiers' not in claim or (property_id not in claim['qualifiers']):  # No property_id qualifier
                return False
            was_found = None
            for claim in claim['qualifiers'][property_id]:
                if was_found := Model.compare(claim, {'id': conditions['qualifiers'][property_id]}):
                    break
            if not was_found:  # Above loop did not find anything
                return False
        return True  # All conditions are met

    @staticmethod
    def find_claim(snak: dict, claims: list):
        for c in claims:
            if Model.qualifier_filter(snak, c) and Model.compare(c['mainsnak'], snak['datavalue']['value']):
                return c
        if len(claims) > 0 and Wikidata.type_of(claims[0]['mainsnak']['property']) == 'external-id':
            claims[0]['mainsnak']['datavalue']['value'] = snak['datavalue']['value']
            return claims[0]

    @staticmethod
    def create_claim(snak: dict, qid: str = None) -> dict:
        claim = {'type': 'statement', 'mainsnak': snak}
        if qid:
            claim['id'] = str(qid) + '$' + str(uuid.uuid4())
        return claim

    @staticmethod
    def set_id(qid, property_id, value, summary):
        claim = Model.create_claim(Model.create_snak(property_id, value), qid)
        if response := Wikidata.edit(data={'summary': summary, 'claim': json.dumps(claim)}, method='wbsetclaim'):
            logging.info('https://www.wikidata.org/wiki/{}#{} created'.format(qid, response['claim']['id']))


class Element:
    config, db_property, db_ref = {}, None, None

    @classmethod
    def initialize(cls, file_name: str) -> bool:
        try:
            with open(os.path.splitext(file_name)[0] + '.json') as file:
                cls.config = {**cls.config, **json.load(file)}
        except OSError:
            pass
        if executed := sys.argv[0].endswith(os.path.basename(file_name)):
            Wikidata.logon(sys.argv[1], sys.argv[2])
        return executed

    @classmethod
    def create_snak(cls, property_id: str, value, lower: str = None, upper: str = None):
        return Model.create_snak(property_id, cls.lut(value) if property_id == 'wikibase-item' else value, lower, upper)

    @classmethod
    def lut(cls, text: str):
        conversion = cls.config and 'translate' in cls.config and text in cls.config['translate']
        return cls.config['translate'][text] if conversion else text

    @staticmethod
    def get_next_chunk(offset: any) -> Tuple[list[str], any]:
        """Fetch array of external identifiers starting from specified offset"""
        return [], None

    @classmethod
    def get_all_items(cls, sparql: str, process=lambda row, _: (row[0], row[1])):
        results = Wikidata.query(sparql, process)
        offset = None
        while True:
            chunk, offset = cls.get_next_chunk(offset)
            if len(chunk) == 0:
                break
            for external_id in chunk:
                if external_id not in results:
                    results[external_id] = None
        return results

    def __init__(self, external_id: str, qid: str = None):
        self.external_id, self.qid, self._entity = external_id, qid, None

    @property
    def entity(self):
        if not self._entity:
            self.qid = self.qid if self.qid else self.haswbstatement(self.external_id)
            self._entity = {'labels': {}, 'claims': {}}
            if self.qid and (result := Wikidata.load([self.qid])):
                self._entity = result[self.qid]
        return self._entity

    def trace(self, message: str, level=20):
        # CRITICAL: 50, ERROR: 40, WARNING: 30, INFO: 20, DEBUG: 10
        LOG = 'https://www.wikidata.org/wiki/{}#{}\t{}'
        logging.log(level, LOG.format(self.qid, self.db_property, message) if self.qid else message)

    @classmethod
    def prepare_data(cls, external_id: str) -> []:
        return [cls.create_snak(cls.db_property, external_id)]

    def obtain_claim(self, snak: dict):
        """Find or create claim, corresponding to the provided snak"""
        if snak is None:
            return
        if isinstance(snak['datavalue']['value'], dict) and 'id' in snak['datavalue']['value']:
            if self.qid == snak['datavalue']['value']['id']:
                return

        if snak['property'] not in self.entity['claims']:
            self.entity['claims'][snak['property']] = []

        if claim := Model.find_claim(snak, self.entity['claims'][snak['property']]):
            if Model.skip_statement(claim):
                return
        else:
            self.entity['claims'][snak['property']].append(claim := Model.create_claim(snak, self.qid))

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

    def add_refs(self, claim: dict, references: set):
        default_ref_exists = False
        only_default_sources = (len(references) == 0)
        claim['references'] = [] if 'references' not in claim else claim['references']
        for exiting_ref in list(claim['references']):
            if 'P248' in exiting_ref['snaks']:
                if (ref_id := exiting_ref['snaks']['P248'][0]['datavalue']['value']['id']) == self.db_ref:
                    default_ref_exists = True
                    self.process_own_reference(only_default_sources, exiting_ref)
                elif ref_id in references:
                    references.remove(ref_id)
            exiting_ref['snaks'].pop('P143', 0)  # Doesn't make sense to keep "imported from" if real source exists
            exiting_ref['snaks'].pop('P4656', 0)
            if len(exiting_ref['snaks']) == 0:
                claim['references'].remove(exiting_ref)

        if not default_ref_exists:
            claim['references'].append(self.process_own_reference(only_default_sources))
        for ref in references:
            claim['references'].append({'snaks': {'P248': [self.create_snak('P248', ref)]}})

    def filter_by_ref(self, unfiltered: list):
        filtered = []
        for statement in unfiltered:
            if 'references' in statement and not Model.skip_statement(statement):
                for ref in statement['references']:
                    if 'P248' in ref['snaks'] and ref['snaks']['P248'][0]['datavalue']['value']['id'] == self.db_ref:
                        filtered.append(statement)
                        break
        return filtered

    def post_process(self):
        """Changes in self.entity that does not depend on specific input"""
        if 'en' not in self.entity['labels']:
            self.entity['labels']['en'] = {'value': self.external_id, 'language': 'en'}

    def get_summary(self):
        return 'batch import from [[' + self.db_ref + ']] for object ' + self.external_id

    def save(self):
        if 'labels' in self.entity and 'ak' in self.entity['labels']:
            self.entity['labels'].pop('ak')
        if 'aliases' in self.entity and 'ak' in self.entity['aliases']:
            self.entity['aliases'].pop('ak')

        data = {'data': json.dumps(self.entity), 'summary': self.get_summary()}
        if 'id' in self.entity:
            data['id'] = self.entity['id']
            data['baserevid'] = self.entity['lastrevid']
        else:
            data['new'] = 'item'

        if response := Wikidata.edit(data, 'wbeditentity'):
            if 'nochange' not in response['entity']:
                self._entity, self.qid = response['entity'], response['entity']['id']
                self.trace('modified' if 'id' in data else 'created')
                return self.qid
            # else:  # Too many
            #     self.trace('no change while saving {}'.format(data['data']))

    def update(self, parsed_data):
        if parsed_data is None:
            return

        original = json.dumps(self.entity)
        affected_statements = {}
        for snak in parsed_data:
            if claim := self.obtain_claim(snak):
                if snak['property'] not in affected_statements and snak['property'] in self.entity['claims']:
                    affected_statements[snak['property']] = self.filter_by_ref(self.entity['claims'][snak['property']])
                if claim in affected_statements[snak['property']]:
                    affected_statements[snak['property']].remove(claim)
                if claim['mainsnak']['datatype'] != 'external-id':
                    self.add_refs(claim, set(snak['source']) if 'source' in snak else set())

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
        if not self.qid or json.dumps(self.entity) != original:
            return self.save()

    @classmethod
    def haswbstatement(cls, external_id, property_id=None):
        if external_id and (property_id := property_id if property_id else cls.db_property):
            return Wikidata.search('haswbstatement:"{}={}"'.format(property_id, external_id))

    @classmethod
    def get_by_id(cls, external_id: str):
        """Attempt to find qid by external_id or create it"""
        try:
            if qid := cls.haswbstatement(external_id):
                return qid
            return cls(external_id).update(cls.prepare_data(external_id))
        except ValueError as e:
            logging.warning('Found {} instances of {}="{}", skipping'.format(e.args[0], cls.db_property, external_id))
