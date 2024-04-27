from __future__ import annotations

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
    login, __password, __token = '', '', 'bad'
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
        """Wikidata API v1 call with JSON format, see https://wikidata.org/w/api.php"""
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
        Wikidata.login = login if login else Wikidata.login
        Wikidata.__password = password if password else Wikidata.__password
        token = Wikidata.call('query', {'meta': 'tokens', 'type': 'login'})['query']['tokens']['logintoken']
        Wikidata.call('login', {'lgtoken': token, 'lgname': Wikidata.login, 'lgpassword': Wikidata.__password})

    @staticmethod
    def load(items: set[str]):
        """Load up to 50 wikidata entities, returns None in case of error"""
        if len(items) > 0:
            result = Wikidata.call('wbgetentities',
                                   {'props': 'claims|info|labels|aliases', 'ids': '|'.join(sorted(items))})
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
    property, _config = None, {}

    @classmethod
    def initialize(cls, file_name: str) -> bool:
        try:
            with open(os.path.splitext(file_name)[0] + '.json') as file:
                cls._config = {**cls._config, **json.load(file)}
        except OSError:
            pass
        if need_init := (sys.argv[0].endswith(os.path.basename(file_name)) and not Wikidata.login):
            Wikidata.logon(sys.argv[1], sys.argv[2])
        return need_init

    @classmethod
    def config(cls, *kwargs):
        # returns requested setting or None
        result = cls._config
        for _id in kwargs:
            if _id in result:
                result = result[_id]
            else:
                return None
        return result

    @classmethod
    def lut(cls, text: str):
        return qid if (qid := cls.config('translate', text)) else text

    @staticmethod
    def get_next_chunk(_: any) -> Tuple[list[str], any]:
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

    @classmethod
    def prepare_data(cls, external_id: str) -> []:
        return [cls.create_snak(cls.property, external_id)]

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

    @classmethod
    def create_snak(cls, property_id: str, value, lower: str = None, upper: str = None):
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
            if not re.search('Q\\d+$', (value := cls.lut(value))):
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
    def equals(snak: dict, value: dict) -> bool:
        return 'datavalue' in snak and Model.serialize(snak['datavalue']['value']) == Model.serialize(value)

    @staticmethod
    def qualifier_filter(conditions: dict, claim: dict) -> bool:
        for property_id in (conditions['qualifiers'] if 'qualifiers' in conditions else {}):
            if 'qualifiers' not in claim or (property_id not in claim['qualifiers']):  # No property_id qualifier
                return False
            was_found = None
            for claim in claim['qualifiers'][property_id]:
                if was_found := Model.equals(claim, {'id': conditions['qualifiers'][property_id]}):
                    break
            if not was_found:  # Above loop did not find anything
                return False
        return True  # All conditions are met


class Claim:
    db_ref = None

    def __init__(self, claim: dict):
        self.claim = claim

    @classmethod
    def construct(cls, snak: dict, ident: str = None):
        if snak is None:
            return
        if 'datavalue' in snak and isinstance(snak['datavalue']['value'], dict) and 'id' in snak['datavalue']['value']:
            if ident == snak['datavalue']['value']['id']:
                return
        (claim := cls({'type': 'statement', 'mainsnak': snak})).process_decorators(snak)
        if ident:  # can be full claim id or just qid
            claim.claim['id'] = ident if '$' in ident else ident + '$' + str(uuid.uuid4())
        return claim

    def process_decorators(self, snak: dict):
        if 'qualifiers' in snak:
            self.claim['qualifiers'] = self.claim['qualifiers'] if 'qualifiers' in self.claim else {}
            for p_id in snak['qualifiers']:
                if p_id not in self.claim['qualifiers']:
                    self.claim['qualifiers'][p_id] = []
                self.claim['qualifiers'][p_id].append(Model.create_snak(p_id, snak['qualifiers'][p_id]))

        if ('source' in snak) or (self.claim['mainsnak']['datatype'] != 'external-id'):  # ToDo why we need such if?
            self.claim['references'] = self.claim['references'] if 'references' in self.claim else []
            sources = set(snak['source']) if 'source' in snak else set()
            sources = {self.db_ref} | sources if self.db_ref else sources
            decorator = snak['decorators'] if 'decorators' in snak else {}
            for src_id in sources:
                self.claim['references'].append({**Claim._create_ref(src_id, decorator), 'wdpy': 1})

    @staticmethod
    def _create_ref(src_id: str, decorators: dict):
        snaks = {'P248': [Model.create_snak('P248', src_id)]}
        for p_id in decorators:
            if src_id != decorators[p_id]:
                snaks[p_id] = [Model.create_snak(p_id, decorators[p_id])]
        return {'snaks': snaks}

    _pub_dates, _redirects = {'Q66617668': 19240101, 'Q4026990': 99999999, 'Q654724': 19240101}, {}

    @staticmethod
    def __get_snaks(reference: {}, property_id: str) -> []:
        result = []
        for ref in reference['snaks'][property_id] if property_id in reference['snaks'] else []:
            result.append(ref['datavalue']['value']['id'])
        return result

    @staticmethod
    def get_latest_ref_date(claim: dict):
        latest = 0
        if 'references' in claim:
            Claim._preload(claim['references'] if 'references' in claim else [])
            for ref in claim['references']:
                if 'P248' in ref['snaks']:
                    ref_id = ref['snaks']['P248'][0]['datavalue']['value']['id']
                    if Claim._pub_dates[ref_id] and Claim._pub_dates[ref_id] > latest:
                        latest = Claim._pub_dates[ref_id]
        return latest

    def save(self, summary):
        Wikidata.edit(data={'summary': summary, 'claim': json.dumps(self.claim)}, method='wbsetclaim')

    def check_if_no_refs(self) -> bool:
        if 'remove' not in self.claim:
            Claim._preload(refs := self.claim['references'] if 'references' in self.claim else [])
            if len(new_refs := self._confirms(self._remove_duplicates(refs))) == 0 and len(refs) > 0:
                return True
            self.claim['references'] = new_refs

    @staticmethod
    def _preload(references: []):
        qid = set()
        for ref in references:
            for p248 in ref['snaks']['P248'] if 'P248' in ref['snaks'] else []:
                if p248['datavalue']['value']['id'] not in Claim._pub_dates:
                    qid.add(p248['datavalue']['value']['id'])
        if qid and (result := Wikidata.load(qid)):
            for ref_id, item in result.items():
                p577 = None
                if 'redirects' in item:
                    Claim._redirects[ref_id] = item['redirects']['to']
                if 'claims' in item and 'P577' in item['claims']:
                    p577 = item['claims']['P577'][0]['mainsnak']['datavalue']['value']
                Claim._pub_dates[ref_id] = int(Model.serialize(p577)) if p577 else None

    @staticmethod
    def _remove_duplicates(references: []) -> []:
        """ Resolve redirects and merge P248 duplicates"""
        result, prior = [], {}
        for ref in references:
            if 'P248' in ref['snaks']:
                if (_id := ref['snaks']['P248'][0]['datavalue']['value']['id']) in Claim._redirects:
                    ref['snaks']['P248'][0]['datavalue']['value']['id'] = (_id := Claim._redirects[_id])
                if _id in prior:
                    for p12132 in (ref['snaks']['P12132'] if 'P12132' in ref['snaks'] else []):
                        if p12132['datavalue']['value']['id'] not in Claim.__get_snaks(prior[_id], 'P12132'):
                            if 'P12132' not in prior[_id]['snaks']:
                                prior[_id]['snaks']['P12132'] = []
                            prior[_id]['snaks']['P12132'].append(p12132)
                    for property_id in ref['snaks']:
                        if property_id not in prior[_id]['snaks']:
                            prior[_id]['snaks'][property_id] = ref['snaks'][property_id]
                    if 'wdpy' in ref:
                        prior[_id]['wdpy'] = 1
                    continue  # do not add into results
                prior[_id] = ref
            result.append(ref)
        return result

    @classmethod
    def _confirms(cls, references: []) -> []:
        result, aggregator, aggregator_needed = [], None, True
        for ref in references:
            if ref.pop('wdpy', 0):
                if cls.db_ref in Claim.__get_snaks(ref, 'P248'):
                    aggregator = ref
                else:
                    aggregator_needed = False
                    result.append(ref)  # .confirm(ref, self))
            elif cls.db_ref not in Claim.__get_snaks(ref, 'P248'):
                if 'P12132' in ref['snaks']:
                    for r in list(ref['snaks']['P12132']):
                        if r['datavalue']['value']['id'] == cls.db_ref:
                            ref['snaks']['P12132'].remove(r)
                    if len(ref['snaks']['P12132']) == 0:
                        continue
                result.append(ref)
        if aggregator and aggregator_needed:
            result.append(aggregator)  # .confirm(aggregator, self))
        return result

    def find_more_precise_claim(self, statements: []) -> dict:
        """Look for statement with more precise value and deprecate current if found"""
        if 'datavalue' in self.claim['mainsnak']:  # not novalue|somevalue
            value = self.claim['mainsnak']['datavalue']['value']
            for claim in statements:
                if self.claim != claim and 'datavalue' in claim['mainsnak']:
                    if Model.serialize(claim['mainsnak']['datavalue']['value'], value) == Model.serialize(value):
                        self.claim['rank'] = 'deprecated'
                        self.claim['qualifiers'] = {} if 'qualifiers' not in self.claim else self.claim['qualifiers']
                        self.claim['qualifiers']['P2241'] = [Model.create_snak('P2241', 'Q42727519')]
                        return claim
            if 'qualifiers' in self.claim and 'P2241' in self.claim['qualifiers']:
                if 'Q42727519' == self.claim['qualifiers']['P2241'][0]['datavalue']['value']['id']:
                    self.claim['qualifiers'].pop('P2241')
                    self.claim['rank'] = 'normal'


class Element:
    _model, _claim, __cache = Model, Claim, {}

    def __init__(self, external_id: str, qid: str = None):
        self.external_id, self.qid, self._entity = external_id, qid, None

    @property
    def entity(self):
        if not self._entity:
            try:
                self.qid = self.qid if self.qid else self.haswbstatement(self.external_id)
            except ValueError as e:
                logging.warning('Found {} items of {}="{}"'.format(e.args[0], self._model.property, self.external_id))
            self._entity = {'labels': {}, 'claims': {}}
            if self.qid and (result := Wikidata.load({self.qid})):
                self._entity = result[self.qid]
        return self._entity

    def trace(self, message: str, level=20):
        # CRITICAL: 50, ERROR: 40, WARNING: 30, INFO: 20, DEBUG: 10
        pattern = 'https://www.wikidata.org/wiki/{}#{}\t{}'
        logging.log(level, pattern.format(self.qid, self._model.property, message) if self.qid else message)

    def find_claim(self, snak: dict):
        for c in (claim_list := self.entity['claims'][snak['property']]):
            if snak['snaktype'] == 'novalue':
                if c['mainsnak']['snaktype'] == 'novalue':
                    return self._claim(c)
            elif Model.qualifier_filter(snak, c) and Model.equals(c['mainsnak'], snak['datavalue']['value']):
                return self._claim(c)
        if len(claim_list) > 0 and Wikidata.type_of(snak['property']) == 'external-id':  # Force rewrite external id
            claim_list[0]['mainsnak']['datavalue']['value'] = snak['datavalue']['value']
            return self._claim(claim_list[0])

    def obtain_claim(self, snak: dict):
        """Find or create claim, corresponding to the provided snak"""
        if snak['property'] not in self.entity['claims']:
            self.entity['claims'][snak['property']] = []

        if claim := self.find_claim(snak):
            claim.process_decorators(snak)
            return claim.claim
        if claim := self._claim.construct(snak, self.qid):
            self.entity['claims'][snak['property']].append(claim.claim)
            return claim.claim

    def deprecate_all_but_one(self, property_id: str):
        minimal = 999999
        for statement in self.entity['claims'][property_id]:
            if 'rank' in statement and statement['rank'] == 'preferred':
                return  # do not change any ranks

            if 'mespos' in statement and minimal > int(statement['mespos']):  # ToDo: mespos should not be in wd.py
                minimal = int(statement['mespos'])

        for statement in self.entity['claims'][property_id]:
            if 'mespos' in statement:  # normal for statements with minimal mespos, deprecated for the rest
                if int(statement['mespos']) == minimal:
                    statement['rank'] = 'normal'
                elif 'hash' not in statement['mainsnak'] and 'rank' not in statement:
                    statement['rank'] = 'deprecated'

        for statement in self.entity['claims'][property_id]:
            self._claim(statement).find_more_precise_claim(self.entity['claims'][property_id])

        latest = 0
        for statement in self.entity['claims'][property_id]:
            if 'remove' not in statement and ('rank' not in statement or statement['rank'] == 'normal'):
                if (current := self._claim.get_latest_ref_date(statement)) > latest:
                    latest = current

        remaining_normal = 1  # only one statement supported by latest sources should remain normal
        for statement in self.entity['claims'][property_id]:
            if 'remove' not in statement and ('rank' not in statement or statement['rank'] == 'normal'):
                if remaining_normal == 0 or latest > Claim.get_latest_ref_date(statement):
                    statement['rank'] = 'deprecated'
                else:
                    remaining_normal -= 1

    def delete_claim(self, claim):
        if 'hash' in claim['mainsnak']:  # Already saved
            claim['remove'] = 1  # For already saved claims we have to request server to remove them
        else:
            self.entity['claims'][claim['mainsnak']['property']].remove(claim)  # Non-saved claim can be simply removed

    def remove_all_but_one(self, property_id):
        group_by = self._model.config(property_id, 'id')
        latest = {}  # None if leave as is otherwise latest publication date for all claims
        for claim in list(self.entity['claims'][property_id]):
            if 'remove' not in claim:
                group = None
                if group_by:
                    if 'qualifiers' not in claim or group_by not in claim['qualifiers']:
                        self.delete_claim(claim)
                        continue
                    group = Model.serialize(claim['qualifiers'][group_by][0]['datavalue']['value'])

                latest[group] = 0 if group not in latest else latest[group]
                if 'datavalue' not in claim['mainsnak']:
                    latest[group] = None
                elif latest[group] is not None:
                    latest[group] = cur if (cur := Claim.get_latest_ref_date(claim)) > latest[group] else latest[
                        group]

        for claim in list(self.entity['claims'][property_id]):
            if 'remove' not in claim:
                group = Model.serialize(claim['qualifiers'][group_by][0]['datavalue']['value']) if group_by else None
                if (latest[group] is None) and ('datavalue' in claim['mainsnak']):
                    self.delete_claim(claim)
                    continue
                elif (latest[group] is not None) and (Claim.get_latest_ref_date(claim) != latest[group]):
                    self.delete_claim(claim)
                    continue
                latest[group] = 99999999  # keep this claim and remove all others

    def post_process(self):
        """Changes in wd element in a way, that does not depend on the input"""
        if 'en' not in self.entity['labels']:
            self.entity['labels']['en'] = {'value': self.external_id, 'language': 'en'}

    def get_summary(self):
        return 'batch import from [[' + self._claim.db_ref + ']] for object ' + self.external_id

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
        if parsed_data:
            original = json.dumps(self.entity)

            affected_properties = set()
            for snak in parsed_data:
                if (claim := self.obtain_claim(snak)) and (claim['mainsnak']['datatype'] != 'external-id'):
                    affected_properties.add(snak['property'])

            for property_id in affected_properties:
                for c in list(self.entity['claims'][property_id]):
                    if self._claim(c).check_if_no_refs():
                        self.delete_claim(c)

                if self._model.config(property_id):
                    self.remove_all_but_one(property_id)
                elif Wikidata.type_of(property_id) in ['quantity', 'string'] or property_id == 'P577':
                    self.deprecate_all_but_one(property_id)

            self.post_process()

            if not self.qid or json.dumps(self.entity) != original:
                return self.save()

    @classmethod
    def haswbstatement(cls, external_id, property_id=None):
        if external_id and (property_id := property_id if property_id else cls._model.property):
            return Wikidata.search('haswbstatement:"{}={}"'.format(property_id, external_id))

    @classmethod
    def run(cls, external_id: str, qid: str = None):
        return cls(external_id, qid).update(cls._model.prepare_data(external_id))

    @classmethod
    def get_by_id(cls, external_id: str):
        """Attempt to find qid by external_id or create it"""
        try:
            if cls.__cache is None:
                sparql = 'SELECT ?c ?i {{ ?i p:{}/ps:{} ?c }}'.format(cls._model.property, cls._model.property)
                cls.__cache = result if (result := Wikidata.query(sparql)) else {}
            if external_id not in cls.__cache:
                cls.__cache[external_id] = qid if (qid := cls.haswbstatement(external_id)) else cls.run(external_id)
            return cls.__cache[external_id]
        except ValueError as e:
            logging.warning('Found {} instances of {}="{}", skip'.format(e.args[0], cls._model.property, external_id))
