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
from decimal import Decimal, DecimalException, InvalidOperation
from urllib.parse import unquote

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
    property, db_ref, _config = None, None, {}

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

    @classmethod
    def next(cls) -> list:
        """Retrieves next chunk of external identifiers"""
        return []

    def __init__(self, external_id: str, snaks: list = None):
        self.input_snaks = snaks if snaks is not None else [self.create_snak(self.property, external_id)]
        self.external_id = external_id

    @classmethod
    def prepare_data(cls, external_id: str):
        return cls(external_id)

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
        elif 'language' in standard:
            return value['text'] + '@' + value['language']
        return float('nan')  # because NaN != NaN

    @classmethod
    def create_snak(cls, property_id: str, value, lower: str = None, upper: str = None):
        """Create snak based on provided id of the property and string value"""
        if not (t := Wikidata.type_of(property_id)) or value is None or value == '' or value == 'NaN':
            return None
        snak = {'datatype': t, 'property': property_id, 'snaktype': 'value', 'datavalue': {'value': value, 'type': t}}
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
            if not (value := cls.lut(value)) or not re.search('Q\\d+$', value):
                return None
            snak['datavalue'] = {'type': 'wikibase-entityid', 'value': {'entity-type': 'item', 'id': value}}
        elif snak['datatype'] == 'time':
            if not (value := Model.parse_date(snak['datavalue']['value'])):
                return
            snak['datavalue']['value'] = value
        elif snak['datatype'] == 'external-id':
            snak['datavalue']['type'] = 'string'
        elif snak['datatype'] == 'monolingualtext':
            snak['datavalue']['value'] = {'text': value, 'language': 'en'}
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
            for c in claim['qualifiers'][property_id]:
                if was_found := Model.equals(c, {'id': conditions['qualifiers'][property_id]}):
                    break
            if not was_found:  # Above loop did not find anything
                return False
        return True  # All conditions are met

    @classmethod
    def enrich_qualifier(cls, snak, value):
        if (not snak) or (not cls.config(snak['property'].upper(), 'id')):
            return snak
        for pattern in (config := cls.config(snak['property'].upper()))['translate']:
            if value.startswith(pattern):
                return {**snak, 'qualifiers': {config['id']: config['translate'][pattern]}}

    def get_qid(self):
        return None if self else None  # To disable "can be made static" warning


class Claim:
    def __init__(self, claim: dict):
        self.claim = claim

    @classmethod
    def construct(cls, snak: dict, ident: str = None):
        if snak is None:
            return
        if 'datavalue' in snak and isinstance(snak['datavalue']['value'], dict) and 'id' in snak['datavalue']['value']:
            if ident == snak['datavalue']['value']['id']:
                return
        claim = Claim({'type': 'statement', 'mainsnak': snak})
        if ident:  # can be full claim id or just qid
            claim.claim['id'] = ident if '$' in ident else ident + '$' + str(uuid.uuid4())
        return claim

    def process_decorators(self, snak: dict, db_ref: str):
        if 'qualifiers' in snak:
            for p_id in snak['qualifiers']:
                if new_qualifier := Model.create_snak(p_id, snak['qualifiers'][p_id]):
                    self.claim['qualifiers'] = self.claim['qualifiers'] if 'qualifiers' in self.claim else {}
                    if p_id in self.claim['qualifiers']:
                        if Model.equals(self.claim['qualifiers'][p_id][0], new_qualifier['datavalue']['value']):
                            continue
                    self.claim['qualifiers'][p_id] = [new_qualifier]

        if ('source' in snak) or (self.claim['mainsnak']['datatype'] != 'external-id'):  # ToDo why we need such if?
            self.claim['references'] = self.claim['references'] if 'references' in self.claim else []
            sources = set(snak['source']) if 'source' in snak else set()
            sources = {db_ref} | sources if db_ref else sources
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
            result.append(v['id'] if isinstance(v := ref['datavalue']['value'], dict) and 'id' in v else v)
        return result

    @staticmethod
    def get_latest_ref_date(claim: dict):
        latest = 0
        if 'references' in claim:
            for ref in claim['references']:
                if 'P248' in ref['snaks']:
                    ref_id = ref['snaks']['P248'][0]['datavalue']['value']['id']
                    if Claim._pub_dates[ref_id] and Claim._pub_dates[ref_id] > latest:
                        latest = Claim._pub_dates[ref_id]
        return latest

    def save(self, summary):
        Wikidata.edit(data={'summary': summary, 'claim': json.dumps(self.claim)}, method='wbsetclaim')

    def check_if_no_refs(self, db_ref: str, ref_properties: set) -> bool:
        if 'remove' not in self.claim and ('references' in self.claim):
            self.claim['references'] = self._remove_duplicates(self.claim['references'], ref_properties)
            self.claim['references'] = self._confirms(self.claim['references'], db_ref)
            return len(self.claim['references']) == 0

    @classmethod
    def extract_references(cls, claim: dict):
        result = set()
        for ref in claim['references'] if 'references' in claim else []:
            for p248 in ref['snaks']['P248'] if 'P248' in ref['snaks'] else []:
                if p248['datavalue']['value']['id'] not in Claim._pub_dates:
                    result.add(p248['datavalue']['value']['id'])
        return result

    @classmethod
    def preload(cls, qids: set):
        if qids and (result := Wikidata.load(qids)):
            for ref_id, item in result.items():
                p577 = None
                if 'redirects' in item:
                    Claim._redirects[ref_id] = item['redirects']['to']
                if ('claims' in item) and ('P577' in item['claims']):
                    if 'datavalue' in (item['claims']['P577'][0]['mainsnak']):
                        p577 = item['claims']['P577'][0]['mainsnak']['datavalue']['value']
                Claim._pub_dates[ref_id] = int(Model.serialize(p577)) if p577 else None

    @staticmethod
    def _remove_duplicates(references: [], ref_properties: set) -> []:
        """ Resolve redirects and merge P248 duplicates"""
        result, prior = [], {}
        for ref in references:
            if set(ref['snaks'].keys()) <= ref_properties:
                if (_id := ref['snaks']['P248'][0]['datavalue']['value']['id']) in Claim._redirects:
                    ref['snaks']['P248'][0]['datavalue']['value']['id'] = (_id := Claim._redirects[_id])
                if _id in prior:
                    for p12132 in (ref['snaks']['P12132'] if 'P12132' in ref['snaks'] else []):
                        if p12132['datavalue']['value']['id'] not in Claim.__get_snaks(prior[_id], 'P12132'):
                            if 'P12132' not in prior[_id]['snaks']:
                                prior[_id]['snaks']['P12132'] = []
                            prior[_id]['snaks']['P12132'].append(p12132)
                    p5997 = sorted(Claim.__get_snaks(prior[_id], 'P5997') + Claim.__get_snaks(ref, 'P5997'))
                    if len(p5997) > 0:
                        prior[_id]['snaks']['P5997'] = [Model.create_snak('P5997', p5997[0])]
                    if 'wdpy' in ref:
                        prior[_id]['wdpy'] = 1
                    continue  # do not add into results
                prior[_id] = ref
            result.append(ref)
        return result

    @staticmethod
    def _confirms(references: [], db_ref: str) -> []:
        result, aggregator, aggregator_needed = [], None, True
        for ref in references:
            if ref.pop('wdpy', 0):
                if db_ref in Claim.__get_snaks(ref, 'P248'):
                    aggregator = ref
                else:
                    aggregator_needed = False
                    result.append(ref)  # .confirm(ref, self))
            elif db_ref not in Claim.__get_snaks(ref, 'P248'):
                if 'P12132' in ref['snaks']:
                    for r in list(ref['snaks']['P12132']):
                        if r['datavalue']['value']['id'] == db_ref:
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
    _model, __cache = Model, {}
    __slots__ = 'qid', 'external_id', '_entity', '_affected', '__original', '_queue'

    def __init__(self, external_id: str, qid: str = None):
        self.external_id, self.qid, self._entity, self._affected, self.__original = external_id, qid, None, set(), None
        self._queue = []
        if qid or (qid := self.get_qid()):  # There is a chance to find qid down the road
            self.set_qid(qid)

    def set_qid(self, qid):
        self.get_cache()[self.external_id] = self.qid = qid
        self._entity = self.__original = None
        return qid

    def get_qid(self):
        try:
            if self.external_id in self.get_cache():
                return self.get_cache()[self.external_id]
            elif qid := self.haswbstatement(self.external_id):
                self.set_qid(qid)  # ToDo: find more elegant solution
                self.trace('primary cache miss "{}"'.format(self.external_id))
                return qid
        except ValueError as e:
            logging.warning('{} instances {}="{}", skip'.format(e.args[0], self._model.property, self.external_id))
            self.set_qid(None)

    @property
    def entity(self):
        if not self._entity:
            self._entity = {'labels': {}, 'claims': {}}
            if self.qid and (result := Wikidata.load({self.qid})):
                self._entity = result[self.qid]
            self.__original = json.dumps(self._entity, sort_keys=True)
        return self._entity

    @property
    def need_update(self) -> bool:
        if self.__original is None:
            return (self.external_id not in self.get_cache()) or (self.get_cache()[self.external_id])

    def trace(self, message: str, level=20):
        # CRITICAL: 50, ERROR: 40, WARNING: 30, INFO: 20, DEBUG: 10
        pattern = 'https://www.wikidata.org/wiki/{}#{}\t{}'
        logging.log(level, pattern.format(self.qid, type(self)._model.property, message) if self.qid else message)

    def find_claim(self, snak: dict):
        for c in (claim_list := self.entity['claims'][snak['property']]):
            if snak['snaktype'] == 'novalue':
                if c['mainsnak']['snaktype'] == 'novalue':
                    return Claim(c)
            elif Model.qualifier_filter(snak, c) and Model.equals(c['mainsnak'], snak['datavalue']['value']):
                return Claim(c)
        if len(claim_list) > 0 and Wikidata.type_of(snak['property']) == 'external-id':  # Force rewrite external id
            claim_list[0]['mainsnak']['datavalue']['value'] = snak['datavalue']['value']
            return Claim(claim_list[0])

    def obtain_claim(self, snak: dict):
        """Find or create claim, corresponding to the provided snak"""
        if snak['property'] not in self.entity['claims']:
            self.entity['claims'][snak['property']] = []

        if not (claim := self.find_claim(snak)):
            if not (claim := Claim.construct(snak, self.qid)):
                return
            self.entity['claims'][snak['property']].append(claim.claim)
        claim.process_decorators(snak, self._model.db_ref)
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
            Claim(statement).find_more_precise_claim(self.entity['claims'][property_id])

        latest = 0
        for statement in self.entity['claims'][property_id]:
            if 'remove' not in statement and ('rank' not in statement or statement['rank'] == 'normal'):
                if (current := Claim.get_latest_ref_date(statement)) > latest:
                    latest = current

        remaining_normal = 1  # only one statement supported by latest sources should remain normal
        for statement in self.entity['claims'][property_id]:
            if 'remove' not in statement and ('rank' not in statement or statement['rank'] == 'normal'):
                if remaining_normal == 0 or latest > Claim.get_latest_ref_date(statement):
                    statement['rank'] = 'deprecated'
                else:
                    remaining_normal -= 1

    def delete_claim(self, claim):
        if 'hash' in claim['mainsnak']:  # already saved
            self._queue.append({'id': claim['id'], 'remove': ''})  # request server to delete claim
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

    def serialize(self) -> str:
        result = self._queue
        for property_id in self._affected:
            result += self.entity['claims'][property_id]
        return ('{{"claims":{},"labels":{},"aliases":{}}}'.format(json.dumps(result), json.dumps(self.entity['labels']),
                                                                  json.dumps(self.entity['aliases'])))

    def post_process(self):
        new_sources = set()
        for property_id in self._affected:
            for c in self.entity['claims'][property_id]:
                new_sources.update(Claim.extract_references(c))
        Claim.preload(new_sources)

        for property_id in self._affected:
            for c in list(self.entity['claims'][property_id]):
                if Claim(c).check_if_no_refs(self._model.db_ref, set(self._model.config('references'))):
                    self.delete_claim(c)

            if self._model.config(property_id):
                self.remove_all_but_one(property_id)
            elif Wikidata.type_of(property_id) in ['quantity', 'string', 'monolingualtext'] or property_id == 'P577':
                self.deprecate_all_but_one(property_id)

        if 'en' not in self.entity['labels']:
            self.entity['labels']['en'] = {'value': self.external_id, 'language': 'en'}

    def get_summary(self):
        return 'batch import from [[' + self._model.db_ref + ']] for object ' + self.external_id

    def save(self):
        if (self.__original is None) or (json.dumps(self.entity, sort_keys=True) == self.__original):
            return

        self.post_process()

        data = {'data': self.serialize(), 'summary': self.get_summary()}
        if 'id' in self.entity:
            data['id'] = self.entity['id']
            data['baserevid'] = self.entity['lastrevid']
        else:
            data['new'] = 'item'

        if response := Wikidata.edit(data, 'wbeditentity'):
            if 'nochange' not in response['entity']:
                self.set_qid(response['entity']['id'])
                self.trace('modified' if 'id' in data else 'created')
                return self.qid
            # else:  # Too many
            #     self.trace('no change while saving {}'.format(data['data']))

    def apply(self, parsed_data: Model):
        if parsed_data:
            if self.external_id != parsed_data.external_id:
                self.trace('"{}" will be replaced with "{}"'.format(self.external_id, parsed_data.external_id))
                self.external_id = parsed_data.external_id
                if self.qid is None and (qid := self.get_qid()):
                    self.set_qid(qid)

            if self.qid is None:
                for snak in parsed_data.input_snaks:
                    if (snak['datatype'] == 'external-id') and (snak['property'] != self._model.property):
                        if qid := self.haswbstatement(snak['datavalue']['value'], snak['property']):
                            self.set_qid(qid)
                            break
                if self.qid is None:
                    self.set_qid(parsed_data.get_qid())

            for snak in parsed_data.input_snaks:
                if self.obtain_claim(snak):
                    self._affected.add(snak['property'])
        else:
            self.trace('no data retrieved for "{}"'.format(self.external_id), 30)
            self.set_qid(None)

    @classmethod
    def haswbstatement(cls, external_id, property_id=None):
        if external_id and (property_id := property_id if property_id else cls._model.property):
            return Wikidata.search('haswbstatement:"{}={}"'.format(property_id, external_id))

    @classmethod
    def get_by_id(cls, external_id: str, forced: bool = False):
        """Attempt to find qid by external_id or create it"""
        if (instance := cls(external_id)).need_update or forced:
            instance.apply(cls._model.prepare_data(external_id))
        return instance

    @classmethod
    def get_cache(cls, reset=None) -> dict:
        if reset is not None:
            cls.__cache = reset
        elif cls.__cache is None:
            sparql = 'SELECT ?c ?i {{ ?i p:{}/ps:{} ?c }}'.format(cls._model.property, cls._model.property)
            cls.__cache = result if (result := Wikidata.query(sparql)) else {}
        return cls.__cache


class TAPClient(Model):
    """Retrieve data from TAP 'endpoint' using 'queries' specified in json-file"""
    _dataset, _ADQL_WRAPPER = {}, 'SELECT * FROM ({}) a WHERE {}'

    @classmethod
    def load(cls, condition=None) -> dict:
        result = {}
        for lines in cls.config('queries'):
            query = ''.join(lines)
            if condition:
                query = cls._ADQL_WRAPPER.format(query, condition)
            TAPClient.query(cls.config('endpoint'), query, result)
        return result

    @classmethod
    def prepare_data(cls, external_id):
        model = super().prepare_data(external_id)
        if external_id in cls._dataset:
            rows = cls._dataset.pop(external_id)
        elif external_id in (result := cls.load('main_id = \'{}\''.format(external_id))):
            rows = result[external_id]
        else:
            return None

        for row in rows:
            for col in row:
                if row[col] and re.search('\\d+$', col):
                    model.construct_snak(row, col)
                    if col.upper() in ['P6257', 'P6258']:  # add J2000 epoch
                        model.input_snaks.append(cls.create_snak('P6259', 'Q1264450'))
        return model

    def construct_snak(self, row, col, new_col=None):
        new_col = (new_col if new_col else col).upper()
        if col == 'p397':
            result: dict = TAPClient.get_parent_snak(row[col])
        elif Wikidata.type_of(new_col) != 'quantity':
            result: dict = TAPClient.create_snak(new_col, row[col])
        elif (col + 'h' not in row) or (row[col + 'h'] == ''):
            result: dict = TAPClient.create_snak(new_col, TAPClient.format_figure(row, col))
        else:
            try:
                high = TAPClient.format_figure(row, col + 'h')
                low = TAPClient.format_figure(row, col + 'l')
                result: dict = TAPClient.create_snak(new_col, TAPClient.format_figure(row, col), low, high)
            except InvalidOperation:
                return

        if result is not None:
            if 'mespos' in row:
                result['mespos'] = row['mespos']
            if col + 'u' in row and (unit := self.lut(row[col + 'u'])):
                result['datavalue']['value']['unit'] = 'http://www.wikidata.org/entity/' + unit
            reference = row[col + 'r'] if col + 'r' in row and row[col + 'r'] else None
            reference = row['reference'] if 'reference' in row and row['reference'] else reference
            if reference and (ref_id := TAPClient.parse_url(re.sub('.*(http\\S+).*', '\\g<1>', reference))):
                result['source'] = [ref_id]

        if result := self.enrich_qualifier(result, row['qualifier'] if 'qualifier' in row else row[col]):
            self.input_snaks.append(result)

    @staticmethod
    def query(url, adql, result=None):
        if response := Wikidata.request(url + '/sync', data={'request': 'doQuery', 'lang': 'adql', 'format': 'csv',
                                                             'maxrec': -1, 'query': adql}, stream=True):
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

    _parents, __PATTERN = None, 'https://www.wikidata.org/wiki/{}#P528\tcatalogue cache miss "{}"'

    @staticmethod
    def get_parent_snak(name: str):
        if TAPClient._parents is None:
            TAPClient._parents = Wikidata.query('SELECT DISTINCT ?c ?i { ?i ^ps:P397 []; wdt:P528 ?c }',
                                                lambda row, _: (row[0].lower(), row[1]))

        name = name[:-1] if re.search('OGLE.+L$', name) else name  # In SIMBAD OGLE names are w/o trailing 'L'
        if name.lower() not in TAPClient._parents:
            import simbad_dap
            if (simbad_id := simbad_dap.Model.get_id_by_name(name)) is None:
                return
            if simbad_id.lower() not in TAPClient._parents:
                if (instance := simbad_dap.Element.get_by_id(simbad_id)).need_update:
                    # instance.apply(Model(simbad_id, [Model.create_snak('P528', name)]))
                    instance.save()
                if instance.qid is None:
                    return
                TAPClient._parents[simbad_id.lower()] = instance.qid
            TAPClient._parents[name.lower()] = TAPClient._parents[simbad_id.lower()]
            logging.info(TAPClient.__PATTERN.format(TAPClient._parents[name.lower()], name))
        if snak := TAPClient.create_snak('P397', TAPClient._parents[name.lower()]):
            return {**snak, 'decorators': {'P5997': name}}

    @staticmethod
    def format_figure(row, col):  # SIMBAD-specific way to specify figure precision
        return TAPClient.format_float(row[col],
                                      int(row[col + 'p']) if col + 'p' in row and row[col + 'p'] != '' else -1)

    @staticmethod
    def parse_url(url: str) -> str:
        """Try to find qid of the reference based on the url provided"""
        import ads
        import arxiv

        if url and url.strip() and (url := url.split()[0]):  # get text before first whitespace and strip
            for pattern, repl in TAPClient.config('transform').items():
                if (query := unquote(re.sub(pattern, repl, url, flags=re.S))).startswith('P'):
                    if query.startswith('P818='):
                        (instance := arxiv.Element.get_by_id(query.replace('P818=', ''))).save()
                        if instance.qid:
                            return instance.qid
                    elif query.startswith('P819='):
                        (instance := ads.Element.get_by_id(query.replace('P819=', ''))).save()
                        if instance.qid:
                            return instance.qid
                    else:  # fallback
                        try:
                            return Wikidata.search('haswbstatement:' + query)
                        except ValueError as e:
                            logging.warning('Found {} instances of {}'.format(e.args[0], query))


class AstroItem(Element):
    __const, _model = None, TAPClient

    def obtain_claim(self, snak):
        snak['decorators'] = snak['decorators'] if 'decorators' in snak else {}
        snak['decorators']['P12132'] = self._model.db_ref
        if claim := super().obtain_claim(snak):
            if 'mespos' in snak and ('mespos' not in claim or int(claim['mespos']) > int(snak['mespos'])):
                claim['mespos'] = snak['mespos']
            if snak['property'] == 'P1215':
                if 'qualifiers' in snak and 'P1227' in snak['qualifiers'] and snak['qualifiers']['P1227'] == 'Q4892529':
                    claim['rank'] = 'preferred'  # V-magnitude is always preferred
        return claim

    def remove_all_but_one(self, property_id):
        if property_id not in ['P528']:
            super().remove_all_but_one(property_id)

    def post_process(self):
        from astropy import coordinates
        super().post_process()
        try:
            ra = self.entity['claims']['P6257'][0]['mainsnak']['datavalue']['value']['amount']
            dec = self.entity['claims']['P6258'][0]['mainsnak']['datavalue']['value']['amount']
            tla = coordinates.SkyCoord(ra, dec, frame='icrs', unit='deg').get_constellation(short_name=True)
            if AstroItem.__const is None:
                AstroItem.__const = Wikidata.query(
                    'SELECT DISTINCT ?n ?i {?i wdt:P31/wdt:P279* wd:Q8928; wdt:P1813 ?n}')
            target = None
            for claim in list(self.entity['claims']['P59'] if 'P59' in self.entity['claims'] else []):
                if target or (claim['mainsnak']['datavalue']['value']['id'] != AstroItem.__const[tla]):
                    self.delete_claim(claim)
                else:
                    target = claim
            target = target if target else self.obtain_claim(TAPClient.create_snak('P59', AstroItem.__const[tla]))
            target['references'] = [{'snaks': {'P887': [TAPClient.create_snak('P887', 'Q123764736')]}}]
        except KeyError:
            return


class Article(Element):
    def obtain_claim(self, snak: dict):
        if snak['property'] == 'P356':
            session = requests.Session()
            session.headers.update({'User-Agent': Wikidata.USER_AGENT})
            if Wikidata.request('https://doi.org/' + snak['datavalue']['value'], session) is None:
                return
            snak['datavalue']['value'] = snak['datavalue']['value'].upper()
        return super().obtain_claim(snak)

    def post_process(self):
        super().post_process()
        self.sort_authors('P2093', self.sort_authors('P50', []))

    def sort_authors(self, property_id, already_used):
        authors = {}
        if property_id in self.entity['claims']:
            for claim in list(self.entity['claims'][property_id]):
                if ('qualifiers' in claim) and ('P1545' in claim['qualifiers']):
                    if (num := int(claim['qualifiers']['P1545'][0]['datavalue']['value'])) not in already_used:
                        authors[num] = claim
                    self.delete_claim(claim)
            self._queue += list(dict(sorted(authors.items())).values())
        return list(authors.keys())


Model.initialize(__file__)  # to load wd.json
