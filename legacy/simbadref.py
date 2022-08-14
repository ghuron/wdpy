#!/usr/bin/python3
import csv
import datetime
import json
import sys
import time
import uuid

import dateutil.parser
import requests


def get_claim(entity, property_id, target, qualifier):
    if property_id in entity['claims']:
        for candidate in entity['claims'][property_id]:
            if qualifier is not None:
                if 'qualifiers' not in candidate or \
                        qualifier['id'] not in candidate['qualifiers'] or \
                        candidate['qualifiers'][qualifier['id']][0]['datavalue']['value']['id'] != qualifier['value']:
                    continue

            if target is None:
                return candidate
            if 'datavalue' not in candidate['mainsnak']:
                continue
            if isinstance(target, str):
                if candidate['mainsnak']['datavalue']['value'] == target:
                    return candidate
            else:
                if 'id' in target:
                    if candidate['mainsnak']['datavalue']['value']['id'] == target['id']:
                        return candidate
                if 'amount' in target:
                    source = candidate['mainsnak']['datavalue']['value']
                    if source['amount'].strip('+') == target['amount'].strip('+'):
                        if 'lowerBound' not in source and 'lowerBound' not in target:
                            return candidate
                        if 'lowerBound' in source and 'lowerBound' in target and \
                                source['lowerBound'].strip('+') == target['lowerBound'].strip('+'):
                            return candidate
    else:
        entity['claims'][property_id] = []

    new_claim = create_claim(entity['id'] if 'id' in entity else '', property_id, target)

    if 'id' not in entity:
        del new_claim['id']
    entity['claims'][property_id].append(new_claim)
    return new_claim


def create_claim(entity_id, property_id, target):
    types = {'P31': 'wikibase-entityid', 'P215': 'string', 'P223': 'string', 'P248': 'wikibase-entityid',
             'P304': 'string', 'P356': 'external-id', 'P478': 'string', 'P577': 'time',
             'P528': 'string', 'P642': 'wikibase-entityid', 'P819': 'external-id', 'P881': 'wikibase-entityid',
             'P972': 'wikibase-entityid',
             'P1090': 'quantity', 'P1215': 'quantity', 'P1227': 'wikibase-entityid', 'P1433': 'wikibase-entityid',
             'P1476': 'monolingualtext', 'P1545': 'string', 'P2093': 'string',
             'P2214': 'quantity',
             'P2215': 'quantity', 'P2216': 'quantity', 'P2227': 'quantity', 'P2386': 'quantity', 'P2583': 'quantity',
             'P4296': 'quantity', 'P6257': 'quantity', 'P6258': 'quantity', 'P6259': 'wikibase-entityid',
             'P6879': 'quantity'}

    serialized_value = "{}"
    value_type = types[property_id] if types[property_id] != 'external-id' else 'string'
    if target is not None:
        if isinstance(target, str):
            serialized_value = '"' + target + '"'
        else:
            serialized_value = json.dumps(target)

    return json.loads('{"id":"' + entity_id + '$' + str(uuid.uuid4()) + \
                      '", "type": "statement", "mainsnak": {"datatype": "' + types[property_id] + '", "property": "' + \
                      property_id + '", "snaktype": "value", "datavalue": {"type": "' + value_type + '", "value": ' + \
                      serialized_value + '}}}')


def query(sparql, aggregate=False):
    major = {}
    minor = {}
    with requests.Session() as session:
        session.headers.update({'Accept': 'text/csv',
                                'User-Agent': 'simbadbot/0.0 (https://github.com/ghuron/wdpy; https://www.wikidata.org/wiki/User:Ghuron)'})
        download = session.post('https://query.wikidata.org/sparql', params={'query': sparql})
        decoded_content = download.content.decode('utf-8')

        cr = csv.reader(decoded_content.splitlines(), delimiter=',')
        my_list = list(cr)
        for row in my_list[1:]:
            row = [item.replace('http://www.wikidata.org/entity/', '') for item in row]
            if aggregate:
                if row[0] not in major:
                    major[row[0]] = []
                major[row[0]] = major[row[0]] + [row[1]]
            else:
                major[row[0]] = row[1]
            if len(row) > 2:
                if row[2] == '': row[2] = '1800-01-01T00:00:00Z'
                minor[row[1]] = dateutil.parser.parse(row[2])
            else:
                minor[row[1]] = row[0]
    return major, minor


def enrich_claim(target, ref_id):
    if 'references' in target:
        if json.dumps(target['references']).find('"' + ref_id + '"') > 0:
            return False

    if 'references' not in target:
        target['references'] = []
    else:
        if ref_id == 'Q654724':
            return False  # simbad

    target['references'].append({"snaks": {"P248": [create_claim("", "P248", {"id": ref_id})['mainsnak']]}})
    return True


author = {}

cr = csv.reader(requests.post('http://simbad.u-strasbg.fr/simbad/sim-tap/sync', params={
    'request': 'doQuery',
    'lang': 'adql',
    'format': 'csv',
    'maxrec': -1,
    'query': 'select oidbibref, name, pos from author where oidbibref<250000',
}).content.decode('utf-8').splitlines(), delimiter=',')
my_list = list(cr)
for row in my_list[1:]:
    if row[0] not in author:
        author[row[0]] = []
    author[row[0]].append([row[1], row[2]])
cr = csv.reader(requests.post('http://simbad.u-strasbg.fr/simbad/sim-tap/sync', params={
    'request': 'doQuery',
    'lang': 'adql',
    'format': 'csv',
    'maxrec': -1,
    'query': 'select oidbibref, name, pos from author where oidbibref>=250000',
}).content.decode('utf-8').splitlines(), delimiter=',')
my_list = list(cr)
for row in my_list[1:]:
    if row[0] not in author:
        author[row[0]] = []
    author[row[0]].append([row[1], row[2]])

wdapi = requests.Session()
wdapi.headers.update(
    {'User-Agent': 'catbot/0.0 (https://github.com/ghuron/wdpy; https://www.wikidata.org/wiki/User:Ghuron)'})

r1 = wdapi.get('https://www.wikidata.org/w/api.php', params={
    'format': 'json',
    'action': 'query',
    'meta': 'tokens',
    'type': 'login',
})

r2 = wdapi.post('https://www.wikidata.org/w/api.php', data={
    'format': 'json',
    'action': 'login',
    'lgname': sys.argv[1],
    'lgpassword': sys.argv[2],
    'lgtoken': r1.json()['query']['tokens']['logintoken'],
})

csrftoken = 'badtoken'

articles = query(
    'select ?bibcode ?article { ?article wdt:P819 ?bibcode; }')[
    0]

journals = query('select ?code ?journal { ?journal wdt:P1300 ?code }')[0]

ref = requests.post('http://simbad.u-strasbg.fr/simbad/sim-tap/sync', params={
    'request': 'doQuery',
    'lang': 'adql',
    'format': 'json',
    'maxrec': -1,
    'query': 'select oidbib, bibcode, "year", journal, page, last_page, volume, title, doi from ref',
}).json()

for row in ref['data']:
    if row[7] is None or row[7] == '???':
        continue
    row[7] = row[7].strip('.').strip()
    if row[1] in articles:
        item = requests.get('https://www.wikidata.org/w/api.php', params={
            'format': 'json', 'action': 'wbgetentities', 'maxlag': '50',  # 'props': 'claims|info',
            'ids': articles[row[1]]}).json()['entities'][articles[row[1]]]
    else:
        # item = {'claims': {}, 'labels': {}}
        continue

    title = get_claim(item, 'P1476', None, None)
    if 'text' not in title['mainsnak']['datavalue']['value'] or len(title['mainsnak']['datavalue']['value']['text']) * 1.5 > len(row[7]):
        continue
    title['mainsnak']['datavalue']['value']['text'] = row[7]
    enrich_claim(title, 'Q654724')
    item['labels']['en'] = {'value': row[7], 'language': 'en'}

    # if 'P31' not in item['claims']:
    #     get_claim(item, 'P31', {'id': 'Q13442814'}, None)
    # enrich_claim(get_claim(item, 'P819', row[1], None), 'Q654724')
    # publication = get_claim(item, 'P577', None, None)
    # if 'precision' not in publication['mainsnak']['datavalue']['value']:
    #     publication['mainsnak']['datavalue']['value']['precision'] = 9
    #     publication['mainsnak']['datavalue']['value']['calendarmodel'] = 'http://www.wikidata.org/entity/Q1985727'
    #     publication['mainsnak']['datavalue']['value']['time'] = '+' + str(row[2]) + '-01-01T00:00:00Z'
    #     publication['mainsnak']['datavalue']['value']['timezone'] = 0
    #     publication['mainsnak']['datavalue']['value']['before'] = 0
    #     publication['mainsnak']['datavalue']['value']['after'] = 0
    #     enrich_claim(publication, 'Q654724')
    # if row[3] in journals:
    #     enrich_claim(get_claim(item, 'P1433', {"id": journals[row[3]]}, None), 'Q654724')
    # pages = get_claim(item, 'P304', None, None)
    # pages['mainsnak']['datavalue']['value'] = str(row[4]) + '–' + str(row[5]) if row[5] is not None else str(row[4])
    # enrich_claim(pages, 'Q654724')
    # enrich_claim(get_claim(item, 'P478', str(row[6]), None), 'Q654724')
    # if 'en' not in item['labels']:
    #     item['labels']['en'] = {'value': row[7], 'language': 'en'}
    # if 'P1476' not in item['claims']:
    #     enrich_claim(get_claim(item, 'P1476', {'text': row[7], 'language': 'en'}, None), 'Q654724')
    # if row[8] is not None:
    #     doi = get_claim(item, 'P356', None, None)
    #     doi['mainsnak']['datavalue']['value'] = row[8]
    #     enrich_claim(doi, 'Q654724')
    #
    # if str(row[0]) in author and 'P2093' not in item['claims'] and 'P50' not in item['claims']:
    #     for person in author[str(row[0])]:
    #         name = HumanName(re.sub('\.(\w)', '. \g<1>', person[0]))
    #         name.capitalize()
    #         claim = get_claim(item, 'P2093', str(name), None)
    #         claim['qualifiers'] = {'P1545': [create_claim("", 'P1545', person[1])['mainsnak']]}
    #         enrich_claim(claim, 'Q654724')

    for retries in range(1, 3):
        try:
            data = {
                'format': 'json',
                'action': 'wbeditentity',
                'maxlag': '50',
                'data': json.dumps(item),
                'summary': 'batch import from [[Q654724|SIMBAD]] reference "' +
                           item['claims']['P819'][0]['mainsnak']['datavalue']['value'] + '"',
                'token': csrftoken
            }
            if 'id' in item:
                data['id'] = item['id']
                data['baserevid'] = item['lastrevid']
            else:
                data['new'] = 'item'

            response = wdapi.post('https://www.wikidata.org/w/api.php', data=data).content.decode('utf-8').lower()
            if "badtoken" in response:
                r4 = wdapi.get('https://www.wikidata.org/w/api.php',
                               params={'format': 'json', 'action': 'query', 'maxlag': '50', 'meta': 'tokens', })
                if 'query' in r4.json():
                    csrftoken = r4.json()['query']['tokens']['csrftoken']
                    continue
            if not 'error' in response or 'editconflict' in response:
                break

            print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ':' + response,
                  file=sys.stderr)

            time.sleep(3)
        except requests.exceptions.RequestException:
            pass
    # time.sleep(1)
