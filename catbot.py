#!/usr/bin/python3
import copy
import csv
import datetime
import json
import re
import sys
import time
import uuid

import requests


def are_references_dismissable(statement):
    if 'qualifiers' in statement:
        return False
    if not 'references' in statement:
        return True
    if len(statement['references']) == 1:
        if statement['references'][0]['snaks-order'] == ['P143']:
            return True
    return False


def get_items(csv):
    result = {}
    ids = list(set([qid for row in csv for qid in row[:3] if qid.startswith('Q')]))

    for offset in range(0, len(ids), 50):
        try:
            subset = requests.get('https://www.wikidata.org/w/api.php', params={
                'format': 'json', 'action': 'wbgetentities', 'maxlag': '50', 'props': 'claims|info',
                'ids': '|'.join(ids[offset:offset + 50])}).json()
            if 'entities' in subset:
                result.update(subset['entities'])
        except Exception as e:
            pass

    return result


def get_superclasses(item, propertyId):
    super_classes = []
    if 'claims' in item:
        if 'P279' in item['claims']:
            for statement in item['claims']['P279']:
                if 'datavalue' in statement['mainsnak']:
                    super_classes.append(statement['mainsnak']['datavalue']['value']['id'])
        if 'P131' in item['claims'] and propertyId in ['P19', 'P20', 'P119']:
            for statement in item['claims']['P131']:
                if 'datavalue' in statement['mainsnak']:
                    super_classes.append(statement['mainsnak']['datavalue']['value']['id'])
        if 'P361' in item['claims'] and propertyId in ['P69']:
            for statement in item['claims']['P361']:
                if 'datavalue' in statement['mainsnak']:
                    super_classes.append(statement['mainsnak']['datavalue']['value']['id'])
    return super_classes


def is_datetime_updateable(source, target):
    if source['precision'] > 10 or source['precision'] >= target['precision'] or \
            source['calendarmodel'] != target['calendarmodel']:
        return False
    digits_in_year = source['time'].find('-', 1) - 1
    compare_till = source['precision'] - (8 - digits_in_year)
    if source['precision'] == 10:
        compare_till = compare_till + 3
    return source['time'][:compare_till] == target['time'][:compare_till]


wdapi = requests.Session()
wdapi.headers.update(
    {'User-Agent': 'catbot/0.0 (https://github.com/ghuron/wdpy; https://www.wikidata.org/wiki/User:Ghuron)'})

# get login token
r1 = wdapi.get('https://www.wikidata.org/w/api.php', params={
    'format': 'json',
    'action': 'query',
    'meta': 'tokens',
    'type': 'login',
})

# log in
r2 = wdapi.post('https://www.wikidata.org/w/api.php', data={
    'format': 'json',
    'action': 'login',
    'lgname': sys.argv[1],
    'lgpassword': sys.argv[2],
    'lgtoken': r1.json()['query']['tokens']['logintoken'],
})

csrftoken = '<badtoken>'

for prop in ['P131', 'P17', 'P569', 'P840', 'P59', 'P881', 'P6087', 'P571', 'P53', 'P403', 'P39', 'P19', 'P20', 'P176',
             'P58', 'P1056', 'P621', 'P522', 'P462', 'P136', 'P407', 'P577', 'P825', 'P364', 'P2632', 'P161',
             'P138', 'P1046', 'P61', 'P400', 'P50', 'P170', 'P611', 'P162', 'P86', 'P57', 'P410', 'P4884',
             'P264', 'P412', 'P413', 'P54', 'P1399', 'P1303', 'P512', 'P119', 'P411', 'P140', 'P101',
             'P102', 'P69', 'P108', 'P509', 'P21', 'P175', 'P3150', 'P106']:
    # 'P2962', 'P97', 'P171', 'P405', 'P141', 'P1344',
    categories = '?s pq:' + prop + ' ?item; ps:P4224 ?type . ?cat p:P4224 ?s'
    filter = 'FILTER NOT EXISTS {?person wdt:' + prop + ' ?item}'
    if prop == 'P3150':
        filter = '; p:P569/psv:P569/wikibase:timePrecision ?precision FILTER (?precision = 9 || ?precision = 10) .'
    else:
        if prop == 'P405':
            filter = 'FILTER NOT EXISTS {?person p:P225/pq:P405 []}'

    with requests.Session() as wdqs:
        wdqs.headers.update({'Accept': 'application/json',
                             'User-Agent': 'catbot/0.0 (https://github.com/ghuron/wdpy; https://www.wikidata.org/wiki/User:Ghuron)'})
        try:
            count = wdqs.post('https://query.wikidata.org/sparql', params={
                'query': 'SELECT (COUNT(*) AS ?count) {' + categories + '}'
            }).json()['results']['bindings'][0]['count']['value']
        except:
            count = 60000

    for offset in range(0, int(count), 10):
        with requests.Session() as wdqs:
            wdqs.headers.update({'Accept': 'text/csv',
                                 'User-Agent': 'catbot/0.0 (https://github.com/ghuron/wdpy; https://www.wikidata.org/wiki/User:Ghuron)'})
            try:
                download = wdqs.post('https://query.wikidata.org/sparql', params={
                    'query': """
                        PREFIX mw: <http://tools.wmflabs.org/mw2sparql/ontology#>
                        SELECT ?person ?item ?cat (SAMPLE(?site) AS ?s) WITH {
                            SELECT * {""" + categories + """} LIMIT 10 OFFSET """ + str(offset) + """
                        } as %q {    
                            hint:Query hint:optimizer "None"
                            INCLUDE %q
                            ?catArticle schema:about ?cat
                            SERVICE <http://tools.wmflabs.org/mw2sparql/sparql> {
                                SELECT * {
                                    ?article mw:inCategory ?catArticle
                                }
                            }
                            ?article schema:about ?person .
                            ?person wdt:P31 ?type """ + filter + """
                            ?article schema:isPartOf ?url . ?site wdt:P856 ?url; wdt:P31/wdt:P279 wd:Q33120876
                            FILTER (?site != wd:Q20789766)
                        } GROUP BY ?person ?item ?cat"""
                })
            except requests.exceptions.RequestException:
                break
        if download.status_code != 200:
            print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ': Query error: ' + str(download.status_code),
                  file=sys.stderr)
            continue

        decoded_content = download.content.decode('utf-8').replace('http://www.wikidata.org/entity/', '')

        my_list = list(csv.reader(decoded_content.splitlines(), delimiter=','))[1:]
        cache = get_items(my_list)

        for row in my_list:
            blacklist = ['Q706268', 'Q4740163', 'Q19008', 'Q273809', 'Q317149', 'Q181900', 'Q1030348', 'Q90465',
                         'Q456873', 'Q1322048', 'Q328804', 'Q7251', 'Q4152794', 'Q320154', 'Q763289', 'Q12382773',
                         'Q76326', 'Q9212085', 'Q40939', 'Q154993', 'Q156572', 'Q541599', 'Q76437', 'Q28911612',
                         'Q3123785', 'Q383541', 'Q19928416', 'Q17131', 'Q232725', 'Q43067', 'Q122386', 'Q2995934',
                         'Q41635', 'Q122386', 'Q37577', 'Q713439', 'Q43274', 'Q509124', 'Q11031', 'Q345', 'Q3349145',
                         'Q3117649', 'Q3180990', 'Q4730963', 'Q167545', 'Q570982', 'Q145746', 'Q9203192']
            if len(row) != 4 or row[0] in blacklist or not row[0].startswith('Q'):
                continue

            if row[0] in cache:
                person = cache[row[0]]
            else:
                continue

            value = '"datatype": "wikibase-item", "datavalue": {"type": "wikibase-entityid", "value": ' \
                    '{"entity-type": "item", "id":"' + row[1] + '"}}, "property": "' + prop + '", "snaktype": "value"'

            if prop in ('P569', 'P571', 'P577', 'P621'):
                if row[2] in cache and \
                        'qualifiers' in cache[row[2]]['claims']['P4224'][0] and \
                        'datavalue' in cache[row[2]]['claims']['P4224'][0]['qualifiers'][prop][0]:
                    value = '"property": "' + prop + '", "snaktype": "value", "datatype": "time", "datavalue": ' + \
                            json.dumps(cache[row[2]]['claims']['P4224'][0]['qualifiers'][prop][0]['datavalue'])
                else:
                    continue

            claim = json.loads('{"id":"?uuid", "type": "statement", "mainsnak": {?item}}'
                               .replace('?uuid', row[0] + '$' + str(uuid.uuid4())).replace('?item', value)
                               )

            if prop in person['claims']:
                if not prop in ('P569', 'P571', 'P577', 'P621'):
                    replaceable = []
                    if row[1] in cache:
                        replaceable = get_superclasses(cache[row[1]], prop)

                    for s in person['claims'][prop]:
                        if not 'rank' in claim:  # adding new claim is impossible
                            claim = {}
                        if 'datavalue' in s['mainsnak']:
                            entityId = s['mainsnak']['datavalue']['value']['id']
                            if entityId == row[1]:  # exactly this statement already exists
                                claim = {}  # any modification is prohibited unnecessary
                                break
                            if entityId in replaceable:  # we can update statement with more accurate info
                                if are_references_dismissable(s):  # no "real" sources
                                    if (prop != 'P106' or entityId == 'Q901') and \
                                            (prop != 'P140' or entityId != 'Q5043') and \
                                            (prop != 'P136') and \
                                            (prop != 'P407'):
                                        claim = copy.deepcopy(s)
                                        claim['mainsnak']['datavalue']['value']['id'] = row[1]
                else:
                    claim2 = claim
                    claim = {}
                    if are_references_dismissable(person['claims'][prop][0]) and len(person['claims'][prop]) == 1:
                        if 'datavalue' in person['claims'][prop][0]['mainsnak']:
                            if is_datetime_updateable(person['claims'][prop][0]['mainsnak']['datavalue']['value'],
                                                      claim2['mainsnak']['datavalue']['value']):
                                claim = claim2
                                claim['id'] = person['claims'][prop][0]['id']

            if prop == 'P405':
                claim = {}
                if len(person['claims']['P225']) == 1 and are_references_dismissable(person['claims']['P225'][0]):
                    claim = json.loads(json.dumps(person['claims']['P225'][0]))
                    claim['qualifiers'] = json.loads('{"P405": [{' + value + '}]}')

            if prop == 'P3150':
                claim2 = claim
                claim = {}
                if are_references_dismissable(person['claims']['P569'][0]) and len(person['claims']['P569']) == 1:
                    months = {'Q108': 1, 'Q109': 2, 'Q110': 3, 'Q118': 4, 'Q119': 5, 'Q120': 6,
                              'Q121': 7, 'Q122': 8, 'Q123': 9, 'Q124': 10, 'Q125': 11, 'Q126': 12}
                    claim2['mainsnak'] = json.loads(json.dumps(person['claims']['P569'][0]['mainsnak']))
                    claim2['id'] = person['claims']['P569'][0]['id']
                    dateValue = claim2['mainsnak']['datavalue']['value']
                    newDate = cache[row[1]]['claims']['P361'][0]
                    month = months[newDate['mainsnak']['datavalue']['value']['id']]
                    day = newDate['qualifiers']['P1545'][0]['datavalue']['value']
                    if dateValue['precision'] == 9 or (dateValue['precision'] == 10 and
                                                       int(re.search('-(\d\d)-', dateValue['time']).group(1)) == month):
                        dateValue['precision'] = 11
                        dateValue['time'] = re.sub(r'-\d\d-\d\dT', "-{:02}-{:02}T".format(int(month), int(day)),
                                                   dateValue['time'])
                        claim = claim2

            if not claim or (row[1] in json.dumps(person) and prop in ['P162', 'P86', 'P175', 'P57', 'P170']):
                continue

            claim['references'] = json.loads('[{"snaks": {"P143": [{"snaktype": '
                                             '"value", "property": "P143", "datatype": "wikibase-item", "datavalue": '
                                             '{"type": "wikibase-entityid", "value": {"entity-type": "item", "id": "?source"}}}]}}]'
                                             .replace('?source', row[3]))

            for retries in range(1, 3):
                response = wdapi.post('https://www.wikidata.org/w/api.php', data={
                    'format': 'json',
                    'action': 'wbsetclaim',
                    'maxlag': '50',
                    'claim': json.dumps(claim),
                    'baserevid': person['lastrevid'],
                    'summary': 'because included in the [[' + row[2] + ']]',
                    'token': csrftoken
                }).content.decode('utf-8').lower()

                if "badtoken" in response:
                    r4 = wdapi.get('https://www.wikidata.org/w/api.php',
                                   params={'format': 'json', 'action': 'query', 'maxlag': '50', 'meta': 'tokens', })
                    if 'query' in r4.json():
                        csrftoken = r4.json()['query']['tokens']['csrftoken']
                        continue

                if "abusefilter-warning-p225" in response:
                    continue

                if "anonymous" in response:
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

                if not 'error' in response or 'editconflict' in response:
                    continue

                print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ':' + response, file=sys.stderr)

                time.sleep(60)

            del cache[row[0]]
            time.sleep(10)
