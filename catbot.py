#!/usr/bin/python3
import copy
import csv
import json
import re
import time
import uuid

import requests


def are_references_dismissable(statement):
    if not 'references' in statement:
        return True
    if len(statement['references']) == 1:
        if statement['references'][0]['snaks-order'] == ['P143']:
            return True
    return False


def get_items(csv):
    result = {}
    ids = list(set([row[0].replace('http://www.wikidata.org/entity/', '') for row in csv if row[0].startswith('http')] +
                   [row[1].replace('http://www.wikidata.org/entity/', '') if row[1].startswith('http') else
                    row[1].replace('http://www.wikidata.org/entity/', '') for row in csv if len(row) == 4]))

    for offset in range(0, len(ids), 50):
        try:
            subset = requests.Session().post('https://www.wikidata.org/w/api.php', params={
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


session = requests.Session()

# get login token
r1 = session.get('https://www.wikidata.org/w/api.php', params={
    'format': 'json',
    'action': 'query',
    'meta': 'tokens',
    'type': 'login',
})

# log in
r2 = session.post('https://www.wikidata.org/w/api.php', data={
    'format': 'json',
    'action': 'login',
    'lgname': '<username>',
    'lgpassword': '<password>',
    'lgtoken': r1.json()['query']['tokens']['logintoken'],
})

for prop in ['P170','P840', 'P611', 'P162', 'P86', 'P175', 'P57', 'P410', 'P264', 'P413', 'P54', 'P3150', 'P106', 'P19', 'P20',
             'P1399', 'P1303', 'P512', 'P119', 'P411', 'P140', 'P412', 'P101', 'P102', 'P39', 'P69', 'P108', 'P509',
             'P21']:  # 'P2962', 'P97',
    categories = '?s pq:' + prop + ' ?item; ps:P4224 ?type . ?cat p:P4224 ?s'
    filter = 'FILTER NOT EXISTS {?person wdt:' + prop + ' ?item}'
    if prop == 'P3150':
        filter = '; p:P569/psv:P569/wikibase:timePrecision ?precision FILTER (?precision = 9 || ?precision = 10) .'
    session.headers.update({'Accept': 'application/json'})
    try:
        count = session.post('https://query.wikidata.org/sparql', params={
            'query': 'SELECT (COUNT(*) AS ?count) {' + categories + '}'
        }).json()['results']['bindings'][0]['count']['value']
    except:
        count = 60000

    for offset in range(0, int(count), 10):
        session.headers.update({'Accept': 'text/csv'})
        try:
            download = session.post('https://query.wikidata.org/sparql', params={
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
        decoded_content = download.content.decode('utf-8')
        if download.status_code != 200:
            print('Error: ' + str(download.status_code))

        cr = csv.reader(decoded_content.splitlines(), delimiter=',')
        my_list = list(cr)
        cache = get_items(my_list[1:])

        for row in my_list:
            row = [item.replace('http://www.wikidata.org/entity/', '') for item in row]
            blacklist = ['Q706268', 'Q4740163', 'Q19008', 'Q273809', 'Q317149', 'Q181900', 'Q1030348', 'Q90465',
                         'Q456873', 'Q1322048', 'Q328804', 'Q7251', 'Q4152794', 'Q320154', 'Q763289', 'Q12382773',
                         'Q76326', 'Q9212085', 'Q40939', 'Q154993', 'Q156572', 'Q541599', 'Q76437', 'Q28911612',
                         'Q3123785', 'Q383541', 'Q19928416', 'Q17131', 'Q232725', 'Q43067', 'Q122386', 'Q2995934',
                         'Q41635', 'Q122386', 'Q37577', 'Q713439', 'Q43274', 'Q509124', 'Q11031', 'Q345', 'Q3349145',
                         'Q3117649']
            if len(row) != 4 or row[0] in blacklist or not row[0].startswith('Q') or not row[1].startswith('Q'):
                continue

            if row[0] in cache:
                person = cache[row[0]]
            else:
                continue

            claim = json.loads('{"id":"?uuid", "type": "statement", "mainsnak": {"snaktype": "value", '
                               '"property": "?prop", "datatype": "wikibase-item", "datavalue": {"type": "wikibase-entityid",'
                               '"value": {"entity-type": "item", "id": "?item"}}}}'
                               .replace('?uuid', row[0] + '$' + str(uuid.uuid4())).replace('?item', row[1])
                               .replace('?prop', prop))

            if prop in person['claims']:
                replaceable = []
                if row[1] in cache:
                    replaceable = get_superclasses(cache[row[1]], prop)

                for s in person['claims'][prop]:
                    if not 'rank' in claim:  # adding new claim is impossible
                        claim = {}
                    if 'datavalue' in s['mainsnak']:
                        entityId = s['mainsnak']['datavalue']['value']['id']
                        if entityId == row[1]:  # exactly this statement already exists
                            break  # any modification is unnecessary
                        if entityId in replaceable:  # we can update statement with more accurate info
                            if not ('references' in s) or \
                                    (len(s['references']) == 1 and s['references'][0]['snaks-order'] == [
                                        'P143']):  # no "real" sources
                                if (prop != 'P106' or entityId == 'Q901') and (prop != 'P140' or entityId != 'Q5043'):
                                    claim = copy.deepcopy(s)
                                    claim['mainsnak']['datavalue']['value']['id'] = row[1]
                                    break

            if prop == 'P3150':
                claim = {}
                if are_references_dismissable(person['claims']['P569'][0]) and len(person['claims']['P569']) == 1:
                    months = {'Q108': 1, 'Q109': 2, 'Q110': 3, 'Q118': 4, 'Q119': 5, 'Q120': 6,
                              'Q121': 7, 'Q122': 8, 'Q123': 9, 'Q124': 10, 'Q125': 11, 'Q126': 12}
                    claim = copy.deepcopy(person['claims']['P569'][0])
                    dateValue = claim['mainsnak']['datavalue']['value']
                    newDate = cache[row[1]]['claims']['P361'][0]
                    month = months[newDate['mainsnak']['datavalue']['value']['id']]
                    day = newDate['qualifiers']['P1545'][0]['datavalue']['value']
                    if dateValue['precision'] == 9 or (dateValue['precision'] == 10 and
                                                       int(re.search('-(\d\d)-', dateValue['time']).group(1)) == month):
                        dateValue['precision'] = 11
                        dateValue['time'] = re.sub(r'-\d\d-\d\dT', "-{:02}-{:02}T".format(int(month), int(day)),
                                                   dateValue['time'])
                    else:
                        claim = {}

            if not claim or (row[1] in json.dumps(person) and prop in ['P162', 'P86', 'P175', 'P57', 'P170']):
                continue

            claim['references'] = json.loads('[{"snaks": {"P143": [{"snaktype": '
                                             '"value", "property": "P143", "datatype": "wikibase-item", "datavalue": '
                                             '{"type": "wikibase-entityid", "value": {"entity-type": "item", "id": "?source"}}}]}}]'
                                             .replace('?source', row[3]))

            # get edit token
            token = session.get('https://www.wikidata.org/w/api.php', params={
                'format': 'json',
                'action': 'query',
                'maxlag': '50',
                'meta': 'tokens',
            }).json()['query']['tokens']['csrftoken']

            # Save modified claim
            r4 = session.post('https://www.wikidata.org/w/api.php', data={
                'format': 'json',
                'action': 'wbsetclaim',
                'maxlag': '50',
                'claim': json.dumps(claim),
                'baserevid': person['lastrevid'],
                'summary': 'because included in the [[' + row[2] + ']]',
                'token': token
            })

            del cache[row[0]]

            time.sleep(0.5)
