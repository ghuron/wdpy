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
    ids = list(set([row[0].replace('http://www.wikidata.org/entity/', '') for row in csv if row[0] != 'person'] +
                   [row[1].replace('http://www.wikidata.org/entity/', '') for row in csv if
                    len(row) == 4 and row[1] != 'item']))

    for offset in range(0, len(ids), 50):
        try:
            subset = requests.Session().post('https://www.wikidata.org/w/api.php', params={
                'format': 'json', 'action': 'wbgetentities', 'maxlag': '50', 'props': 'claims|info',
                'ids': '|'.join(ids[offset:offset + 50])}).json()
            if 'entities' in subset:
                result.update(subset['entities'])
        except:
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

for prop in ['P3150', 'P106', 'P19', 'P20', 'P1399', 'P1303', 'P512', 'P119', 'P411',
             'P140', 'P412', 'P101', 'P102', 'P39', 'P69', 'P108', 'P509', 'P21']:  # 'P2962', 'P97', 'P54',
    categories = '?s pq:' + prop + ' ?item; ps:P4224 wd:Q5 FILTER NOT EXISTS {?item wdt:P31 wd:Q3624078} ?cat p:P4224 ?s . ?catArticle schema:about ?cat'
    filter = 'FILTER NOT EXISTS {?person wdt:' + prop + ' ?item}'
    if prop == 'P3150':
        filter = '; p:P569/psv:P569/wikibase:timePrecision 9 .'
    session.headers.update({'Accept': 'application/json'})
    try:
        count = session.post('https://query.wikidata.org/sparql', params={
            'query': 'SELECT (COUNT(*) AS ?count) {' + categories + '}'
        }).json()['results']['bindings'][0]['count']['value']
    except:
        count = 200000

    for offset in range(0, int(count), 1000):
        session.headers.update({'Accept': 'text/csv'})
        try:
            download = session.post('https://query.wikidata.org/sparql', params={
                'query': """
                    PREFIX mw: <http://tools.wmflabs.org/mw2sparql/ontology#>
                    SELECT ?person ?item ?cat (SAMPLE(?site) AS ?s) {
                        hint:Query hint:optimizer "None"
                        { SELECT * {""" + categories + """} LIMIT 1000 OFFSET """ + str(offset) + """}
                        SERVICE <http://tools.wmflabs.org/mw2sparql/sparql> {
                            SELECT * {
                                ?article mw:inCategory ?catArticle
                            }
                        }
                        ?article schema:about ?person .
                        ?person wdt:P31 wd:Q5 """ + filter + """
                        ?article schema:isPartOf ?url . ?site wdt:P856 ?url; wdt:P31/wdt:P279 wd:Q33120876
                        FILTER (?site != wd:Q20789766)
                    } GROUP BY ?person ?item ?cat"""
            })
        except requests.exceptions.RequestException:
            break
        decoded_content = download.content.decode('utf-8')
        if 'exception' in decoded_content:
            print('timeout')

        cr = csv.reader(decoded_content.splitlines(), delimiter=',')
        my_list = list(cr)
        cache = get_items(my_list)

        for row in my_list:
            row = [item.replace('http://www.wikidata.org/entity/', '') for item in row]
            blacklist = ['Q706268', 'Q4740163', 'Q19008', 'Q273809', 'Q317149', 'Q181900', 'Q1030348', 'Q90465',
                         'Q456873', 'Q1322048', 'Q328804', 'Q7251', 'Q4152794', 'Q320154', 'Q763289', 'Q12382773',
                         'Q76326', 'Q9212085', 'Q40939', 'Q154993', 'Q156572', 'Q541599', 'Q76437', 'Q28911612',
                         'Q3123785', 'Q383541', 'Q19928416']
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
                if are_references_dismissable(person['claims']['P569'][0]):
                    months = {'Q108': 1, 'Q109': 2, 'Q110': 3, 'Q118': 4, 'Q119': 5, 'Q120': 6,
                              'Q121': 7, 'Q122': 8, 'Q123': 9, 'Q124': 10, 'Q125': 11, 'Q126': 12}
                    claim = copy.deepcopy(person['claims']['P569'][0])
                    dateValue = claim['mainsnak']['datavalue']['value']
                    dateValue['precision'] = 11
                    newDate = cache[row[1]]['claims']['P361'][0]
                    month = months[newDate['mainsnak']['datavalue']['value']['id']]
                    day = newDate['qualifiers']['P1545'][0]['datavalue']['value']
                    dateValue['time'] = re.sub(r'-\d\d-\d\dT', "-{:02}-{:02}T".format(int(month), int(day)),
                                               dateValue['time'])

            if not claim:
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
            
