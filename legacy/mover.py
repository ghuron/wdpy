#!/usr/bin/python3
import datetime
import json
import sys
import time

import requests


def static_vars(**kwargs):
    def decorate(func):
        for k in kwargs:
            setattr(func, k, kwargs[k])
        return func

    return decorate


@static_vars(csrf_token='badtoken')
def wd_save(wd_session, item):
    # normalize(item['claims'])
    data = {
        'format': 'json', 'action': 'wbeditentity', 'data': json.dumps(item),
        'summary': 'extracting info about star system into separate item',
    }
    if 'id' in item:
        data['id'] = item['id']
        data['baserevid'] = item['lastrevid']
    else:
        data['new'] = 'item'

    print ('eee')

    for retries in range(1, 3):
        try:
            data['token'] = wd_save.csrf_token
            response = wd_session.post('https://www.wikidata.org/w/api.php', data=data).content.decode('utf-8').lower()

            print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ':' + response)

            if 'error' not in response or 'editconflict' in response:
                time.sleep(0.5)
                return json.loads(response)['entity']['id'].upper()

            if 'badtoken' in response:
                r4 = wd_session.get('https://www.wikidata.org/w/api.php',
                                    params={'format': 'json', 'action': 'query', 'meta': 'tokens', })
                if 'query' in r4.json():
                    wd_save.csrf_token = r4.json()['query']['tokens']['csrftoken']
                    continue

            print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ':' + response)

            time.sleep(10)
        except requests.exceptions.RequestException as e:
            print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ':' + e)


def mw_logon(login, password):
    session = requests.Session()
    session.headers.update({'User-Agent': 'simbadbot/0.1 (https://www.wikidata.org/wiki/User:Ghuron)'})

    response = session.get('https://www.wikidata.org/w/api.php', params={
        'format': 'json', 'action': 'query', 'meta': 'tokens', 'type': 'login',
    })

    response = session.post('https://www.wikidata.org/w/api.php', data={
        'format': 'json', 'action': 'login', 'lgname': login, 'lgpassword': password,
        'lgtoken': response.json()['query']['tokens']['logintoken'],
    })
    return session


wdapi = mw_logon(sys.argv[1], sys.argv[2])

qid = 'Q3544119'
response = requests.get('https://www.wikidata.org/w/api.php',
                        params={'format': 'json', 'action': 'wbgetentities',
                                'ids': qid}).json()
if 'entities' in response:
    old = response['entities'][qid]
    item = {'claims': {}}
    item['labels'] = json.loads(json.dumps(old['labels']))
    item['aliases'] = json.loads(json.dumps(old['aliases']))
    # item['descriptions'] = json.loads(json.dumps(old['descriptions']))
    new_id = wd_save(wdapi, item)
