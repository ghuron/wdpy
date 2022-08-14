import csv
import datetime
import json
import sys
import time
from contextlib import closing

import requests
from requests.structures import CaseInsensitiveDict


def wdqs_query(sparql, process=lambda new, existing: new):
    major = CaseInsensitiveDict()
    with requests.Session() as session:
        session.headers.update({'Accept': 'text/csv',
                                'User-Agent': 'simbadbot/0.1 (https://www.wikidata.org/wiki/User:Ghuron)'})

        with closing(session.post('https://query.wikidata.org/sparql', params={'query': sparql}, stream=True)) as r:
            reader = csv.reader(r.iter_lines(decode_unicode='utf-8'), delimiter=',', quotechar='"')
            next(reader)
            for line in reader:
                line = [item.replace('http://www.wikidata.org/entity/', '') for item in line]
                if len(line) == 2:
                    major[line[0]] = process(line[1], major[line[0]] if line[0] in major else [])
    return major

def static_vars(**kwargs):
    def decorate(func):
        for k in kwargs:
            setattr(func, k, kwargs[k])
        return func

    return decorate

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

@static_vars(csrf_token='badtoken')
def wd_save(wd_session, item, summary):
    data = {
        'format': 'json', 'action': 'wbsetclaim', 'maxlag': '15', 'claim': json.dumps(item), 'summary': summary,
    }

    for retries in range(1, 3):
        try:
            data['token'] = wd_save.csrf_token
            response = wd_session.post('https://www.wikidata.org/w/api.php', data=data).content.decode('utf-8').lower()
            if 'error' not in response or 'editconflict' in response:
                time.sleep(0.5)
                return

            if 'badtoken' in response:
                r4 = wd_session.get('https://www.wikidata.org/w/api.php',
                                    params={'format': 'json', 'action': 'query', 'meta': 'tokens', })
                if 'query' in r4.json():
                    wd_save.csrf_token = r4.json()['query']['tokens']['csrftoken']
                    continue

            print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ':' +
                  # item['claims']['P3083'][0]['mainsnak']['datavalue']['value'] + ':' +
                  response, file=sys.stderr)

            wd_session = mw_logon(sys.argv[1], sys.argv[2])
            time.sleep(10)
        except requests.exceptions.RequestException:
            pass

if len(sys.argv) == 3:
    wdapi = mw_logon(sys.argv[1], sys.argv[2])

for offset in range(0, 8200000, 10000):
    statements = wdqs_query('select ?simbad ?id { ?id ps:P3083 ?simbad } LIMIT 10000 OFFSET ' + str(offset))

    script = 'format obj "%OBJECT:-:%MAIN_ID"\r\nquery ' + '\r\nquery '.join([id for id in statements.keys()])
    output = requests.post(
        'http://simbad.u-strasbg.fr/simbad/sim-script', data=
        {'submit': 'submit script',
         'script': 'output console=off script=off\r\n' + script}
    ).content.decode('utf-8').splitlines()
    for line in output:
        cells = line.strip().split(':-:')
        if len(cells) != 2 or cells[0] == ' '.join(cells[1].split()) or not cells[0] in statements:
            continue

        item = json.loads('{"id":"' + statements[cells[0]].replace('-', '$', 1).replace('statement/', '') + \
                          '", "type": "statement", "mainsnak": {"datatype": "' + 'external-id' +
                          '", "property": "' + 'P3083' + '", "snaktype": "value", "datavalue": {"type": "' +
                          'string' + '", "value": "' + ' '.join(cells[1].split()) + '"}}}')

        wd_save(wdapi, item, 'was ' + cells[0] )
    time.sleep(5)
