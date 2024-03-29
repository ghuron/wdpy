#!/usr/bin/python3
import csv
import datetime
import json
import math
import random
import re
import sys
import time
import uuid
from contextlib import closing
from decimal import Decimal, DecimalException
from inspect import currentframe, getframeinfo

import dateutil.parser
import requests
from astropy import coordinates as coord


def static_vars(**kwargs):
    def decorate(func):
        for k in kwargs:
            setattr(func, k, kwargs[k])
        return func

    return decorate


def wdqs_query(sparql, process=lambda new, existing: new):
    major = {}
    with requests.Session() as session:
        session.headers.update({'Accept': 'text/csv',
                                'User-Agent': 'simbadbot/0.1 (https://www.wikidata.org/wiki/User:Ghuron)'})
        download = session.post('https://query.wikidata.org/sparql', params={'query': sparql})
        download.raise_for_status()
        decoded_content = download.content.decode('utf-8')

        cr = csv.reader(decoded_content.splitlines(), delimiter=',')
        my_list = list(cr)
        for line in my_list[1:]:
            line = [item.replace('http://www.wikidata.org/entity/', '') for item in line]
            if len(line) == 2:
                major[line[0]] = process(line[1], major[line[0]] if line[0] in major else [])
    return major


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


@static_vars(pubs=wdqs_query('select ?item ?date { ?item wdt:P819 []; OPTIONAL { ?item wdt:P577 ?date }}'))
def get_latest_publication_date(claim):
    latest = dateutil.parser.parse('1800-01-01T00:00:00Z')
    get_latest_publication_date.pubs['Q66617668'] = '1924-01-01T00:00:00Z'
    if 'references' in claim:
        for ref in claim['references']:
            if 'P248' not in ref['snaks']: continue
            if ref['snaks']['P248'][0]['datavalue']['value']['id'] in get_latest_publication_date.pubs:
                text = get_latest_publication_date.pubs[ref['snaks']['P248'][0]['datavalue']['value']['id']]
                try:
                    if dateutil.parser.parse(text) > latest:
                        latest = dateutil.parser.parse(text)
                except ValueError as e:
                    print('Text "{}" line {} exception {}'.format(text, getframeinfo(currentframe()).lineno, e))

            else:
                if ref['snaks']['P248'][0]['datavalue']['value']['id'] == 'Q654724':
                    latest = dateutil.parser.parse('1800-01-02T00:00:00Z')
    return latest


def normalize(claims):
    for prop_id in ['P215', 'P881', 'P1090', 'P2214', 'P2215', 'P2216', 'P2227', 'P2386', 'P2583', 'P4296', 'P6879',
                    'P7015']:
        if prop_id in claims:
            if len(claims[prop_id]) > 1:
                latest = dateutil.parser.parse('1800-01-01T00:00:00Z')
                for statements in claims[prop_id]:
                    published = get_latest_publication_date(statements)
                    if published > latest:
                        latest = published
                for statements in claims[prop_id]:
                    published = get_latest_publication_date(statements)
                    if published == dateutil.parser.parse('1800-01-01T00:00:00Z'):
                        continue
                    if published < latest:
                        statements['rank'] = 'deprecated'
                    else:
                        statements['rank'] = 'normal'


@static_vars(csrf_token='badtoken')
def wd_save(wd_session, item):
    normalize(item['claims'])
    data = {
        'format': 'json', 'action': 'wbeditentity', 'maxlag': '5', 'data': json.dumps(item),
        'summary': 'batch import from [[Q654724|SIMBAD]] for object "' +
                   item['claims']['P3083'][0]['mainsnak']['datavalue']['value'] + '"',
    }
    if 'id' in item:
        data['id'] = item['id']
        data['baserevid'] = item['lastrevid']
    else:
        data['new'] = 'item'

    for retries in range(1, 3):
        try:
            data['token'] = wd_save.csrf_token
            response = wd_session.post('https://www.wikidata.org/w/api.php', data=data).content.decode('utf-8').lower()
            if 'error' not in response or 'editconflict' in response:
                time.sleep(0.5)
                return json.loads(response)['entity']['id'].upper()

            if 'badtoken' in response:
                r4 = wd_session.get('https://www.wikidata.org/w/api.php',
                                    params={'format': 'json', 'action': 'query', 'meta': 'tokens', })
                if 'query' in r4.json():
                    wd_save.csrf_token = r4.json()['query']['tokens']['csrftoken']
                    continue

            print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ':' +
                  item['claims']['P3083'][0]['mainsnak']['datavalue']['value'] + ':' + response, file=sys.stderr)

            time.sleep(10)
        except requests.exceptions.RequestException:
            pass


@static_vars(types=None)
def create_claim(entity_id, property_id, target):
    if create_claim.types is None:
        create_claim.types = wdqs_query('SELECT ?prop ?type { ?prop wikibase:propertyType ?type }')
        for prop in create_claim.types:
            create_claim.types[prop] = create_claim.types[prop].replace('http://wikiba.se/ontology#', ''). \
                replace('WikibaseItem', 'wikibase-entityid').replace('ExternalId', 'external-id').lower()

    serialized_value = '{}'
    value_type = create_claim.types[property_id] if create_claim.types[property_id] != 'external-id' else 'string'
    if target is not None:
        serialized_value = ('"' + target + '"') if isinstance(target, str) else json.dumps(target)

    return json.loads('{"id":"' + entity_id + '$' + str(uuid.uuid4()) + \
                      '", "type": "statement", "mainsnak": {"datatype": "' + create_claim.types[property_id] +
                      '", "property": "' + property_id + '", "snaktype": "value", "datavalue": {"type": "' +
                      value_type + '", "value": ' + serialized_value + '}}}')


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
                    if float(source['amount']) == float(target['amount']):
                        if 'lowerBound' in source and 'lowerBound' in target and float(source['lowerBound']) == float(
                                target['lowerBound']):
                            return candidate
                        if 'lowerBound' not in target:
                            return candidate
    else:
        entity['claims'][property_id] = []

    new_claim = create_claim(entity['id'] if 'id' in entity else '', property_id, target)

    if 'id' not in entity:
        del new_claim['id']
    entity['claims'][property_id].append(new_claim)
    return new_claim


@static_vars(articles=wdqs_query('select ?code ?article { ?article wdt:P819 ?code }'))
def enrich_claim(target, bibcode=''):
    ref_id = enrich_claim.articles[bibcode] if bibcode in enrich_claim.articles else 'Q654724'

    if 'references' not in target:
        target['references'] = []

    for ref in target['references']:  # get rid of simbads
        if 'P248' in ref['snaks']:
            if ref['snaks']['P248'][0]['datavalue']['value']['id'] == 'Q654724' and ref_id != 'Q654724':
                target['references'].remove(ref)

    for ref in target['references']:  # if the same reference exists - get our of here
        if 'P248' in ref['snaks']:
            if ref['snaks']['P248'][0]['datavalue']['value']['id'] == ref_id:
                return

    target['references'].append({'snaks': {'P248': [create_claim('', 'P248', {'id': ref_id})['mainsnak']]}})


def type_can_be_added(instance_of, claims, super_types):
    if 'P31' in claims:
        for instance_claim in claims['P31']:
            p31 = instance_claim['mainsnak']['datavalue']['value']['id']
            if instance_of == p31:
                enrich_claim(instance_claim)
                return False
            if p31 in super_types and instance_of in super_types[p31]:
                return False
    return True


def format_float(figure, digits):
    if 0 <= int(digits) < 20:
        return ('{0:.' + digits + 'f}').format(Decimal(figure))
    else:
        return str(Decimal(figure))


def set_amount(quantity, digits, unit, bound=None):
    if unit != '1':
        unit = 'http://www.wikidata.org/entity/' + unit
    data_value = {'amount': format_float(quantity, digits), 'unit': unit}
    if bound is not None and bound != '':
        data_value['lowerBound'] = format_float(Decimal(quantity) - Decimal(bound), digits)
        data_value['upperBound'] = format_float(Decimal(quantity) + Decimal(bound), digits)

    return data_value


def tap_query(sql):
    result = {}
    with closing(requests.post('http://simbad.u-strasbg.fr/simbad/sim-tap/sync', params={
        'request': 'doQuery', 'lang': 'adql', 'format': 'csv', 'maxrec': -1, 'query': sql,
    }, stream=True)) as r:
        reader = csv.reader(r.iter_lines(decode_unicode='utf-8'), delimiter=',', quotechar='"')
        next(reader, None)  # skip the headers
        for line in reader:
            for i in range(1, len(line)):
                if isinstance(line[i], str):
                    line[i] = ' '.join(line[i].split())
            if line[0] not in result:
                result[line[0]] = []
            result[line[0]].append(line[1:])
    return result


# sim_object = wdqs_query2('SELECT ?simbad ?item { ?item wdt:P3083 ?simbad }')

blacklist = wdqs_query('SELECT ?what ?intermediate {?what wdt:P31 wd:Q17444909 . ?intermediate wdt:P279+ ?what}',
                       lambda new, existing: existing + [new])

var_types = wdqs_query('SELECT ?gcvs ?item {?item wdt:P279+ wd:Q6243; p:P528[ps:P528 ?gcvs; pq:P972 wd:Q222662]}')
constellations = wdqs_query('SELECT ?name ?item {?item wdt:P31 wd:Q8928; wdt:P1813 ?name}')

otypes = {'?': 'Q6999', 'ev': 'Q2680861', 'Rad': 'Q1931185', 'mR': 'Q67201491', 'cm': 'Q67201524', 'mm': 'Q67201561',
          'smm': 'Q67201574', 'HI': 'Q67201586', 'rB': 'Q15809070', 'Mas': 'Q1341811', 'IR': 'Q67206691',
          'FIR': 'Q67206701', 'NIR': 'Q67206785', 'red': 'Q71797619', 'ERO': 'Q71797766', 'blu': 'Q71798532',
          'UV': 'Q71798788', 'X': 'Q2154519', 'ULX': 'Q129686', 'gam': 'Q71962386', 'gB': 'Q22247', 'grv': 'Q71962637',
          'gLe': 'Q185243', 'GWE': 'Q24748034', '..?': 'Q72053253', 'G?': 'Q72053617', 'C?G': 'Q72054258',
          'Gr?': 'Q72533545', '**?': 'Q72534196', 'EB?': 'Q72534536', 'Sy?': 'Q72672560', 'CV?': 'Q72704237',
          'No?': 'Q72705413', 'Pec?': 'Q72802810', 'Y*?': 'Q72802977', 'RB?': 'Q72802727', 'vid': 'Q845371',
          'SCG': 'Q27521', 'ClG': 'Q204107', 'GrG': 'Q1491746', 'CGG': 'Q71963409', 'PaG': 'Q28738741', 'IG': 'Q644507',
          'Gl?': 'Q72803708', 'Cl*': 'Q168845', 'GlC': 'Q11276', 'OpC': 'Q11387', 'As*': 'Q9262', 'St*': 'Q935337',
          'MGr': 'Q19364629', '**': 'Q13890', 'EB*': 'Q1457376', 'Al*': 'Q24452', 'bL*': 'Q830831', 'WU*': 'Q691269',
          'SB*': 'Q1993624', 'El*': 'Q1332364', 'Sy*': 'Q18393176', 'CV*': 'Q1059564', 'DQ*': 'Q1586249',
          'AM*': 'Q294562', 'NL*': 'Q27995884', 'No*': 'Q6458', 'DN*': 'Q244264', 'XB*': 'Q5961', 'LXB': 'Q71963788',
          'HXB': 'Q71963720', 'ISM': 'Q41872', 'Cld': 'Q1054444', 'DNe': 'Q204194', 'RNe': 'Q203958', 'MoC': 'Q272447',
          'glb': 'Q213936', 'SFR': 'Q27150479', 'HVC': 'Q1621824', 'HII': 'Q11282', 'PN': 'Q13632', 'SNR': 'Q207436',
          'out': 'Q12053157', 'HH': 'Q50048', '*': 'Q523', 'V*?': 'Q66521853', 'Pe*': 'Q1142192', 'HB*': 'Q72803426',
          'Y*O': 'Q497654', 'Ae*': 'Q1044693', 'Em*': 'Q72803622', 'Be*': 'Q812800', 'BS*': 'Q5848', 'RG*': 'Q66619666',
          'C*': 'Q130019', 'S*': 'Q1153392', 'sg*': 'Q193599', 's*r': 'Q5898', 's*y': 'Q1142197', 's*b': 'Q1048372',
          'HS*': 'Q54231557', 'pA*': 'Q66619774', 'WD*': 'Q5871', 'ZZ*': 'Q136562', 'LM*': 'Q72803170',
          'BD*': 'Q101600', 'N*': 'Q4202', 'OH*': 'Q2007502', 'pr*': 'Q1062509', 'TT*': 'Q6232', 'WR*': 'Q6251',
          'PM*': 'Q2247863', 'HV*': 'Q1036344', 'V*': 'Q6243', 'Ir*': 'Q1141054', 'Or*': 'Q1352333', 'RI*': 'Q71965844',
          'Er*': 'Q1362543', 'Fl*': 'Q285400', 'FU*': 'Q957044', 'RC*': 'Q920941', 'Ro*': 'Q15917122',
          'a2*': 'Q1141942', 'Psr': 'Q4360', 'BY*': 'Q797219', 'RS*': 'Q1392913', 'Pu*': 'Q353834', 'RR*': 'Q726242',
          'Ce*': 'Q188593', 'dS*': 'Q836976', 'RV*': 'Q727379', 'WV*': 'Q936076', 'bC*': 'Q764463', 'cC*': 'Q10451997',
          'gD*': 'Q1493194', 'SX*': 'Q24319', 'LP*': 'Q1153690', 'Mi*': 'Q744691', 'sr*': 'Q1054411', 'SN*': 'Q3937',
          'su*': 'Q3132741', 'Pl?': 'Q18611609', 'Pl': 'Q44559', 'G': 'Q318', 'GiC': 'Q318', 'BiC': 'Q1151284',
          'GiG': 'Q318', 'DLA': 'Q5212927', 'rG': 'Q217012', 'LSB': 'Q115518', 'EmG': 'Q72802508', 'SBG': 'Q726611',
          'AGN': 'Q46587', 'LIN': 'Q2557101', 'SyG': 'Q213930', 'Sy1': 'Q71965429', 'Sy2': 'Q71965638',
          'Bla': 'Q221221', 'BLL': 'Q195385', 'OVV': 'Q7073158', 'QSO': 'Q83373', 'LP?': 'Q523', '*i*': 'Q523',
          'C*?': 'Q523', 'cor': 'Q97570336', 'S*?': 'Q523', 'le?': 'Q71962637', 'BD?': 'Q3132741', 'WD?': 'Q523'}

oid = list(range(0, 15500000, 300))

wdapi = mw_logon(sys.argv[1], sys.argv[2])

for idx in range(0, len(oid) - 2):
    time.sleep(2)
# while True:
#     idx = random.randint(0, len(oid) - 2)
    basic = tap_query('''SELECT oid, main_id, 
                            morph_type, morph_bibcode, 
                            sp_type, sp_bibcode, 
                            otype_txt, 
                            ra, ra_prec, dec, dec_prec, coo_bibcode, 
                            plx_value, plx_err, plx_prec, plx_bibcode,
                            pmdec, pm_err_min, pmdec_prec, pmra, pm_err_maj, pmra_prec, pm_bibcode,
                            rvz_radvel, rvz_err, rvz_radvel_prec, rvz_bibcode
                        FROM basic WHERE ( oid BETWEEN {} AND {} )'''.format(oid[idx], oid[idx + 1]))

    query = ''
    for id in basic:
        query += "'" + basic[id][0][0] + "' "
    try:
        sim_object = wdqs_query('SELECT ?simbad ?item { ?item wdt:P3083 ?simbad VALUES ?simbad {' + query + '}}')
    except requests.exceptions.HTTPError:
        continue

    variability = None

    for id in basic:
        row = basic[id][0]
        if row[0] not in sim_object:
            item = {'claims': {}, 'labels': {'en': {'value': row[0], 'language': 'en'}}}
            get_claim(item, 'P3083', row[0], None)
            # continue
        else:
            continue
            # response = requests.get('https://www.wikidata.org/w/api.php',
            #                         params={'format': 'json', 'action': 'wbgetentities',
            #                                 'props': 'claims|info', 'ids': sim_object[row[0]]}).json()
            # if 'entities' not in response:
            #     continue
            # item = response['entities'][sim_object[row[0]]]

        if variability is None:
            variability = tap_query('''SELECT oidref, vartyp, bibcode from mesVar WHERE oidref BETWEEN {} AND {}'''
                                    .format(oid[idx], oid[idx + 1]))

            parent = tap_query('''SELECT DISTINCT l.child AS child, p.main_id AS parent
                            FROM h_link l INNER JOIN basic p ON p.oid = l.parent
                            WHERE membership=100 AND ( l.child BETWEEN {} AND {} )'''.format(oid[idx], oid[idx + 1]))

            distance = tap_query('''SELECT oidref, dist, dist_prec, plus_err, unit, bibcode
                                    FROM mesDistance WHERE oidref BETWEEN {} AND {}'''.format(oid[idx], oid[idx + 1]))

            diameter = tap_query('''SELECT oidref, diameter, error, diameter_prec, unit, bibcode FROM mesDiameter
                                    WHERE  oidref BETWEEN {} AND {}'''.format(oid[idx], oid[idx + 1]))

            pm = tap_query('''SELECT oidref, pmde, pmde_err, pmde_prec, pmra, pmra_err, pmra_prec, bibcode FROM mesPM
                                WHERE oidref BETWEEN {} AND {}'''.format(oid[idx], oid[idx + 1]))

            spectr = tap_query('''SELECT oidref, sptype, bibcode FROM mesMk
                                WHERE oidref BETWEEN {} AND {}'''.format(oid[idx], oid[idx + 1]))

            velocity = tap_query('''SELECT oidref, velType, velValue, meanError, velValue_prec, bibcode FROM mesVelocities
                                    WHERE  oidref BETWEEN {} AND {}'''.format(oid[idx], oid[idx + 1]))

            rotation = tap_query('''SELECT oidref, vsini, vsini_err, vsini_prec, upvsini, bibcode FROM mesRot
                                    WHERE  oidref BETWEEN {} AND {}'''.format(oid[idx], oid[idx + 1]))

            parallax = tap_query('''SELECT oidref, plx, plx_err, plx_prec, bibcode FROM mesPlx
                                    WHERE  oidref BETWEEN {} AND {}'''.format(oid[idx], oid[idx + 1]))

            fe_h = tap_query('''SELECT oidref, teff, fe_h, fe_h_prec, log_g, log_g_prec, bibcode FROM mesFe_H
                                WHERE  oidref BETWEEN {} AND {}'''.format(oid[idx], oid[idx + 1]))

            flux = tap_query('''SELECT oidref, filter, flux, flux_err, flux_prec, bibcode FROM FLUX
                                WHERE oidref BETWEEN {} AND {}'''.format(oid[idx], oid[idx + 1]))

            type = tap_query('''SELECT oidref, otypes FROM ALLTYPES
                                WHERE oidref BETWEEN {} AND {}'''.format(oid[idx], oid[idx + 1]))

            ids = tap_query('''SELECT oidref, ids FROM IDS
                                WHERE oidref BETWEEN {} AND {}'''.format(oid[idx], oid[idx + 1]))

        if row[1].strip() != '':  # morph_type
            enrich_claim(get_claim(item, 'P223', row[1].strip(), None), row[2])  # morph_bibcode

        if row[3].strip() != '':  # sp_type
            sp_type = row[3].strip().strip()
            if len(sp_type) > 3 and sp_type[2] == ' ':
                sp_type = sp_type[:2] + sp_type[3:]
            enrich_claim(get_claim(item, 'P215', sp_type, None), row[4])  # sp_bibcode

        main_type = row[5].strip()  # otype_txt
        if main_type in otypes:
            if type_can_be_added(otypes[main_type], item['claims'], blacklist):
                target = get_claim(item, 'P31', {'id': otypes[main_type]}, None)
                target['rank'] = 'normal'
                enrich_claim(target)
        else:
            if 'id' not in item:
                print("Object {} unknown type {}".format(row[0], main_type))
                continue  # if new item and main_type is unknown

        try:
            amount = set_amount(row[6], row[7], 'Q28390')  # if it throws exception, we will not add any claims
            claim = get_claim(item, 'P6257', None, None)
            claim['mainsnak']['datavalue']['value'] = amount
            enrich_claim(claim, row[10])  # coo_bibcode
        except (ValueError, DecimalException) as e:
            print("Object {} line {} exception {}".format(row[0], getframeinfo(currentframe()).lineno, e))

        try:
            amount = set_amount(row[8], row[9], 'Q28390')  # if it throws exception, we will not add any claims
            claim = get_claim(item, 'P6258', None, None)  # dec
            claim['mainsnak']['datavalue']['value'] = amount
            enrich_claim(claim, row[10])  # coo_bibcode
        except (ValueError, DecimalException) as e:
            print("Object {} line {} exception {}".format(row[0], getframeinfo(currentframe()).lineno, e))

        if 'P6258' in item['claims'] and 'references' in item['claims']['P6258'][0]:
            claim = get_claim(item, 'P6259', None, None)
            claim['mainsnak']['datavalue']['value']['id'] = 'Q1264450'
            enrich_claim(claim, row[10])

        if 'P59' not in item['claims'] and 'P6257' in item['claims'] and 'P6258' in item['claims']:
            point = coord.SkyCoord(
                item['claims']['P6257'][0]['mainsnak']['datavalue']['value']['amount'],
                item['claims']['P6258'][0]['mainsnak']['datavalue']['value']['amount'],
                frame='icrs', unit='deg')
            const = constellations[point.get_constellation(short_name=True)]
            if isinstance(const, str):
                claim = get_claim(item, 'P59', {'id': const}, None)
            else:
                claim = get_claim(item, 'P59', {'id': const[0]}, None)

        try:  # 11 -> plx_value, plx_err, plx_prec
            amount = set_amount(row[11], row[13], 'Q21500224', row[12])
            enrich_claim(get_claim(item, 'P2214', amount, None), row[14])  # plx_bibcode
        except (ValueError, DecimalException) as e:
            print("Object {} line {} exception {}".format(row[0], getframeinfo(currentframe()).lineno, e))

        try:  # 15 -> pmdec, pm_err_min, pmdec_prec
            claim = get_claim(item, 'P2215', set_amount(row[15], row[17], 'Q22137107', row[16]),
                              {'id': 'P642', 'value': 'Q76287'})
            claim['qualifiers'] = {'P642': [create_claim('', 'P642', {'id': 'Q76287'})['mainsnak']]}
            enrich_claim(claim, row[21])  # pm_bibcode
        except (ValueError, DecimalException) as e:
            print("Object {} line {} exception {}".format(row[0], getframeinfo(currentframe()).lineno, e))

        try:  # 18 -> pmra, pm_err_maj, pmra_prec
            claim = get_claim(item, 'P2215', set_amount(row[18], row[20], 'Q22137107', row[19]),
                              {'id': 'P642', 'value': 'Q13442'})
            claim['qualifiers'] = {'P642': [create_claim('', 'P642', {'id': 'Q13442'})['mainsnak']]}
            enrich_claim(claim, row[21])  # pm_bibcode
        except (ValueError, DecimalException) as e:
            print("Object {} line {} exception {}".format(row[0], getframeinfo(currentframe()).lineno, e))

        try:  # 22 -> rvz_radvel, rvz_err, rvz_radvel_prec
            amount = set_amount(row[22], row[24], 'Q3674704', row[23])
            enrich_claim(get_claim(item, 'P2216', amount, None), row[25])  # rvz_bibcode
        except (ValueError, DecimalException) as e:
            print("Object {} line {} exception {}".format(row[0], getframeinfo(currentframe()).lineno, e))

        if id in parent:
            for sim_parent in parent[id]:
                if sim_parent[0] in sim_object:
                    parent_prop = 'P361'
                    for claim in item['claims']['P31']:
                        if claim['mainsnak']['datavalue']['value']['id'] == 'Q44559':
                            parent_prop = 'P397'  # expoplanet
                            break
                    claim = get_claim(item, parent_prop, {'id': sim_object[sim_parent[0]]}, None)
                    enrich_claim(claim)

        if id in variability:
            for var in variability[id]:
                if var[0].upper() in var_types:
                    enrich_claim(get_claim(item, 'P881', {'id': var_types[var[0].upper()]}, None), var[1])

        units = {'pc': 'Q12129', 'kpc': 'Q11929860', 'Mpc': 'Q3773454'}
        if id in distance:
            for measurement in distance[id]:
                if measurement[3] in units:
                    try:
                        amount = set_amount(measurement[0], measurement[1], units[measurement[3]], measurement[2])
                        claim = get_claim(item, 'P2583', amount, None)
                        enrich_claim(claim, measurement[4])
                    except (ValueError, DecimalException) as e:
                        print("Object {} line {} exception {}".format(row[0], getframeinfo(currentframe()).lineno, e))

        if id in diameter:
            for measurement in diameter[id]:
                try:
                    if measurement[3] == 'km':
                        amount = set_amount('{0:.0f}'.format(Decimal(measurement[0])), measurement[2], 'Q828224',
                                            '{0:.0f}'.format(Decimal(measurement[1])))
                        enrich_claim(get_claim(item, 'P2386', amount, None), measurement[4])
                    else:
                        if measurement[3] == 'mas':
                            amount = set_amount(measurement[0], measurement[2], 'Q21500224', measurement[1])
                            enrich_claim(get_claim(item, 'P5348', amount, None), measurement[4])
                except (ValueError, DecimalException) as e:
                    print("Object {} line {} exception {}".format(row[0], getframeinfo(currentframe()).lineno, e))

        if id in pm:
            for measurement in pm[id]:
                try:
                    claim = get_claim(item, 'P2215',
                                      set_amount(measurement[0], measurement[2], 'Q22137107', measurement[1]),
                                      {'id': 'P642', 'value': 'Q76287'})
                    claim['qualifiers'] = {'P642': [create_claim('', 'P642', {'id': 'Q76287'})['mainsnak']]}
                    enrich_claim(claim, measurement[6])
                except (ValueError, DecimalException) as e:
                    print("Object {} line {} exception {}".format(row[0], getframeinfo(currentframe()).lineno, e))

                try:
                    claim = get_claim(item, 'P2215',
                                      set_amount(measurement[3], measurement[5], 'Q22137107', measurement[4]),
                                      {'id': 'P642', 'value': 'Q13442'})
                    claim['qualifiers'] = {'P642': [create_claim('', 'P642', {'id': 'Q13442'})['mainsnak']]}
                    enrich_claim(claim, measurement[6])
                except (ValueError, DecimalException) as e:
                    print("Object {} line {} exception {}".format(row[0], getframeinfo(currentframe()).lineno, e))

        if id in spectr:
            for measurement in spectr[id]:
                sp_type = measurement[0].strip()
                if len(sp_type) > 3 and sp_type[2] == ' ':
                    sp_type = sp_type[:2] + sp_type[3:]
                enrich_claim(get_claim(item, 'P215', sp_type, None), measurement[1])

        if id in velocity:
            for measurement in velocity[id]:
                try:
                    amount = set_amount(measurement[1], measurement[3], 'Q3674704', measurement[2])
                    if measurement[0].lower() == 'v':
                        enrich_claim(get_claim(item, 'P2216', amount, None), measurement[4])
                    else:
                        if measurement[0].lower() == 'z':
                            amount['unit'] = '1'
                            enrich_claim(get_claim(item, 'P1090', amount, None), measurement[4])
                except (ValueError, DecimalException) as e:
                    print("Object {} line {} exception {}".format(row[0], getframeinfo(currentframe()).lineno, e))

        if id in rotation:
            for measurement in rotation[id]:
                try:
                    amount = set_amount(measurement[0], measurement[2], 'Q3674704', measurement[1])
                    claim = get_claim(item, 'P4296', amount, None)
                    if measurement[3].strip() == '<':
                        claim['qualifiers'] = {'P1480': [create_claim('', 'P1480', {'id': 'Q52834024'})['mainsnak']]}
                    enrich_claim(claim, measurement[4])
                except (ValueError, DecimalException) as e:
                    print("Object {} line {} exception {}".format(row[0], getframeinfo(currentframe()).lineno, e))

        if id in parallax:
            for measurement in parallax[id]:
                try:
                    amount = set_amount(measurement[0], measurement[2], 'Q21500224', measurement[1])
                    enrich_claim(get_claim(item, 'P2214', amount, None), measurement[3])
                except (ValueError, DecimalException) as e:
                    print("Object {} line {} exception {}".format(row[0], getframeinfo(currentframe()).lineno, e))

        if id in fe_h:
            for measurement in fe_h[id]:
                try:
                    enrich_claim(get_claim(item, 'P6879', set_amount(measurement[0], '0', 'Q11579'), None),
                                 measurement[5])
                except (ValueError, DecimalException) as e:
                    print("Object {} line {} exception {}".format(row[0], getframeinfo(currentframe()).lineno, e))

                try:
                    enrich_claim(get_claim(item, 'P2227', set_amount(measurement[1], measurement[2], '1'), None),
                                 measurement[5])
                except (ValueError, DecimalException) as e:
                    print("Object {} line {} exception {}".format(row[0], getframeinfo(currentframe()).lineno, e))

                try:
                    accuracy = int(measurement[4]) if int(measurement[4]) > -round(float(measurement[3])) else -round(
                        float(measurement[3]))
                    gravity = ''
                    while True:
                        candidate = round(math.pow(10, float(measurement[3])), accuracy)
                        if round(math.log10(candidate), int(measurement[4])) != float(measurement[3]):
                            break
                        gravity = candidate
                        accuracy = accuracy - 1
                    enrich_claim(get_claim(item, 'P7015', set_amount(gravity, '0', 'Q39699418'), None), measurement[5])
                except (ValueError, DecimalException, OverflowError) as e:
                    print("Object {} line {} exception {}".format(row[0], getframeinfo(currentframe()).lineno, e))

        band = {'K': 'Q2520419', 'V': 'Q4892529', 'B': 'Q6746395', 'R': 'Q15977411', 'U': 'Q15977921',
                'I': 'Q15987557', 'J': 'Q15991308', 'H': 'Q16556693', 'G': 'Q66659648',
                'u': 'Q72675509', 'g': 'Q72675633', 'r': 'Q72675737', 'i': 'Q72675868', 'z': 'Q72675951'}
        if id in flux:
            for measurement in flux[id]:
                if measurement[0] not in band:
                    continue
                claim = get_claim(item, 'P1215', None, {'id': 'P1227', 'value': band[measurement[0]]})
                claim['qualifiers'] = {'P1227': [create_claim('', 'P1227', {'id': band[measurement[0]]})['mainsnak']]}
                claim['mainsnak']['datavalue']['value'] = set_amount(measurement[1], measurement[3], '1',
                                                                     measurement[2])
                if measurement[0] == 'V':
                    claim['rank'] = 'preferred'
                enrich_claim(claim, measurement[4])

        if id in type:
            for measurement in type[id][0][0].split('|'):
                if measurement in otypes and type_can_be_added(otypes[measurement], item['claims'], blacklist):
                    target = get_claim(item, 'P31', {'id': otypes[measurement]}, None)
                    if main_type != measurement and 'rank' not in target:
                        target['rank'] = 'deprecated'
                        main_type = ''
                    enrich_claim(target, 'Q654724')

        # region catalogue
        catalog = {'[A64]': 'Q68418195', '[A86]': 'Q68451923', '[A94]': 'Q68927929', '[ABB2003]': 'Q58909751',
                   '[ABB2014]': 'Q68420067', '[ABG2007]': 'Q66710401', '[ABH87]': 'Q68149801', '[ACS2016]': 'Q59731317',
                   '[AD95]': 'Q68162030', '[ADK2000]': 'Q68430110', '[ADM2008]': 'Q59246388', '[ADP2010]': 'Q56553142',
                   '[ADP79]': 'Q68481303', '[ADW79]': 'Q68207653', '[AGO2007]': 'Q68764868', '[AHP2016]': 'Q66649492',
                   '[ALF2001]': 'Q58913860', '[AM2002]': 'Q68509829', '[AMB2011]': 'Q69002547',
                   '[AMT2006]': 'Q68462181', '[AN2004]': 'Q68799802', '[ASC2008]': 'Q59246798',
                   '[ATL2005]': 'Q68963298', '[ATZ98]': 'Q66587020', '[AWA2011]': 'Q59787796', '[B55b]': 'Q68144921',
                   '[B61]': 'Q68199191', '[B77]': 'Q68155997', '[B79]': 'Q66649047', '[B90]': 'Q68396727',
                   '[B92b]': 'Q68112138', '[BAH2001]': 'Q58909788', '[BBE90]': 'Q68732767', '[BC2010]': 'Q68991739',
                   '[BC94]': 'Q68765456', '[BCA2007]': 'Q58908921', '[BCD2014]': 'Q60504118', '[BCK2003]': 'Q68884810',
                   '[BCM89]': 'Q68615611', '[BCS2002]': 'Q56557747', '[BD2000]': 'Q68922359', '[BD2004a]': 'Q68847246',
                   '[BD2004b]': 'Q68847382', '[BDF99]': 'Q60504387', '[BE83]': 'Q68461707', '[BEV98]': 'Q68767559',
                   '[BFK2008]': 'Q68920119', '[BFK2009]': 'Q68414380', '[BFS85]': 'Q68664644', '[BGK2008]': 'Q68722138',
                   '[BGM2005b]': 'Q58982634', '[BGS2006b]': 'Q68407018', '[BHC2011]': 'Q60015914',
                   '[BHG88]': 'Q68102717', '[BHR2005]': 'Q59246236', '[BII2011]': 'Q56552002', '[BJS91]': 'Q68546407',
                   '[BKG2004]': 'Q68537222', '[BKV2009]': 'Q66710450', '[BL2004b]': 'Q68404713', '[BM99]': 'Q55866034',
                   '[BMA2003]': 'Q60735004', '[BME2006]': 'Q60028398', '[BMH2014]': 'Q68799381',
                   '[BMK2001b]': 'Q61658000', '[BMM2014]': 'Q62623364', '[BMR2007]': 'Q59840488',
                   '[BNM96]': 'Q66470563', '[BRC2005]': 'Q68849271', '[BRS2016]': 'Q56942768', '[BTG98]': 'Q68428501',
                   '[BTH2006]': 'Q68594986', '[BTK2009]': 'Q59817549', '[BVH2007]': 'Q58913903', '[BW74]': 'Q68451919',
                   '[BWE97]': 'Q68198795', '[BWL2012]': 'Q69041850', '[BYF2011]': 'Q69011987', '[CAB2011]': 'Q59822817',
                   '[CBH2015]': 'Q59945067', '[CBS2005]': 'Q56556773', '[CBS97]': 'Q68770649', '[CBW2017]': 'Q68946173',
                   '[CC74]': 'Q68567267', '[CCH2004]': 'Q68924413', '[CCI2009]': 'Q57730264', '[CCI2010]': 'Q57730207',
                   '[CCR2014]': 'Q59756870', '[CFB2010]': 'Q58907867', '[CFG2001]': 'Q66649312',
                   '[CG2010]': 'Q66710465', '[CHM2009]': 'Q68926092', '[CHT80]': 'Q68586103', '[CJ2010]': 'Q68445566',
                   '[CK2002]': 'Q58489970', '[CK2006]': 'Q68714220', '[CKS2001]': 'Q68948799', '[CKS2006]': 'Q58489941',
                   '[CL2003]': 'Q60739385', '[CLW2011]': 'Q62621022', '[CLX2015]': 'Q68790946',
                   '[CMC2016]': 'Q58786025', '[CMI2010]': 'Q58035609', '[CMW2000]': 'Q68429768', '[CPE99]': 'Q60028463',
                   '[CPF88]': 'Q68404895', '[CPV2011]': 'Q68966326', '[CRC99]': 'Q68398710', '[CT96]': 'Q68818652',
                   '[CUS2014]': 'Q68946157', '[CW83]': 'Q68791399', '[D2005c]': 'Q68849819', '[D71]': 'Q68952177',
                   '[D87]': 'Q62270066', '[D97]': 'Q68662388', '[DAM86]': 'Q68529167', '[DB76]': 'Q68605675',
                   '[DBD86]': 'Q57953453', '[DBG99]': 'Q58914419', '[DBK2009]': 'Q59951959', '[DBS2015]': 'Q68861974',
                   '[DBT90]': 'Q68231986', '[DC78]': 'Q68203482', '[DFL98]': 'Q68969847', '[DFL99]': 'Q66649286',
                   '[DFM2004]': 'Q59246472', '[DFO95]': 'Q68918313', '[DG92]': 'Q68948646', '[DGH2004]': 'Q59729082',
                   '[DGW65]': 'Q68103793', '[DH2015]': 'Q66618182', '[DI91]': 'Q68791811', '[DJF2014]': 'Q60737454',
                   '[DKM2008]': 'Q68943011', '[DKP87]': 'Q68713686', '[DML87]': 'Q68788056', '[DMM94]': 'Q68226130',
                   '[DMP2012]': 'Q68733295', '[DPC2015]': 'Q58004790', '[DRL2005]': 'Q56934490',
                   '[DRS2009]': 'Q58912621', '[DSH2001]': 'Q56522533', '[DSH95]': 'Q68565361', '[DSH99]': 'Q68732300',
                   '[DSJ85]': 'Q68449383', '[DSM2002]': 'Q59764437', '[DSP99]': 'Q56558769', '[DSS2017]': 'Q56548566',
                   '[DT85]': 'Q56028552', '[DTH99]': 'Q68944872', '[DWB80]': 'Q68527863', '[DWC2011]': 'Q56700097',
                   '[DWS97]': 'Q56672097', '[DYC2005]': 'Q60739373', '[DYH2009]': 'Q58898178', '[EHV2002]': 'Q59814930',
                   '[ELS2006]': 'Q68853231', '[EM96]': 'Q66755051', '[ERG2015]': 'Q60018499', '[EVM96]': 'Q68542781',
                   '[EWM2015]': 'Q68448856', '[F81]': 'Q68555953', '[FAB2017]': 'Q59756640', '[FBR2002]': 'Q66470534',
                   '[FCC2013]': 'Q59246685', '[FDF99]': 'Q68935772', '[FEB2018]': 'Q68729491', '[FF93]': 'Q66617631',
                   '[FFF98]': 'Q68869533', '[FFL2006]': 'Q68440644', '[FFL2007]': 'Q53952902', '[FG85]': 'Q68476337',
                   '[FGM2003]': 'Q59246486', '[FHW95]': 'Q68497761', '[FJW97]': 'Q68767133', '[FKC2008]': 'Q58489848',
                   '[FKM2008]': 'Q68413213', '[FKP2017]': 'Q58337393', '[FKT92]': 'Q68917853', '[FLR2004]': 'Q59764174',
                   '[FPK2009]': 'Q68727857', '[FRS2006]': 'Q68851399', '[FSH2012]': 'Q58983623', '[FST98]': 'Q68570764',
                   '[FTC2015]': 'Q58909440', '[G82c]': 'Q68218711', '[GBH2005]': 'Q68437978', '[GC94]': 'Q68374186',
                   '[GC94b]': 'Q68806889', '[GCP2006]': 'Q66710387', '[GDN2015]': 'Q61821840',
                   '[GFG2013b]': 'Q58908489', '[GFG97]': 'Q68448161', '[GGA99]': 'Q68088566', '[GGF2013]': 'Q58908519',
                   '[GGH2002]': 'Q63411551', '[GGM2006]': 'Q68169983', '[GH2015]': 'Q68448789', '[GH93]': 'Q66617645',
                   '[GHD2014]': 'Q69070281', '[GIB2004]': 'Q68707311', '[GK2001]': 'Q59757124',
                   '[GKK2003]': 'Q59871453', '[GKL99]': 'Q56580629', '[GKM2012b]': 'Q69025420', '[GL65]': 'Q66709943',
                   '[GLT2013]': 'Q59943890', '[GMG83]': 'Q68266705', '[GMM2009]': 'Q68810378', '[GMS93]': 'Q68358097',
                   '[GPD88]': 'Q68952135', '[GPS2008]': 'Q58909976', '[GRA2000]': 'Q68506083', '[GS2007]': 'Q69003286',
                   '[GSI2003]': 'Q56557428', '[GSL2010]': 'Q68984264', '[GV72]': 'Q68459236', '[GY84]': 'Q68440178',
                   '[H2013]': 'Q68861703', '[H74b]': 'Q68669827', '[H78b]': 'Q68137008', '[H80b]': 'Q66371838',
                   '[H86]': 'Q68108756', '[HB91]': 'Q66617595', '[HBS2006]': 'Q68714570', '[HCP2006]': 'Q60028407',
                   '[HCS79]': 'Q68628004', '[HDL96]': 'Q68818539', '[HFC93]': 'Q68918025', '[HFH2015]': 'Q68181963',
                   '[HFP2000]': 'Q63643981', '[HG83b]': 'Q68212883', '[HHD2008]': 'Q59770723', '[HHJ2008]': 'Q60028367',
                   '[HHM2010]': 'Q68416438', '[HJN2005]': 'Q68439829', '[HJN2007]': 'Q68442415',
                   '[HKB2009]': 'Q59110932', '[HKB2010]': 'Q54258051', '[HKS2013]': 'Q56550512',
                   '[HL2008]': 'Q68443587', '[HL95]': 'Q68721760', '[HLB98]': 'Q68788423', '[HLL97]': 'Q68374622',
                   '[HMH2007]': 'Q60028378', '[HRB2007]': 'Q60028379', '[HRS2015]': 'Q59959442',
                   '[HSB2012c]': 'Q59809158', '[HSN2016]': 'Q59767513', '[HSP2011]': 'Q69011846',
                   '[HSS84]': 'Q59964397', '[HTU2012]': 'Q69046782', '[HU2001]': 'Q68400328', '[HVH2010]': 'Q57651741',
                   '[HW88b]': 'Q68226450', '[HZ2006b]': 'Q68440978', '[IBC2002]': 'Q59781565', '[IBP2002]': 'Q68849772',
                   '[IHS2009]': 'Q58492543', '[IJC2004]': 'Q68437602', '[ISK2007]': 'Q68161249', '[JBD79]': 'Q68642225',
                   '[JDW2009]': 'Q68727971', '[JE82]': 'Q66682258', '[JFA2008]': 'Q58909638', '[JFB2009]': 'Q59709184',
                   '[JHE2006]': 'Q60028402', '[JMO94]': 'Q68661096', '[JPB2009]': 'Q57768257', '[JPB2015]': 'Q57767988',
                   '[JPO2012]': 'Q66710519', '[JSD2012]': 'Q57524293', '[JSD2015]': 'Q57524271', '[JSF82]': 'Q68642374',
                   '[JSH2011]': 'Q68997552', '[K55]': 'Q68127216', '[KCF2005]': 'Q56040553', '[KCP2016]': 'Q59246679',
                   '[KCS2016]': 'Q68962112', '[KED2011]': 'Q60028309', '[KFM2008]': 'Q56700304',
                   '[KGS2010]': 'Q60021342', '[KI2008]': 'Q68443819', '[KID97]': 'Q68841072', '[KKN86]': 'Q68658618',
                   '[KKR99]': 'Q68769857', '[KLG2007]': 'Q68798928', '[KLI2009]': 'Q58035671', '[KLI2012]': 'Q58035511',
                   '[KLK2001]': 'Q68978977', '[KLK2016]': 'Q68955376', '[KMA2007]': 'Q57746274',
                   '[KMK2013]': 'Q28315857', '[KMO2004]': 'Q68537218', '[KMW82]': 'Q68479899', '[KOS87]': 'Q56002061',
                   '[KOY98]': 'Q68918778', '[KP83]': 'Q68952453', '[KPF99]': 'Q63643984', '[KPS2012]': 'Q68684117',
                   '[KSF2015]': 'Q68971515', '[KSW2012]': 'Q68946664', '[KT2007]': 'Q68854322', '[KT79]': 'Q68481139',
                   '[KVB99]': 'Q68768488', '[KWD2009]': 'Q60028329', '[KWJ66]': 'Q56688346', '[KWM2013]': 'Q59938712',
                   '[L68]': 'Q66710003', '[L89b]': 'Q66469926', '[LAC2009]': 'Q56555962', '[LBB2008]': 'Q56556106',
                   '[LBC2011]': 'Q69011849', '[LBP2018]': 'Q68754816', '[LBT89]': 'Q68917469', '[LBT95]': 'Q68950576',
                   '[LBW2010]': 'Q58919302', '[LBX2017]': 'Q58908095', '[LCC2009]': 'Q57730247',
                   '[LCK2006]': 'Q68908120', '[LDL2006]': 'Q60028399', '[LFO93]': 'Q66361536', '[LFS2013]': 'Q69059369',
                   '[LH2011]': 'Q57651718', '[LKB2014]': 'Q66362534', '[LKI2009]': 'Q68531787', '[LL2014]': 'Q69072379',
                   '[LLC99]': 'Q68664975', '[LLS2010b]': 'Q60028325', '[LM2005]': 'Q68974029', '[LO95]': 'Q68954988',
                   '[LRT2005]': 'Q68709893', '[LTS2012]': 'Q69024702', '[LW75]': 'Q68567265', '[LYC2008]': 'Q68920084',
                   '[M2003]': 'Q66381051', '[M2005c]': 'Q67054941', '[M55]': 'Q68201407', '[M75]': 'Q68527445',
                   '[M95]': 'Q68155439', '[MA93]': 'Q66470435', '[MAC2016]': 'Q58981906', '[MAW2001]': 'Q68841517',
                   '[MB2000]': 'Q62519853', '[MBB2008]': 'Q68958035', '[MBF2013]': 'Q58923568',
                   '[MBS2017]': 'Q56002069', '[MC83b]': 'Q56881604', '[MDI2012]': 'Q56550824', '[MET2005]': 'Q56556848',
                   '[MET2007]': 'Q56032072', '[MFA2007]': 'Q59768111', '[MFK2008]': 'Q58923594',
                   '[MFP2015]': 'Q58489667', '[MGC2004]': 'Q68970936', '[MGL2009]': 'Q56608752',
                   '[MGM2012]': 'Q55968817', '[MGN2003]': 'Q68702824', '[MHP2012]': 'Q69049257',
                   '[MHS2002]': 'Q68512127', '[MIC2007]': 'Q57730343', '[MII2009]': 'Q52595531',
                   '[MJR2015]': 'Q68866473', '[MKN2009]': 'Q68728015', '[MLH2008]': 'Q68721841',
                   '[MMD2006]': 'Q29041270', '[MMD97]': 'Q68918667', '[MMQ2003]': 'Q58913921', '[MMS2013]': 'Q66649462',
                   '[MMU2009]': 'Q66618057', '[MNA2010]': 'Q68445190', '[MPC98]': 'Q29391811', '[MPH2006]': 'Q63643936',
                   '[MRH2014]': 'Q58916267', '[MSD87]': 'Q68454215', '[MSM95]': 'Q68801328', '[MSS2015]': 'Q56550121',
                   '[MT82]': 'Q68474834', '[MTU2008]': 'Q68957021', '[MUE2010]': 'Q68197666', '[MVD2012]': 'Q57902176',
                   '[MWB97]': 'Q68146603', '[MWC2015]': 'Q66755262', '[MWI88]': 'Q68292216', '[MYM2001]': 'Q68648302',
                   '[N75]': 'Q68441108', '[NCK2013]': 'Q62519729', '[NCM2005]': 'Q68891478', '[NFA2013]': 'Q68126728',
                   '[NKB95]': 'Q68451138', '[NLC2006]': 'Q56949466', '[NMO98]': 'Q68920980', '[NOB2003]': 'Q68845676',
                   '[NRB2011]': 'Q68496861', '[NS84]': 'Q68555570', '[NSW2012]': 'Q69028658', '[NTI2009]': 'Q68858772',
                   '[NTO2000]': 'Q68921979', '[NVK2008]': 'Q68412932', '[NW2007]': 'Q68961165',
                   '[NWA2009]': 'Q60023103', '[OF84]': 'Q56813588', '[OFK2014]': 'Q68128419', '[OH83]': 'Q68161810',
                   '[OHS98]': 'Q68398601', '[OI2012]': 'Q69047878', '[OKL2005]': 'Q68437874', '[OMK98]': 'Q68235420',
                   '[OMT96]': 'Q68931389', '[OMW2007]': 'Q68925761', '[OWB2005]': 'Q58922481', '[OWB92]': 'Q68089340',
                   '[OWG93]': 'Q68748978', '[OWH82]': 'Q68119655', '[P85]': 'Q68952400', '[P88]': 'Q68953056',
                   '[P98]': 'Q68592421', '[PBD2003]': 'Q58917437', '[PBR2005]': 'Q68543273', '[PCA2003]': 'Q68924306',
                   '[PCB2011]': 'Q60028302', '[PCE2006]': 'Q57586359', '[PCG2016]': 'Q59868969',
                   '[PFC2012]': 'Q58909819', '[PFH2005]': 'Q63643943', '[PGF2001]': 'Q68841311',
                   '[PGS2005]': 'Q68440007', '[PLO2002]': 'Q58330773', '[PLW2012]': 'Q61832955',
                   '[PM2002]': 'Q68401759', '[PMC2000]': 'Q68695516', '[PMC2001]': 'Q68697061',
                   '[PMD2006]': 'Q59246442', '[PMH2004]': 'Q63643947', '[PMP2006]': 'Q58302763', '[PPR85]': 'Q60028175',
                   '[PRK2011]': 'Q68998636', '[PRS2007]': 'Q68854567', '[PRS2008]': 'Q60028372', '[PS79]': 'Q68188565',
                   '[PSC95]': 'Q68791913', '[PSW80]': 'Q68556609', '[PV96]': 'Q68766238', '[PWC2011]': 'Q66710498',
                   '[QTC2005]': 'Q68810319', '[QZP2008]': 'Q57457926', '[R63]': 'Q68429704', '[R76]': 'Q68481209',
                   '[R77]': 'Q69016192', '[R89]': 'Q68461151', '[R89b]': 'Q68243674', '[RAA2005]': 'Q68563570',
                   '[RAB2011b]': 'Q58916412', '[RBM2010]': 'Q68981890', '[RFR2001]': 'Q68432445',
                   '[RGK2002]': 'Q59757082', '[RGM91]': 'Q68839424', '[RGS2010]': 'Q68123435', '[RHI84]': 'Q66649095',
                   '[RKS2002]': 'Q59707991', '[RKV2003]': 'Q68953746', '[RLD2010]': 'Q68995969',
                   '[RLF2004]': 'Q59764178', '[RM2001b]': 'Q68430885', '[RMN2001]': 'Q68429751',
                   '[RMS2003]': 'Q68846072', '[RMS2015]': 'Q68750595', '[RPL2009]': 'Q58489824',
                   '[RRB2014]': 'Q57746726', '[RRK2001]': 'Q68400352', '[RSD2012]': 'Q58912588',
                   '[RSE2006]': 'Q68465603', '[RSS2010]': 'Q58912616', '[RTG2015]': 'Q58908258', '[RW83]': 'Q68116367',
                   '[S77]': 'Q68947340', '[S78c]': 'Q68564524', '[S96b]': 'Q66436188', '[SBB2014]': 'Q58238843',
                   '[SBK2009]': 'Q68667656', '[SBM2001]': 'Q59939096', '[SCB2001]': 'Q66649314', '[SDC97]': 'Q56559406',
                   '[SDG98]': 'Q69069578', '[SDG99b]': 'Q68798900', '[SDM2004]': 'Q59629123', '[SDS99]': 'Q68921324',
                   '[SEC2010]': 'Q59936377', '[SFZ2012]': 'Q56170916', '[SGM2004]': 'Q59758212',
                   '[SGR2006]': 'Q68954081', '[SHK2000]': 'Q68951951', '[SHL2001]': 'Q68729522', '[SHP97]': 'Q68667946',
                   '[SHS98]': 'Q68788369', '[SHW2006]': 'Q68123379', '[SIP2006]': 'Q59768119', '[SJV2010]': 'Q59769349',
                   '[SK98]': 'Q68102683', '[SKE2001]': 'Q56558098', '[SKH2002]': 'Q68956254', '[SKM2002]': 'Q68975801',
                   '[SKM2012]': 'Q66618120', '[SKN2015]': 'Q69101214', '[SKS2005]': 'Q56556759', '[SKS95]': 'Q59757208',
                   '[SKV94]': 'Q68187630', '[SL63]': 'Q68604125', '[SLB2015]': 'Q58909395', '[SLK2004]': 'Q66470044',
                   '[SLN74]': 'Q68688450', '[SLO58]': 'Q68970346', '[SM2007]': 'Q68442977', '[SMC2009]': 'Q68859037',
                   '[SMD2013]': 'Q58220743', '[SMF2000]': 'Q68506106', '[SMG2009]': 'Q56170918',
                   '[SMM2006b]': 'Q68974245', '[SMO84]': 'Q68769541', '[SMR2006]': 'Q68943277',
                   '[SMT2002]': 'Q59758369', '[SP2002]': 'Q68955222', '[SPB96]': 'Q68147574', '[SPD2011]': 'Q60001907',
                   '[SPE2008b]': 'Q58333125', '[SPG2012]': 'Q59705939', '[SPP2015]': 'Q56550152',
                   '[SRA96]': 'Q66649250', '[SRB80]': 'Q68446666', '[SRH2011]': 'Q68997434', '[SRM2005]': 'Q66755131',
                   '[SSE2005]': 'Q68950794', '[SSH97b]': 'Q66755059', '[SSK2002]': 'Q56557724',
                   '[SSK2002b]': 'Q56557701', '[SSO2006]': 'Q68464360', '[SSP2004]': 'Q68372754',
                   '[SUF99]': 'Q59758819', '[SVG96]': 'Q68438801', '[SW2005]': 'Q68889564', '[SWH2011]': 'Q68495708',
                   '[SWM2014]': 'Q59723515', '[SYS98]': 'Q68428418', '[SYW2010]': 'Q59787836', '[T2015]': 'Q66618180',
                   '[T66b]': 'Q68118069', '[T73]': 'Q66648942', '[T81]': 'Q68685835', '[TBP2010]': 'Q60028313',
                   '[TBS2007]': 'Q59817570', '[TCH91]': 'Q68943031', '[TDL2015]': 'Q59797815', '[TEE2006]': 'Q68180933',
                   '[TFP2015]': 'Q69108046', '[TFV2009]': 'Q58246927', '[TGC2005]': 'Q68406352', '[THL93]': 'Q68524974',
                   '[THW2006]': 'Q68593570', '[TKB2012]': 'Q62623224', '[TKO2016]': 'Q68971127',
                   '[TOS2004]': 'Q68911678', '[TPB95]': 'Q66310642', '[TPS2005]': 'Q68710500', '[TSA98]': 'Q68944879',
                   '[TT97]': 'Q68662225', '[TTG2014]': 'Q69065402', '[TTK2018]': 'Q59797759', '[TTL2012]': 'Q69025156',
                   '[TUW2004]': 'Q68952113', '[TVH89]': 'Q68569229', '[TVS94]': 'Q68791469', '[TW2004b]': 'Q68542572',
                   '[UHK2018]': 'Q59787532', '[UHS2018]': 'Q68969191', '[UT87]': 'Q68918550', '[UTK2014]': 'Q59787708',
                   '[UTK2017]': 'Q56548748', '[V92]': 'Q68424965', '[VAE96]': 'Q59269409', '[VBH2010]': 'Q58913898',
                   '[VBW78]': 'Q68446421', '[VC94]': 'Q68425597', '[VCM2003]': 'Q68448802', '[VDD93]': 'Q68451161',
                   '[VDS2009b]': 'Q68696083', '[VFK98]': 'Q57768407', '[VGK85]': 'Q66682271', '[VKN2011]': 'Q69015629',
                   '[VKP2006]': 'Q68463284', '[VMF94]': 'Q66361962', '[VRC2001]': 'Q68696354', '[VV96]': 'Q66649251',
                   '[VZA2004]': 'Q59984441', '[W60]': 'Q68483096', '[W61c]': 'Q68791406', '[W65]': 'Q68253259',
                   '[W71b]': 'Q68483371', '[WB2008]': 'Q68443440', '[WBH2005]': 'Q68925129', '[WBH99]': 'Q68543381',
                   '[WCA2013]': 'Q57637120', '[WCH93]': 'Q68621333', '[WCO2009]': 'Q64166711', '[WEG2004]': 'Q59763739',
                   '[WGH2008]': 'Q63643889', '[WHI91]': 'Q68917512', '[WHL2012]': 'Q69041639', '[WHL95]': 'Q68623375',
                   '[WHO91]': 'Q68917516', '[WHR97]': 'Q68943088', '[WHW2001]': 'Q68697142', '[WKE96]': 'Q66649248',
                   '[WLF2008b]': 'Q69037947', '[WMG70]': 'Q68138314', '[WOW2012]': 'Q68926578',
                   '[WRW2015]': 'Q68685075', '[WWB83]': 'Q66436442', '[WZA2016]': 'Q68124827', '[XLB2011]': 'Q56551834',
                   '[YBD2009]': 'Q60028333', '[YKK2016]': 'Q59767374', '[YP2004]': 'Q68436954',
                   '[YYG2008]': 'Q68722928', '[ZBO89]': 'Q60637954', '[ZCT89]': 'Q68238193', '[ZEH2003]': 'Q68955292',
                   '[ZK2009]': 'Q68147411', '[ZKK89]': 'Q68603116', '[ZMT95]': 'Q68765848', '[ZZS93]': 'Q68798093',
                   '0ES ': 'Q68251680', '1A ': 'Q72547253', '1BMW ': 'Q60663010', '1C': 'Q3921883', '1C ': 'Q68547272',
                   '1ES': 'Q28843527', '1HERMES ': 'Q57637130', '1HWC ': 'Q72549364', '1RXS': 'Q2813479',
                   '1SWXRT': 'Q58913105', '1SXPS': 'Q58913037', '1XRS': 'Q68158759', '2A': 'Q68128185',
                   '2C': 'Q2330862', '2dFGRS': 'Q2815842', '2dFS': 'Q59957749', '2EG': 'Q68756682', '2EGS': 'Q68918568',
                   '2EUVE': 'Q28913193', '2FAV': 'Q58327507', '2FGL': 'Q56636469', '2FHL': 'Q57541394',
                   '2HWC': 'Q58321893', '2MASS': 'Q1454942', '2MASS6xp': 'Q59928829', '2MASX': 'Q27890669',
                   '2MAXI': 'Q58919472', '2MFGC': 'Q55693523', '2PIGG': 'Q58910208', '2QZ': 'Q59719359',
                   '2RXS': 'Q63643762', '2U': 'Q68731213', '2WHSP': 'Q58914004', '2XMMp': 'Q68854512',
                   '3A': 'Q55968268', '3B': 'Q59911221', '3C': 'Q598937', '3CR': 'Q68173975', '3EG': 'Q58360073',
                   '3FGL': 'Q57541422', '3FHL': 'Q58327490', '3MAXI': 'Q68806080', '3U': 'Q68604392',
                   '3XMM': 'Q58983681', '40P': 'Q68134216', '49W': 'Q68213826', '4B': 'Q68754397', '4C': 'Q3825701',
                   '4CT': 'Q68449775', '4U': 'Q55879134', '4W': 'Q68445051', '5C': 'Q5447486', '6C': 'Q7533119',
                   '6dFGS': 'Q4642551', '6W': 'Q68561375', '7C': 'Q7457693', '7W': 'Q68566038', '8C': 'Q3887269',
                   '9C': 'Q10846265', 'AAO': 'Q66617536', 'Abell': 'Q318624', 'AC': 'Q68250288', 'AC2000': 'Q68427930',
                   'ADS': 'Q937529', 'AG': 'Q68660262', 'AG82': 'Q68207650', 'AGAL': 'Q68951743', 'AGC': 'Q28913486',
                   'AJG': 'Q68756728', 'ALESS': 'Q56555921', 'ALFALFA': 'Q28056499', 'ALS': 'Q28913509',
                   'AM': 'Q28913514', 'Anon': 'Q66310021', 'APG': 'Q757251', 'APMBGC': 'Q55879535', 'ARGO': 'Q58370108',
                   'ASAS': 'Q4385811', 'ASB': 'Q68121359', 'ASCC': 'Q28913521', 'ASP03': 'Q68948752',
                   'ASTEP': 'Q66618230', 'AT': 'Q68474835', 'AzGN': 'Q57088432', 'B1': 'Q68112618', 'B2': 'Q68528453',
                   'B3': 'Q68204643', 'Barnard': 'Q3247327', 'BAT99': 'Q55893994', 'BBDS': 'Q60781650',
                   'BBW': 'Q68204118', 'BD': 'Q845735', 'BDS': 'Q68134725', 'BFS': 'Q68953891', 'BG': 'Q68113173',
                   'BGE': 'Q68144983', 'BGG': 'Q68264107', 'BGPS': 'Q4939961', 'BHR': 'Q68619199', 'BI': 'Q68176677',
                   'BLOX': 'Q68644808', 'BPM': 'Q68567405', 'BRHT': 'Q68569673', 'Bruck': 'Q66470220', 'C': 'Q857461',
                   'CAIRNS': 'Q55722031', 'CCAC': 'Q68974185', 'CCDM': 'Q2624735', 'CCPC': 'Q68956183',
                   'CD-63': 'Q392437', 'CE': 'Q64115122', 'Ced': 'Q3663263', 'CEL': 'Q28913538', 'CF': 'Q68131374',
                   'CGCS': 'Q68811373', 'CGO': 'Q68160559', 'CGPSE': 'Q68717960', 'CGRaBS': 'Q58914066',
                   'Ci': 'Q68205463', 'CJF': 'Q60028137', 'CLANS': 'Q68958574', 'CLASXS': 'Q68924821',
                   'CMC': 'Q68619143', 'CMC14': 'Q66710398', 'CNOC': 'Q68630420', 'CNOC2': 'Q58908385',
                   'Collinder': 'Q2661779', 'CORNISH': 'Q61832947', 'COSMOS2015': 'Q58908146', 'COSMOSVLA': 'Q58908940',
                   'COSMOSVLADP': 'Q58908793', 'CPC': 'Q5034995', 'CPC2': 'Q68264831', 'CPD-63': 'Q2937249',
                   'CPOC': 'Q57757416', 'CRATES': 'Q59878681', 'CRTS': 'Q16918903', 'CSI': 'Q68223839',
                   'CSV': 'Q28914856', 'CTB': 'Q68452345', 'CTS': 'Q5024784', 'CVSO': 'Q58262762',
                   'CXOCDFS': 'Q58909113', 'CXOECDFS': 'Q58909005', 'CXOGC': 'Q58909627', 'CXOGNC': 'Q69011784',
                   'CXOGSG': 'Q69131277', 'CXOMP': 'Q59741057', 'CXOXB': 'Q59910104', 'D31': 'Q68432275',
                   'D33': 'Q68431017', 'DDO': 'Q2074058', 'DENIS': 'Q16248742', 'DIRBE': 'Q68404628',
                   'DIRECT': 'Q54258083', 'DO': 'Q55710790', 'DOBASHI': 'Q69022901', 'DRCG': 'Q66436473',
                   'DUGRS': 'Q66617884', 'DWB': 'Q68566728', 'DWH': 'Q68398589', 'EDCC': 'Q68121438',
                   'EDSGC': 'Q68529567', 'EGO': 'Q68965420', 'EGSIRAC': 'Q68920097', 'ELAISC7': 'Q56924723',
                   'ENACS': 'Q66617840', 'EO': 'Q66710288', 'EON': 'Q69072572', 'ESDO': 'Q68966255', 'ESG': 'Q68785988',
                   'ESO': 'Q5413234', 'EUVE': 'Q28914874', 'EV*': 'Q66310748', 'EVCC': 'Q28914883', 'EXMS': 'Q68159288',
                   'EXSS': 'Q59983527', 'EY': 'Q66617696', 'F3R': 'Q68616330', 'FAUST': 'Q68124363', 'FCC': 'Q29308402',
                   'FDF': 'Q68845113', 'Fermi': 'Q58360131', 'FEST': 'Q68528017', 'FIRBACK': 'Q58217458',
                   'FIRST': 'Q28084844', 'FK5': 'Q15817338', 'FKSZ': 'Q68194874', 'FLASH': 'Q59939509',
                   'FLSGMRT': 'Q56977164', 'FLSVLA': 'Q68970951', 'FSG': 'Q56881474', 'FSZ': 'Q68787735',
                   'Gaia DR1': 'Q37859523', 'Gaia DR2': 'Q51905050', 'GAMA': 'Q5518108', 'GASS': 'Q16928201',
                   'GB3': 'Q68952150', 'GB6': 'Q66381049', 'GCl': 'Q68562773', 'GCRV': 'Q1606717', 'GDDS': 'Q60523474',
                   'GF': 'Q68627069', 'GJ': 'Q1045111', 'GLEAM': 'Q57807518', 'GLG': 'Q57808341', 'Gmb': 'Q68089030',
                   'GMBCG': 'Q57746199', 'GMP': 'Q59942573', 'GMRTEN1': 'Q56977134', 'GMRTLH': 'Q56977141',
                   'GOS': 'Q66710355', 'Goy': 'Q68524446', 'GPM': 'Q66617827', 'GPM1': 'Q68767128', 'GPS': 'Q68447530',
                   'GRA': 'Q68451363', 'GRB': 'Q68587635', 'GSC': 'Q143003', 'GSC2': 'Q15817618', 'GSC2U': 'Q68945347',
                   'GSPC': 'Q66587025', 'GT': 'Q68451465', 'GUM': 'Q2721889', 'GUSBAD': 'Q68389596',
                   'GUVV': 'Q58470245', 'GW': 'Q59953060', 'Hawk': 'Q68193693', 'HBC': 'Q68219173', 'HCG': 'Q1574240',
                   'HD': 'Q111130', 'HFE': 'Q68269747', 'HGAM': 'Q68527697', 'HH': 'Q68614991', 'HIC': 'Q28914996',
                   'HIGALP': 'Q59246075', 'HIP': 'Q537199', 'HIPASS': 'Q304286', 'HMST': 'Q68480280',
                   'HMV': 'Q68522220', 'HPS': 'Q68958484', 'HR': 'Q499138', 'HRDS': 'Q68956239', 'HSNH': 'Q68448057',
                   'HVCS': 'Q57602895', 'HW': 'Q68999434', 'Hyn': 'Q68680440', 'IC': 'Q741672', 'IDS': 'Q6017813',
                   'INTREF': 'Q66362130', 'IPHAS': 'Q3146779', 'IRAS': 'Q27891161', 'IRC': 'Q7858668',
                   'IRSF': 'Q66710426', 'IRSV': 'Q68218914', 'ITG': 'Q68813507', 'IXO': 'Q68943994', 'JP11': 'Q6108661',
                   'Karmn': 'Q60305026', 'KD': 'Q68606924', 'Kes': 'Q68447559', 'KHM31': 'Q68798922', 'KIC': 'Q4042165',
                   'KKC': 'Q68602592', 'KMS': 'Q68917465', 'KNOWS': 'Q60663260', 'KONUS': 'Q68220579',
                   'KOSS': 'Q66649085', 'KPG': 'Q27891791', 'KR': 'Q68952191', 'LAL': 'Q68250249', 'LARCS': 'Q56558423',
                   'LBN': 'Q13411059', 'LCDCS': 'Q67054917', 'LCRS': 'Q6492101', 'LCS1': 'Q59878639',
                   'LDN': 'Q68581348', 'LDUW': 'Q68221516', 'LEDA': 'Q6709611', 'LEE': 'Q66616583', 'LESS': 'Q68161698',
                   'LFT': 'Q28915419', 'LGGS': 'Q54258101', 'LHS': 'Q28915499', 'LIN': 'Q68486805', 'LLNS': 'Q66710087',
                   'LMA': 'Q68237323', 'LMH': 'Q68447977', 'LPM': 'Q28915631', 'LPS': 'Q68621109', 'LQAC': 'Q69024004',
                   'LRWR': 'Q68444848', 'LSPM': 'Q28915706', 'LTT': 'Q28915747', 'LW': 'Q68246712', 'LZK': 'Q68160858',
                   'M': 'Q14530', 'M33SyS': 'Q68968220', 'MaCCo': 'Q68491939', 'MACS': 'Q68288039', 'MAXI': 'Q68452622',
                   'MC1': 'Q68116935', 'MC4': 'Q68561322', 'MCBB': 'Q68799057', 'MCG': 'Q68605063', 'MCPS': 'Q54258163',
                   'MCXC': 'Q69000630', 'MDM': 'Q68767508', 'Melotte': 'Q3663270', 'MGC': 'Q62623484',
                   'MGE': 'Q59246128', 'MGPS': 'Q59878691', 'MHO': 'Q56456966', 'MHR': 'Q68556079',
                   'MIRES': 'Q53985958', 'MLA': 'Q68263291', 'MML': 'Q68506091', 'MPWK': 'Q68451474', 'MRC': 'Q6896794',
                   'MSH': 'Q68421370', 'MSX6C': 'Q68381737', 'MSXDC': 'Q68462300', 'MUSR': 'Q68925111',
                   'MUSYC': 'Q58786711', 'MZ': 'Q68700193', 'N30': 'Q5051307', 'NB': 'Q68145299', 'NEWPS5': 'Q60868164',
                   'NG': 'Q68659054', 'NGC': 'Q14534', 'NGPFG': 'Q68274290', 'NLTT': 'Q28916763', 'NMBS': 'Q69007416',
                   'NPM1G': 'Q55723598', 'NPM2': 'Q68924819', 'NRRF': 'Q68960776', 'NSCS': 'Q68437383',
                   'NSV': 'Q3875194', 'NVSS': 'Q6955104', 'OCISM': 'Q68286413', 'OCl': 'Q9185170', 'OGLE': 'Q67054966',
                   'OHIO': 'Q68445675', 'OHSC': 'Q68423236', 'OTS': 'Q59757121', 'PA': 'Q68299099', 'PACN': 'Q68706926',
                   'PBC': 'Q58909462', 'PBCX': 'Q58909442', 'PCYC': 'Q69011812', 'PDCS': 'Q59764471',
                   'PDS': 'Q65640216', 'PGC': 'Q1479861', 'PhebusB': 'Q68196517', 'PK': 'Q5758172', 'PKS': 'Q68173060',
                   'PLX': 'Q9301600', 'PM': 'Q68969970', 'PM2000': 'Q59703331', 'PMC': 'Q28916890', 'PMMR': 'Q66649084',
                   'PMSC': 'Q29014274', 'POX': 'Q68608071', 'PPA': 'Q56070119', 'PPM': 'Q1449592', 'PPMX': 'Q60305030',
                   'PPMXL': 'Q57075039', 'PRC': 'Q56916495', 'PRH': 'Q68507297', 'PSCz': 'Q28916932',
                   'PSZ2': 'Q57633629', 'PWP': 'Q68476128', 'QORG': 'Q68647368', 'QSO': 'Q27891806', 'R15': 'Q55451155',
                   'RadSCG': 'Q68529123', 'RAFGL': 'Q7275111', 'RasTyc': 'Q68504910', 'RAVE': 'Q1414921',
                   'RAW': 'Q68120996', 'RBPL': 'Q59974058', 'RBS': 'Q28855006', 'RCS': 'Q68405690', 'RCS2': 'Q69001922',
                   'RCW': 'Q2032780', 'REFL': 'Q66424424', 'Reiz': 'Q68515234', 'RF': 'Q68148274', 'RFGC': 'Q68288588',
                   'RFS': 'Q68458229', 'RGO': 'Q66587034', 'RIXOS': 'Q56924812', 'RN': 'Q68561295', 'RNO': 'Q68693831',
                   'ROT': 'Q5051328', 'ROXs': 'Q66710197', 'RRF': 'Q68448065', 'RX': 'Q57568973', 'Ryu': 'Q68197348',
                   'S4': 'Q68204655', 'S5': 'Q68522971', 'SACS': 'Q28917011', 'Sand': 'Q66710088', 'SAO': 'Q984158',
                   'SAXWFC': 'Q58914091', 'SBC7': 'Q66649045', 'SBC9': 'Q68613584', 'SBHW': 'Q68403165',
                   'SCHG': 'Q66755010', 'SCM': 'Q68124668', 'SCMS': 'Q68374445', 'SDC': 'Q68686404', 'SDSS': 'Q840332',
                   'SDSSCG': 'Q58489552', 'SGC': 'Q28917035', 'SGPA': 'Q68917309', 'SH': 'Q2565783',
                   'SHARDS': 'Q58909806', 'ShaSS': 'Q60504101', 'SHBL': 'Q58913846', 'SHOC': 'Q58489541',
                   'SIMPLE': 'Q69003328', 'Slee': 'Q68447068', 'SLW': 'Q68994495', 'SN': 'Q55712879',
                   'SNLS': 'Q3459827', 'SOGRAS': 'Q60021826', 'SOI': 'Q66649003', 'SOXS': 'Q59246328',
                   'SPB': 'Q68450633', 'SPM2.0': 'Q68428635', 'SPM3.2': 'Q54165717', 'SPM4.0': 'Q69002271',
                   'SPOCS': 'Q28920107', 'SPRC': 'Q68730605', 'SRS': 'Q28920115', 'SSCC': 'Q68745945',
                   'SSTGC': 'Q59950558', 'Str': 'Q68135964', 'SUMSS': 'Q59878706', 'SWEEPS': 'Q2665313',
                   'SWXCS': 'Q58913151', 'SZ': 'Q66649039', 'TASS': 'Q66710325', 'TASS4': 'Q66710395',
                   'TD1': 'Q7669825', 'TG': 'Q68350099', 'TGU': 'Q66470002', 'TIC': 'Q58256672', 'TKRS': 'Q68088335',
                   'TSVSC1': 'Q59911169', 'TYC': 'Q2725928', 'UBV': 'Q7863886', 'UBV M': 'Q68201627',
                   'UCAC1': 'Q68430582', 'UCAC2': 'Q66048930', 'UCAC3': 'Q28920122', 'UCAC4': 'Q28920124',
                   'UGC': 'Q615925', 'UGCA': 'Q28920129', 'USGC': 'Q66755104', 'uvby98': 'Q28920135',
                   'UVEX': 'Q59399841', 'UZC': 'Q28920139', 'VCC': 'Q25499075', 'VDB': 'Q2941646', 'VdBH': 'Q66436192',
                   'VIRGOHI': 'Q61657997', 'VRO': 'Q68454236', 'WATCH': 'Q68978994', 'WB': 'Q66424509',
                   'WBL': 'Q61160601', 'WDS': 'Q932275', 'WEB': 'Q66617723', 'WG': 'Q66648959', 'Winnecke': 'Q8025565',
                   'WIRED': 'Q66470540', 'WKB': 'Q68145213', 'WKK98': 'Q59784114', 'WORC': 'Q68120107',
                   'WVSC': 'Q66710563', 'XDEEP2': 'Q68961139', 'XMMOM': 'Q63411517', 'XMMXCS': 'Q59450772',
                   'XSS': 'Q68542276', 'YPAC': 'Q55714048', 'YZ': 'Q8047406', 'YZC': 'Q68205000', 'Z': 'Q68204948',
                   'ZFIRE': 'Q59268319', 'ZFOURGE': 'Q59268314', 'Zkh': 'Q8342933', 'ZYCJ': 'Q68468590'}
        # endregion

        if id in ids:
            for measurement in ids[id][0][0].split('|'):
                code = ' '.join(measurement.split())
                regex = re.search('^([\[\]A-Za-z0-9#]+(?:\s[DM][^\s]*)?)', code)
                if not regex:
                    continue

                if regex.group(1) == '*' or regex.group(1) == 'NAME' or regex.group(1) == 'V*':
                    code = code.replace(regex.group(1), '').strip()

                claim = get_claim(item, 'P528', code, None)
                claim['mainsnak']['datavalue']['value'] = code
                if 'qualifiers' not in claim and regex.group(1) in catalog:
                    claim['qualifiers'] = {
                        'P972': [create_claim('', 'P972', {'id': catalog[regex.group(1)]})['mainsnak']]}
                enrich_claim(claim, 'Q654724')

        new_id = wd_save(wdapi, item)
        if new_id is not None:
            sim_object[row[0]] = new_id
        else:
           wdapi = mw_logon(sys.argv[1], sys.argv[2])
