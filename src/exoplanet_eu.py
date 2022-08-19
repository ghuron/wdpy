#!/usr/bin/python3
import csv
import json
import re
import sys
import time
from collections import OrderedDict
from contextlib import closing
from inspect import getframeinfo, currentframe

import requests
from dateutil import parser
from bs4 import BeautifulSoup, Tag
from astropy import coordinates as coord

from wikidata import WikiData


class SyncBot(WikiData):
    def get_summary(self, entity):
        return 'batch import from [[Q1385430|exoplanet.eu]] for object ' + \
               entity['claims']['P5653'][0]['mainsnak']['datavalue']['value']

    @staticmethod
    def tap_query(url, sql, result=None):
        if result is None:
            result = OrderedDict()
        with closing(requests.post(url + '/sync',
                                   params={'request': 'doQuery', 'lang': 'ADQL', 'format': 'csv', 'maxrec': 20000,
                                           'query': sql}, stream=True)) as r:
            reader = csv.reader(r.iter_lines(decode_unicode='utf-8'), delimiter=',', quotechar='"')
            header = next(reader)
            for line in reader:
                if len(line) > 0:
                    if line[0] not in result:  # first column is always id
                        result[line[0]] = {}  # create empty if not exist
                    for i in range(1, len(line)):
                        if isinstance(line[i], str):
                            line[i] = ' '.join(line[i].split())  # trim multiple spaces to one (simbad)
                        result[line[0]][header[i]] = line[i]

                    for prop in header[1:]:
                        if result[line[0]][prop] != '':
                            if prop + '_min' in header:  # and result[line[0]][prop + '_min'] != '':
                                if prop + '_max' in header:  # and result[line[0]][prop + '_max'] != '':
                                    result[line[0]][prop] = str(result[line[0]][prop]) + '[' + str(
                                        result[line[0]][prop + '_min']) + ',' + str(
                                        result[line[0]][prop + '_max']) + ']'
                            if prop + '_unit' in header:  # and result[line[0]][prop + '_unit'] != '':
                                result[line[0]][prop] = str(result[line[0]][prop]) + str(
                                    result[line[0]][prop + '_unit']).replace('Q', 'U')

                    translation = {'primary transit': 'Q2069919', 'imaging': 'Q15279026',
                                   'microlensing': 'Q1028022', 'ttv': 'Q2945337',
                                   'radial velocity': 'Q2273386', 'astrometry': 'Q181505',
                                   'primary transit#ttv': 'Q2945337'
                                   }
                    if result[line[0]]['p1046'].lower() in translation:
                        result[line[0]]['p1046'] = translation[result[line[0]]['p1046'].lower()]

        return result

    def get_latest_publication_date(self, claim):
        if self.pubs is None:
            self.pubs = self.query('select ?item ?date { ?item wdt:P819 []; OPTIONAL { ?item wdt:P577 ?date }}')
            self.pubs['Q66617668'] = '1924-01-01T00:00:00Z'

        latest = None  # parser.parse('1800-01-01T00:00:00Z')
        if 'references' in claim:
            for ref in claim['references']:
                if 'P248' not in ref['snaks']: continue
                if ref['snaks']['P248'][0]['datavalue']['value']['id'] in self.pubs:
                    text = self.pubs[ref['snaks']['P248'][0]['datavalue']['value']['id']]
                    try:
                        if latest is None or parser.parse(text) > parser.parse(latest):
                            latest = text
                    except ValueError as e:
                        print('Text "{}" line {} exception {}'.format(text, getframeinfo(currentframe()).lineno, e))

                # else:
                #     if ref['snaks']['P248'][0]['datavalue']['value']['id'] == 'Q654724':
                #         latest = parser.parse('1800-01-02T00:00:00Z')
        return latest

    def normalize(self, claims):
        if len(claims) > 1:
            latest = None
            for statement in claims:
                published = self.get_latest_publication_date(statement)
                if latest is None or published is not None and parser.parse(published) > parser.parse(latest):
                    latest = published
            already_set_normal = False
            for statement in claims:
                published = self.get_latest_publication_date(statement)
                if 'rank' not in statement:
                    if published == latest and not already_set_normal:
                        statement['rank'] = 'normal'
                    else:
                        statement['rank'] = 'deprecated'
                else:
                    if already_set_normal:
                        statement['rank'] = 'deprecated'
                    else:
                        if latest is not None:
                            if published is None:
                                statement['rank'] = 'deprecated'
                if 'rank' not in statement or statement['rank'] == 'normal':
                    already_set_normal = True

    def set_ranks(self, entity):
        for property_id in entity['claims']:
            if len(entity['claims'][property_id]) < 2:
                continue
            new_claims = False
            latest = None
            for statement in entity['claims'][property_id]:
                if 'rank' not in statement:
                    new_claims = True
                elif statement['rank'] == 'preferred':
                    new_claims = False
                    break
                published = self.get_latest_publication_date(statement)
                if latest is None or published is not None and parser.parse(published) > parser.parse(latest):
                    latest = published
            if new_claims and latest is not None:
                for statement in entity['claims'][property_id]:
                    published = self.get_latest_publication_date(statement)
                    if published == latest:
                        statement['rank'] = 'preferred'
                        if 'qualifiers' not in statement:
                            statement['qualifiers'] = {}
                        statement['qualifiers']['P7452'] = [self.create_snak('P7452', 'Q98386534')]
                        break

    def parse_sources(self, page):
        patterns = {'(http[s]?://)?(dx\\.)?doi\\.org/': 'haswbstatement:P356=',
                    'http[s]?://(fr\\.)?arxiv\\.org/abs/': 'haswbstatement:P818=',
                    'https://doi.org/10.48550/arXiv.': 'haswbstatement:P818=',
                    'http[s]?://www\\.journals\\.uchicago\\.edu/doi/abs/': 'haswbstatement:P356=',
                    'http://iopscience.iop.org/0004-637X/': 'haswbstatement:P356=10.1088/0004-637X/',
                    'http[s]?://(?:ui\\.)?adsabs.harvard.edu/abs/([^/]+).*': 'haswbstatement:P819=\g<1>',
                    '.+adsabs\\.harvard\\.edu/cgi-bin/nph-bib_query\\?bibcode=([^\\&]+).*': 'haswbstatement:P819=\g<1>',
                    'http://onlinelibrary.wiley.com/doi/([^x]+x).*': 'haswbstatement:P356=\g<1>',
                    'http://online.liebertpub.com/doi/abs/([^\\?]+).*': 'haswbstatement:P356=\g<1>,',
                    '.+isbn=(\d\d\d)(\d)(\d\d\d)(\d\d\d\d\d)(\d)': 'haswbstatement:P212=\g<1>-\g<2>-\g<3>-\g<4>-\g<5>',
                    '.+jstor\\.org/stable/(info/)?': 'haswbstatement:P356='}

        publications = {}
        for p in page.find_all('p', {'class': 'publication'}):
            for a in p.contents:
                if isinstance(a, Tag) and a.get('href') is not None:
                    for search_pattern in patterns:
                        query = re.sub(search_pattern, patterns[search_pattern], a.get('href').strip())
                        if query.startswith('haswbstatement'):
                            ref_id = self.api_search(query)
                            if ref_id:
                                publications[p['id']] = ref_id
                                break
                            # else:
                            #     print(url + ' is missing')
        return publications

    def build_snak(self, row):
        digits = 3 + (len(row['value']) - row['value'].find('.') - 1 if row['value'].find('.') > 0 else 0)
        if row['property'] == 'P6257':
            ra = row['value'].split(':')
            row['unit'] = 28390
            row['value'] = self.format_float(((float(ra[2]) / 60 + float(ra[1])) / 60 + float(ra[0])) * 15, digits)
        elif row['property'] == 'P6258':
            dec = row['value'].split(':')
            row['unit'] = 28390
            if dec[0].startswith('-'):
                row['value'] = self.format_float(-((float(dec[2]) / 60 + float(dec[1])) / 60 - float(dec[0])), digits)
            else:
                row['value'] = self.format_float(((float(dec[2]) / 60 + float(dec[1])) / 60 + float(dec[0])), digits)
        return super().build_snak(row)

    def obtain_claim(self, entity, snak):
        if snak is not None and snak['property'] in ['P6257', 'P6258']:
            if snak['property'] in entity['claims']:
                return None  # do not update existing coordinates
            self.add_refs(self.obtain_claim(entity, self.build_snak({'property': 'P6259', 'value': 'Q1264450'})),
                          [self.db_ref])  # J2000 epoch
        claim = super().obtain_claim(entity, snak)
        if claim is not None and snak['property'] in ['P4501']:
            claim['qualifiers'] = {'P4501': [self.build_snak({'property': 'P1013', 'value': 'Q2832068'})]}
        return claim

    def parse_page(self, exoplanet_id):
        page = BeautifulSoup(requests.Session().get("http://exoplanet.eu/catalog/" + exoplanet_id).content,
                             'html.parser')
        sources = self.parse_sources(page)
        properties = {'planet_planet_status_string_0': 'P31', 'planet_discovered_0': 'P575', 'planet_mass_0': 'P2067',
                      'planet_mass_sini_0': 'P2051', 'planet_axis_0': 'P2233', 'planet_period_0': 'P2146',
                      'planet_eccentricity_0': 'P1096', 'planet_omega_0': 'P2248', 'planet_radius_0': 'P2120',
                      'planet_detection_type_0': 'P1046', 'planet_inclination_0': 'P2045', 'planet_albedo_0': 'P4501',
                      'star_0_stars__ra_0': 'P6257', 'star_0_stars__dec_0': 'P6258'}
        mapping = {'Confirmed': 44559, 'MJ': 651336, 'AU': 1811, 'day': 573, 'deg': 28390, 'JD': 14267, 'TTV': 2945337,
                   'Radial Velocity': 2273386, 'm/s': 182429, 'RJ': 3421309, 'Imaging': 15279026, 'Candidate': 18611609,
                   'Primary Transit': 2069919, 'Microlensing': 1028022, 'Astrometry': 181505, 'Controversial': 18611609}
        result = []
        row = {'source': []}
        for td in page.find_all('td'):
            if td.get('id') in properties and td.text != '—':
                if 'value' in row:
                    result.append(row)
                    row = {'source': []}
                row['property'] = properties[td.get('id')]
                if amount := re.search(
                        '(?P<value>\\d[-.e\\d]+)\\s*\\(\\s*(?P<min>-\\S+)\\s+(?P<max>\\+\\d[-.e\\d]+)\\s*\\)(?P<unit>\\s+[A-Za-z]\\S+)?',
                        td.text):
                    row['value'] = amount.group('value')
                    row['min'] = amount.group('min')
                    row['max'] = amount.group('max')
                    if amount.group('unit'):
                        row['unit'] = mapping[amount.group('unit').strip()]
                elif amount := re.search(
                        '^(?P<value>\\d[-.e\\d]+)\\s*(\\(\\s*±\\s*(?P<bound>\\d[-.e\\d]+)\\s*\\))?(?P<unit>\\s+[A-Za-z]\\S+)?$',
                        td.text):
                    row['value'] = amount.group('value')
                    if amount.group('bound'):
                        row['min'] = '-' + amount.group('bound')
                        row['max'] = amount.group('bound')
                    if amount.group('unit'):
                        row['unit'] = mapping[amount.group('unit').strip()]
                else:
                    row['value'] = 'Q' + str(mapping[td.text]) if td.text in mapping else td.text
            elif 'value' in row:
                if 'showArticle' in str(td):
                    ref_id = re.sub('.+\'(\\d+)\'.+', '\g<1>', str(td))
                    if ref_id in sources:
                        row['source'].append(sources[ref_id])
                elif 'showAllPubs' not in str(td):
                    result.append(row)
                    row = {'source': []}

        if 'value' in row:
            result.append(row)
        return result


# info = SyncBot.tap_query('http://voparis-tap-planeto.obspm.fr/tap', '''
#     SELECT  granule_uid AS id, 'Q44559' AS P31, Target_name AS label, detection_type AS P1046, 'exo' AS P1046, discovered AS P575,
#             mass AS P2067, mass_error_min AS P2067_min, mass_error_max AS P2067_max, 'Q651336' AS P2067_unit,
#             radius AS P2120, radius_error_min AS P2120_min, radius_error_max AS P2120_max, 'Q3421309' AS P2120_unit,
#             semi_major_axis   AS P2233, semi_major_axis_error_min AS P2233_min, semi_major_axis_error_max AS P2233_max, 'Q1811' as  p2233_unit,
#             period AS P2146, period_error_min AS P2146_min, period_error_max AS P2146_max, 'Q573' AS P2146_unit,
#             eccentricity AS P1096, eccentricity_error_min AS P1096_min, eccentricity_error_max AS P1096_max,
#             mass_sin_i AS P2051, mass_sin_i_error_min AS P2051_min, mass_sin_i_error_max AS P2051_max, 'Q651336' AS P2051_unit,
#             periastron AS P2248, periastron_error_min AS P2248_min, periastron_error_max AS P2248_max, 'Q28390' AS P2248_unit,
#             albedo AS P4501, albedo_error_min AS P4501_min, albedo_error_max AS P4501_max,
#             inclination AS P2045, inclination_error_min AS P2045_min, inclination_error_max AS P2045_max, 'Q28390' AS P2045_unit,
#             'Q1385430' as ref, star_name AS P397
#     FROM    exoplanet.epn_core
# ''')
wd_items = WikiData.query('SELECT ?id ?item { ?item wdt:P5653 ?id }')
constellations = WikiData.query('SELECT DISTINCT ?n ?i {?i wdt:P31/wdt:P279* wd:Q8928; wdt:P1813 ?n}')

wd = SyncBot(sys.argv[1], sys.argv[2])
wd.db_ref = 'Q1385430'
wd.db_property = 'P5653'

aa_data = json.loads(requests.post('http://exoplanet.eu/catalog/json/',
                                   {'sSearch': '', 'iSortCol_0': 9, 'iDisplayStart': 0, 'iDisplayLength': 10000,
                                    'sSortDir_0': 'desc', 'sEcho': 1}).content)['aaData']
# i = 0
for record in aa_data:
    id = re.sub('<[^<]+?>', '', record[0])

    # if i == 0:
    #     if id == 'Gaia-ASOI-053 b':
    #         i = 1
    #     continue
    print(id)
    if id in wd_items:
        response = json.loads(wd.api_call('wbgetentities', {'props': 'claims|info', 'ids': wd_items[id]}))
        if 'entities' not in response:
            continue
        item = response['entities'][wd_items[id]]
        del wd_items[id]
        # continue
    else:
        # continue
        item = {'claims': {}, 'labels': {'en': {'value': id, 'language': 'en'}}}
        wd.obtain_claim(item, wd.build_snak({'property': 'P5653', 'value': id}))

    wd.update(item, wd.parse_page(id))

    if 'P59' not in item['claims'] and 'P6257' in item['claims'] and 'P6258' in item['claims']:
        point = coord.SkyCoord(
            item['claims']['P6257'][0]['mainsnak']['datavalue']['value']['amount'],
            item['claims']['P6258'][0]['mainsnak']['datavalue']['value']['amount'],
            frame='icrs', unit='deg')
        const = constellations[point.get_constellation(short_name=True)]
        if isinstance(const, str):
            wd.obtain_claim(item, wd.build_snak({'property':'P59', 'value': const}))
        else:
            wd.obtain_claim(item, wd.build_snak({'property':'P59', 'value': const[0]}))

    wd.save(item)
    time.sleep(3)

for id in wd_items:
    print('https://www.wikidata.org/wiki/' + wd_items[id] + ' is missing')
