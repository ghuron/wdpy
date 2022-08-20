#!/usr/bin/python3
import json
import re
import sys
import time
import requests
from wikidata import WikiData
from bs4 import BeautifulSoup, Tag
from astropy import coordinates as coord


class ExoplanetEu(WikiData):
    def __init__(self, login, password):
        super().__init__(login, password)
        self.db_ref = 'Q1385430'
        self.db_property = 'P5653'
        self.constellations = self.query('SELECT DISTINCT ?n ?i {?i wdt:P31/wdt:P279* wd:Q8928; wdt:P1813 ?n}')

    def get_summary(self, entity):
        return 'batch import from [[Q1385430|exoplanet.eu]] for object ' + \
               entity['claims']['P5653'][0]['mainsnak']['datavalue']['value']

    def get_chunk_from_search(self, offset):
        result = []
        response = requests.post('http://exoplanet.eu/catalog/json/',
                                 {'sSearch': '', 'iSortCol_0': 9, 'iDisplayStart': offset, 'sEcho': 1,
                                  'iDisplayLength': 1000, 'sSortDir_0': 'desc'})
        if response.status_code == 200:
            aa_data = json.loads(response.content)['aaData']
            for record in aa_data:
                result.append(re.sub('<[^<]+?>', '', record[0]))
        return result

    def parse_sources(self, page):
        patterns = {'(http[s]?://)?(dx\\.)?doi\\.org/': 'haswbstatement:P356=',
                    'http[s]?://(fr\\.)?arxiv\\.org/abs/': 'haswbstatement:P818=',
                    'https://doi.org/10.48550/arXiv.': 'haswbstatement:P818=',
                    'http[s]?://www\\.journals\\.uchicago\\.edu/doi/abs/': 'haswbstatement:P356=',
                    'http://iopscience.iop.org/0004-637X/': 'haswbstatement:P356=10.1088/0004-637X/',
                    'http[s]?://(?:ui\\.)?adsabs.harvard.edu/abs/([^/]+).*': 'haswbstatement:P819=\\g<1>',
                    'adsabs\\.harvard\\.edu/cgi-bin/nph-bib_query\\?bibcode=([^\\&]+).*': 'haswbstatement:P819=\\g<1>',
                    'http://onlinelibrary.wiley.com/doi/([^x]+x).*': 'haswbstatement:P356=\\g<1>',
                    'http://online.liebertpub.com/doi/abs/([^\\?]+).*': 'haswbstatement:P356=\\g<1>,',
                    'isbn=(\\d{3})(\\d)(\\d{3})(\\d{5})(\\d)': 'haswbstatement:P212=\\g<1>-\\g<2>-\\g<3>-\\g<4>-\\g<5>',
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

    def parse_page(self, suffix):
        response = requests.Session().get("http://exoplanet.eu/catalog/" + suffix)
        if response.status_code != 200:
            return None
        page = BeautifulSoup(response.content, 'html.parser')
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
                    ref_id = re.sub('.+\'(\\d+)\'.+', '\\g<1>', str(td))
                    if ref_id in sources:
                        row['source'].append(sources[ref_id])
                elif 'showAllPubs' not in str(td):
                    result.append(row)
                    row = {'source': []}

        if 'value' in row:
            result.append(row)
        return result

    def add_constellation(self, item):
        if 'P59' not in item['claims'] and 'P6257' in item['claims'] and 'P6258' in item['claims']:
            point = coord.SkyCoord(
                item['claims']['P6257'][0]['mainsnak']['datavalue']['value']['amount'],
                item['claims']['P6258'][0]['mainsnak']['datavalue']['value']['amount'],
                frame='icrs', unit='deg')
            const = self.constellations[point.get_constellation(short_name=True)]
            if isinstance(const, str):
                wd.obtain_claim(item, wd.build_snak({'property': 'P59', 'value': const}))
            else:
                wd.obtain_claim(item, wd.build_snak({'property': 'P59', 'value': const[0]}))


wd = ExoplanetEu(sys.argv[1], sys.argv[2])
wd_items = wd.get_all_items('SELECT ?id ?item { ?item wdt:P5653 ?id }')

for exoplanet_id in wd_items:
    # exoplanet_id = 'Gaia-ASOI-053 b'
    print(exoplanet_id)
    if wd_items[exoplanet_id] is not None:
        info = json.loads(wd.api_call('wbgetentities', {'props': 'claims|info', 'ids': wd_items[exoplanet_id]}))
        if 'entities' not in info:
            continue
        item = info['entities'][wd_items[exoplanet_id]]
    else:
        item = {'claims': {}, 'labels': {'en': {'value': exoplanet_id, 'language': 'en'}}}
        wd.obtain_claim(item, wd.build_snak({'property': 'P5653', 'value': exoplanet_id}))

    if data := wd.parse_page(exoplanet_id):
        wd.update(item, data)
        wd.add_constellation(item)
        wd.save(item)
    else:
        wd.trace(item, 'was not updated because corresponding exoplanet.eu page was not parsed')

    time.sleep(3)
