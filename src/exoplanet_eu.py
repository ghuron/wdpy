#!/usr/bin/python3
import json
import re
import sys
import time
import requests
import os.path
from simbad_dap import SimbadDAP
from wikidata import WikiData
from bs4 import BeautifulSoup
from astropy import coordinates as coord


class ExoplanetEu(WikiData):
    def __init__(self, login, password):
        super().__init__(login, password)
        self.db_ref = 'Q1385430'
        self.db_property = 'P5653'
        self.offset = 0
        self.force_parent_creation = False
        self.simbad = SimbadDAP(login, password)
        self.constellations = self.query('SELECT DISTINCT ?n ?i {?i wdt:P31/wdt:P279* wd:Q8928; wdt:P1813 ?n}')

    def get_next_chunk(self):
        result = []
        response = requests.post('http://exoplanet.eu/catalog/json/',
                                 {'sSearch': '', 'iSortCol_0': 9, 'iDisplayStart': self.offset, 'sEcho': 1,
                                  'iDisplayLength': 1000, 'sSortDir_0': 'desc'})
        if response.status_code == 200:
            aa_data = json.loads(response.content)['aaData']
            for record in aa_data:
                result.append(re.sub('<[^<]+?>', '', record[0]))
            self.offset += len(result)
        return result

    def obtain_claim(self, entity, snak):
        if snak is not None and snak['property'] in ['P6257', 'P6258']:
            if snak['property'] in entity['claims'] or float(snak['datavalue']['value']['amount']).is_integer():
                return None  # do not update existing coordinates, do not put obviously wrong coordinates
            self.add_refs(self.obtain_claim(entity, self.create_snak('P6259', 'Q1264450')), [self.db_ref])  # J2000
        claim = super().obtain_claim(entity, snak)
        if claim is not None and snak['property'] in ['P4501']:
            claim['qualifiers'] = {'P4501': [self.create_snak('P1013', 'Q2832068')]}
        return claim

    def post_process(self, entity):
        super().post_process(entity)
        if 'P59' not in entity['claims'] and 'P6257' in entity['claims'] and 'P6258' in entity['claims']:
            p = coord.SkyCoord(entity['claims']['P6257'][0]['mainsnak']['datavalue']['value']['amount'],
                               entity['claims']['P6258'][0]['mainsnak']['datavalue']['value']['amount'], frame='icrs',
                               unit='deg')
            const = self.constellations[p.get_constellation(short_name=True)]
            self.obtain_claim(entity, self.create_snak('P59', const if isinstance(const, str) else const[0]))

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

        result = {}
        publications = page.find_all('p', {'class': 'publication'})
        for p in publications:
            links = p.find_all('a', {'target': '_blank'})
            for a in links:
                if a.get('href') is not None:
                    for search_pattern in patterns:
                        query = re.sub(search_pattern, patterns[search_pattern], a.get('href').strip())
                        if query.startswith('haswbstatement'):
                            if ref_id := self.api_search(query):
                                result[p.get('id')] = ref_id
                                break
                    # print(a.get('href') + ' is missing')
        return result

    def get_snaks(self, suffix):
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
                   'Primary Transit': 2069919, 'Microlensing': 1028022, 'Astrometry': 181505, 'Controversial': 18611609,
                   'Retracted': 7936582}
        result = []
        current_snak = None
        for td in page.find_all('td'):
            if td.get('id') in properties and td.text != '—':
                if current_snak is not None:
                    result.append(current_snak)
                    current_snak = None

                if amount := re.search(
                        '(?P<value>\\d[-.e\\d]+)\\s*\\(\\s*(?P<min>-\\S+)\\s+(?P<max>\\+\\d[-.e\\d]+)\\s*\\)(?P<unit>\\s+[A-Za-z]\\S+)?',
                        td.text):
                    current_snak = self.create_snak(properties[td.get('id')], amount.group('value'),
                                                    amount.group('min'), amount.group('max'))
                    if current_snak is not None and amount.group('unit'):
                        current_snak['datavalue']['value']['unit'] = \
                            'http://www.wikidata.org/entity/Q' + str(mapping[amount.group('unit').strip()])
                elif amount := re.search(
                        '^(?P<value>\\d[-.e\\d]+)\\s*(\\(\\s*±\\s*(?P<bound>\\d[-.e\\d]+)\\s*\\))?(?P<unit>\\s+[A-Za-z]\\S+)?$',
                        td.text):
                    if amount.group('bound'):
                        current_snak = self.create_snak(properties[td.get('id')], amount.group('value'),
                                                        '-' + amount.group('bound'), amount.group('bound'))
                    else:
                        current_snak = self.create_snak(properties[td.get('id')], amount.group('value'))
                    if current_snak is not None and amount.group('unit'):
                        current_snak['datavalue']['value']['unit'] = \
                            'http://www.wikidata.org/entity/Q' + str(mapping[amount.group('unit').strip()])
                elif td.text in mapping:
                    current_snak = self.create_snak(properties[td.get('id')], 'Q' + str(mapping[td.text]))
                elif properties[td.get('id')] == 'P6257':
                    digits = 3 + (len(td.text) - td.text.find('.') - 1 if td.text.find('.') > 0 else 0)
                    ra = td.text.split(':')
                    current_snak = self.create_snak(properties[td.get('id')], self.format_float(
                        ((float(ra[2]) / 60 + float(ra[1])) / 60 + float(ra[0])) * 15, digits))
                    if current_snak is not None:
                        current_snak['datavalue']['value']['unit'] = 'http://www.wikidata.org/entity/Q28390'
                elif properties[td.get('id')] == 'P6258':
                    digits = 3 + (len(td.text) - td.text.find('.') - 1 if td.text.find('.') > 0 else 0)
                    dec = td.text.split(':')
                    if dec[0].startswith('-'):
                        current_snak = self.create_snak(properties[td.get('id')], self.format_float(
                            -((float(dec[2]) / 60 + float(dec[1])) / 60 - float(dec[0])), digits))
                    else:
                        current_snak = self.create_snak(properties[td.get('id')], self.format_float(
                            ((float(dec[2]) / 60 + float(dec[1])) / 60 + float(dec[0])), digits))
                    if current_snak is not None:
                        current_snak['datavalue']['value']['unit'] = 'http://www.wikidata.org/entity/Q28390'
                else:
                    current_snak = self.create_snak(properties[td.get('id')], td.text)
            elif current_snak is not None:
                if 'showArticle' in str(td):
                    ref_id = re.sub('.+\'(\\d+)\'.+', '\\g<1>', str(td))
                    if ref_id in sources:
                        if 'source' not in current_snak:
                            current_snak['source'] = []
                        current_snak['source'].append(sources[ref_id])
                elif 'showAllPubs' not in str(td):
                    result.append(current_snak)
                    current_snak = None
            elif len(td.attrs) == 0 and td.parent.parent.get('id') == 'table_' + td.text:
                ident = self.simbad.tap_query('https://simbad.u-strasbg.fr/simbad/sim-tap',
                                              'SELECT main_id FROM ident JOIN basic ON oid = oidref ' +
                                              'WHERE id=\'' + td.text + '\'')
                if len(ident) != 1:
                    continue
                simbad_id = list(ident.keys())[0]
                if (parent_id := self.api_search('haswbstatement:"P3083=' + simbad_id + '"')) is None:
                    if not self.force_parent_creation:
                        continue
                    if (parent_id := self.simbad.sync(simbad_id)) != '':
                        continue

                print('adding ' + parent_id + ' to ' + suffix)
                current_snak = self.create_snak('P397', parent_id)

        if current_snak is not None:
            result.append(current_snak)
        return result


if sys.argv[0].endswith(os.path.basename(__file__)):  # if not imported
    wd = ExoplanetEu(sys.argv[1], sys.argv[2])
    wd_items = wd.get_all_items('SELECT ?id ?item {?item p:P5653/ps:P5653 ?id}')

    for ex_id in wd_items:
        # ex_id = 'Gaia-ASOI-031 b'
        if wd_items[ex_id] is not None:
            try:
                info = json.loads(wd.api_call('wbgetentities', {'props': 'claims|info|labels', 'ids': wd_items[ex_id]}))
            except json.decoder.JSONDecodeError:
                print('Cannot decode wbgetentities response')
                continue
            except requests.exceptions.ConnectionError:
                print('Connection error while calling wbgetentities')
                continue
            if 'entities' not in info:
                continue
            item = info['entities'][wd_items[ex_id]]
        else:
            # continue  # uncomment if we do not want to create new items
            item = {}
        wd.force_parent_creation = 'claims' in item and 'P397' not in item['claims']  # no P397 claim
        wd.sync(ex_id, item)
        time.sleep(2)
