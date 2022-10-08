#!/usr/bin/python3
import json
import re
import sys
import time
import urllib.parse
from decimal import DecimalException
from os.path import basename

import requests
from astropy import coordinates
from bs4 import BeautifulSoup

from arxiv import ArXiv
from simbad_dap import SimbadDAP
from wikidata import WikiData


class ExoplanetEu(WikiData):
    constellations = None
    sources = {}
    db_property = 'P5653'
    db_ref = 'Q1385430'

    def __init__(self, external_id, qid=None):
        super().__init__(external_id, qid)
        self.properties = {'planet_planet_status_string_0': 'P31', 'planet_axis_0': 'P2233', 'planet_mass_0': 'P2067',
                           'planet_eccentricity_0': 'P1096', 'planet_period_0': 'P2146', 'planet_discovered_0': 'P575',
                           'planet_omega_0': 'P2248', 'planet_radius_0': 'P2120', 'planet_detection_type_0': 'P1046',
                           'planet_albedo_0': 'P4501', 'planet_mass_sini_0': 'P2051', 'planet_inclination_0': 'P2045',
                           'star_0_stars__ra_0': 'P6257', 'star_0_stars__dec_0': 'P6258'}

    @staticmethod
    def get_next_chunk(offset):
        identifiers = []
        offset = 0 if offset is None else offset
        result = requests.post('http://exoplanet.eu/catalog/json/',
                               {'sSearch': '', 'iSortCol_0': 9, 'iDisplayStart': offset, 'sEcho': 1,
                                'iDisplayLength': 1000, 'sSortDir_0': 'desc'})
        if result.status_code == 200:
            aa_data = json.loads(result.content)['aaData']
            for record in aa_data:
                identifiers.append(re.sub('<[^<]+?>', '', record[0]))
            offset += len(identifiers)
        return identifiers, offset

    def obtain_claim(self, snak):
        if snak is None:
            return
        if snak['property'] in ['P6257', 'P6258'] and float(snak['datavalue']['value']['amount']).is_integer():
            return  # do not put obviously wrong coordinates
        if self.entity is not None and 'claims' in self.entity and snak['property'] in self.entity['claims']:
            if snak['property'] in ['P6257', 'P6258']:
                return  # do not update coordinates, because exoplanets.eu ra/dec is usually low precision
            if self.db_property not in self.entity['claims']:  # if updating star
                if snak['property'] in STAR.values():
                    if snak['property'] == 'P1215':
                        for claim in self.entity['claims']['P1215']:
                            if 'qualifiers' in claim and 'P1227' in claim['qualifiers']:
                                if claim['qualifiers']['P1227'][0]['datavalue']['value']['id'] == 'Q4892529':
                                    return
                    else:
                        return
        claim = super().obtain_claim(snak)
        if claim is not None:
            if snak['property'] in ['P4501']:
                claim['qualifiers'] = {'P4501': [self.create_snak('P1013', 'Q2832068')]}
            elif snak['property'] == 'P1215':
                claim['qualifiers'] = {'P1227': [self.create_snak('P1227', 'Q4892529')]}
                claim['rank'] = 'preferred'  # V-magnitude is always preferred
        return claim

    def post_process(self):
        super().post_process()
        if 'P6257' in self.entity['claims'] and 'P6258' in self.entity['claims']:
            self.obtain_claim(WikiData.create_snak('P6259', 'Q1264450'))  # J2000
            if 'P59' not in self.entity['claims']:
                ra = self.entity['claims']['P6257'][0]['mainsnak']['datavalue']['value']['amount']
                dec = self.entity['claims']['P6258'][0]['mainsnak']['datavalue']['value']['amount']
                tla = coordinates.SkyCoord(ra, dec, frame='icrs', unit='deg').get_constellation(short_name=True)
                SPARQL = 'SELECT DISTINCT ?n ?i {?i wdt:P31/wdt:P279* wd:Q8928; wdt:P1813 ?n}'
                self.constellations = self.query(SPARQL) if self.constellations is None else self.constellations
                self.obtain_claim(WikiData.create_snak('P59', self.constellations[tla]))

    @staticmethod
    def parse_url(url: str):
        if not url:
            return
        patterns = {'.*48550/arXiv\\.(\\d{4}.\\d+|[a-z\\-]+(\\.[A-Z]{2})?\\/\\d{7}).*': 'P818=\\g<1>',
                    '(http[s]?://)?(dx\\.)?doi\\.org/': 'P356=',
                    '.*arxiv\\.org/(abs|pdf)/(\\d{4}.\\d+|[a-z\\-]+(\\.[A-Z]{2})?\\/\\d{7}).*': 'P818=\\g<2>',
                    'http[s]?://www\\.journals\\.uchicago\\.edu/doi/abs/': 'P356=',
                    '.*iopscience.iop.org/1538-3881/(.+\\d)/?$': 'P356=10.1088/0004-6256/\\g<1>',
                    '.*iopscience.iop.org/(.+\\d)/?$': 'P356=10.1088/\\g<1>',
                    '.*iop.org/EJ/abstract/1402-4896/(.+\\d)/?$': 'P356=10.1088/0031-8949/\\g<1>',
                    '.*iop.org/EJ/abstract/1538-4357/(.+\\d)/?$': 'P356=10.1088/0004-637X/\\g<1>',
                    '.*iop.org/EJ/abstract/(.+\\d)/?$': 'P356=10.1088/\\g<1>',
                    '.*/aa(\\d+)-(\\d\\d)\\.(html|pdf)': 'P356=10.1051/0004-6361:20\\g<2>\\g<1>',
                    '.*/aa(\\d+)-(\\d{2})\\.(html|pdf)': 'P356=10.1051/0004-6361/20\\g<2>\\g<1>',
                    'http[s]?://(?:ui\\.)?adsabs.harvard.edu/abs/([^/]+).*': 'P819=\\g<1>',
                    'adsabs\\.harvard\\.edu/cgi-bin/nph-bib_query\\?bibcode=([^\\&]+).*': 'P819=\\g<1>',
                    'http://onlinelibrary.wiley.com/doi/([^x]+x).*': 'P356=\\g<1>',
                    'http://online.liebertpub.com/doi/abs/([^\\?]+).*': 'P356=\\g<1>',
                    '.*bn=(\\d{3})(\\d)(\\d{3})(\\d{5})(\\d)': 'P212=\\g<1>-\\g<2>-\\g<3>-\\g<4>-\\g<5>',
                    '.+jstor\\.org/stable/(info/)?': 'P356=',
                    '.*doi=([^&]+)(&.+)?$': 'P356=\\g<1>',
                    '.*/(nature\\d+).html': 'P356=10.1038/\\g<1>'}
        for search_pattern in patterns:
            query = urllib.parse.unquote(re.sub(search_pattern, patterns[search_pattern], url, flags=re.S))
            if query.startswith('P818='):
                if ref_id := ArXiv.get_by_id(query.replace('P818=', '')):
                    return ref_id
            elif query.startswith('P') and (ref_id := WikiData.api_search('haswbstatement:' + query)):
                return ref_id
        print('Could not parse url: ' + url)

    def parse_sources(self, source):
        publications = source.find_all('p', {'class': 'publication'})
        for p in publications:
            links = p.find_all('a', {'target': '_blank'})
            for a in links:
                if p.get('id') not in self.sources and a.get('href') is not None:
                    if (ref_id := self.parse_url(a.get('href').strip())) is not None:
                        self.sources[p.get('id')] = ref_id
                        break

    def parse_text(self, property_id, text):
        ids = {'Confirmed': 44559, 'MJ': 651336, 'AU': 1811, 'day': 573, 'deg': 28390, 'JD': 14267, 'TTV': 2945337,
               'Radial Velocity': 2273386, 'm/s': 182429, 'RJ': 3421309, 'Imaging': 15279026, 'Candidate': 18611609,
               'Primary Transit': 2069919, 'Microlensing': 1028022, 'Astrometry': 181505, 'Controversial': 18611609,
               'Retracted': 7936582, 'pc': 12129, 'Gyr': 524410, 'RSun': 48440, 'K': 11579, 'MSun': 180892}
        num = '\\d[-.e\\d]+'
        unit = '\\s*(?P<unit>[A-Za-z]\\S*)?'
        if reg := re.search(
                '(?P<value>' + num + ')\\s*\\(\\s*-+(?P<min>' + num + ')\\s+(?P<max>\\+' + num + ')\\s*\\)' + unit,
                text):
            result = self.create_snak(property_id, reg.group('value'), reg.group('min'), reg.group('max'))
        elif reg := re.search(
                '^(?P<value>' + num + ')\\s*(\\(\\s*±\\s*(?P<bound>' + num + ')\\s*\\))?' + unit + '$', text):
            if reg.group('bound'):
                result = self.create_snak(property_id, reg.group('value'), reg.group('bound'), reg.group('bound'))
            else:
                result = self.create_snak(property_id, reg.group('value'))
        elif len(deg := text.split(':')) == 3:
            try:
                digits = 3 + (len(text) - text.find('.') - 1 if text.find('.') > 0 else 0)
                mult = 15 if property_id == 'P6257' else 1
                if deg[0].startswith('-'):
                    angle = -((float(deg[2]) / 60 + float(deg[1])) / 60 - float(deg[0]))
                else:
                    angle = +((float(deg[2]) / 60 + float(deg[1])) / 60 + float(deg[0]))
                result = self.create_snak(property_id, self.format_float(angle * mult, digits))
                result['datavalue']['value']['unit'] = 'http://www.wikidata.org/entity/Q28390'
            except (ValueError, DecimalException):
                return self.create_snak(property_id, text)
        elif text in ids:
            return self.create_snak(property_id, 'Q' + str(ids[text]))
        else:
            return self.create_snak(property_id, text)

        if reg is not None and reg.group('unit'):
            result['datavalue']['value']['unit'] = 'http://www.wikidata.org/entity/Q' + str(ids[reg.group('unit')])
        return result

    def prepare_data(self, source=None):
        super().prepare_data()
        if not (parsing_planet := ('P1046' in self.properties.values())):
            self.input_snaks = []  # do not write P5356:exoplanet_id for the host star
        self.parse_sources(source)
        current_snak = None
        for td in source.find_all('td'):
            if td.get('id') in self.properties and td.text != '—':
                if current_snak is not None:
                    self.input_snaks.append(current_snak)
                current_snak = self.parse_text(self.properties[td.get('id')], td.text)
            elif current_snak is not None:
                if 'showArticle' in str(td) and (ref_id := re.sub('.+\'(\\d+)\'.+', '\\g<1>', str(td))) in self.sources:
                    current_snak['source'] = [] if 'source' not in current_snak else current_snak['source']
                    current_snak['source'].append(self.sources[ref_id])
                elif 'showAllPubs' not in str(td) and current_snak is not None:
                    self.input_snaks.append(current_snak)
                    current_snak = None
            elif len(td.attrs) == 0 and parsing_planet and (td.parent.parent.get('id') == 'table_' + td.text):
                ident = SimbadDAP.tap_query('https://simbad.u-strasbg.fr/simbad/sim-tap',
                                            'SELECT main_id FROM ident JOIN basic ON oid = oidref ' +
                                            'WHERE id=\'' + td.text + '\'')
                if len(ident) == 1:
                    no_parent = self.entity and 'claims' in self.entity and 'P397' not in self.entity['claims']
                    if host_id := SimbadDAP.get_by_id(list(ident.keys())[0], no_parent):
                        current_snak = ExoplanetEu.create_snak('P397', host_id)

        if current_snak is not None:
            self.input_snaks.append(current_snak)


STAR = {'star_0_stars__distance_0': 'P2583', 'star_0_stars__spec_type_0': 'P215', 'star_0_stars__age_0': 'P7584',
        'star_0_stars__magnitude_v_0': 'P1215', 'star_0_stars__teff_0': 'P6879', 'star_0_stars__radius_0': 'P2120',
        'star_0_stars__metallicity_0': 'P2227', 'star_0_stars__mass_0': 'P2067'}

if sys.argv[0].endswith(basename(__file__)):  # if not imported
    ExoplanetEu.logon(sys.argv[1], sys.argv[2])
    wd_items = ExoplanetEu.get_all_items('SELECT ?id ?item {?item p:P5653/ps:P5653 ?id}')

    for ex_id in wd_items:
        # ex_id = '55 Cnc e'
        item = ExoplanetEu(ex_id, wd_items[ex_id])
        try:
            response = requests.Session().get("http://exoplanet.eu/catalog/" + ex_id)
            if response.status_code != 200:
                print('Error {} while retrieving "{}", skipping it'.format(response.status_code, ex_id))
                continue
        except (requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout) as e:
            print('Error {} while retrieving "{}", skipping it'.format(e, ex_id))
            continue
        page = BeautifulSoup(response.content, 'html.parser')
        item.prepare_data(page)
        item.update()
        if 'P397' in item.entity['claims'] and len(item.entity['claims']['P397']) == 1:
            if 'datavalue' in item.entity['claims']['P397'][0]['mainsnak']:  # parent != "novalue"
                parent = ExoplanetEu(ex_id, item.entity['claims']['P397'][0]['mainsnak']['datavalue']['value']['id'])
                parent.properties = STAR
                parent.prepare_data(page)
                parent.update()
        page.decompose()
        time.sleep(10)
