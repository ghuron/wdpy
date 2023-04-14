#!/usr/bin/python3
import json
import re
from decimal import DecimalException
from os.path import basename
from sys import argv
from time import sleep
from urllib.parse import unquote

import requests
from astropy import coordinates
from bs4 import BeautifulSoup

from arxiv import ArXiv
from simbad_dap import SimbadDAP
from wikidata import WikiData


class ExoplanetEu(WikiData):
    constellations = None
    sources = {'4966': 'Q66424531'}
    db_property = 'P5653'
    db_ref = 'Q1385430'

    def __init__(self, external_id, qid=None):
        super().__init__(external_id, qid)
        self.properties = {'planet_planet_status_string_0': 'P31', 'planet_axis_0': 'P2233', 'planet_mass_0': 'P2067',
                           'planet_eccentricity_0': 'P1096', 'planet_period_0': 'P2146', 'planet_discovered_0': 'P575',
                           'planet_omega_0': 'P2248', 'planet_radius_0': 'P2120', 'planet_detection_type_0': 'P1046',
                           'planet_albedo_0': 'P4501', 'planet_mass_sini_0': 'P2051', 'planet_inclination_0': 'P2045',
                           'star_0_stars__ra_0': 'P6257', 'star_0_stars__dec_0': 'P6258',
                           'star_0_stars__alternate_names_0': 'P397'}

    @staticmethod
    def get_next_chunk(offset):
        identifiers = []
        offset = 0 if offset is None else offset
        params = {'sSearch': '', 'iSortCol_0': 9, 'iDisplayStart': offset, 'sEcho': 1, 'iDisplayLength': 1000,
                  'sSortDir_0': 'desc'}
        if (result := requests.post('http://exoplanet.eu/catalog/json/', params)).status_code == 200:
            for record in json.loads(result.content)['aaData']:
                identifiers.append(re.sub('<[^<]+?>', '', record[0]))
        return identifiers, offset + params['iDisplayLength']

    def obtain_claim(self, snak):
        if snak is None:
            return
        if snak['property'] in ['P6257', 'P6258'] and float(snak['datavalue']['value']['amount']).is_integer():
            return  # do not put obviously wrong coordinates
        if self.entity and 'claims' in self.entity and snak['property'] in self.entity['claims']:
            if snak['property'] in ['P6257', 'P6258']:
                return  # do not update coordinates, because exoplanets.eu ra/dec is usually low precision
            if self.db_property not in self.entity['claims'] or \
                    self.entity['claims'][self.db_property][0]['mainsnak']['datavalue']['value'] != self.external_id:
                if snak['property'] in STAR.values():
                    if snak['property'] == 'P1215':
                        for claim in self.entity['claims']['P1215']:
                            if 'qualifiers' in claim and 'P1227' in claim['qualifiers']:
                                if claim['qualifiers']['P1227'][0]['datavalue']['value']['id'] == 'Q4892529':
                                    return
                    else:
                        return
        if claim := super().obtain_claim(snak):
            if snak['property'] in ['P4501']:
                claim['qualifiers'] = {'P4501': [WikiData.create_snak('P1013', 'Q2832068')]}
            elif snak['property'] == 'P1215':
                claim['qualifiers'] = {'P1227': [WikiData.create_snak('P1227', 'Q4892529')]}
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
        transform = {'.*48550/arXiv\\.(\\d{4}.\\d+|[a-z\\-]+(\\.[A-Z]{2})?\\/\\d{7}).*': 'P818=\\g<1>',
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
                     '.*/articles/aa/abs/2004/18/aa0959/aa0959.html': 'P356=10.1051/0004-6361:20035959',
                     'http[s]?://(?:ui\\.)?adsabs.harvard.edu/abs/([^/]+).*': 'P819=\\g<1>',
                     'adsabs\\.harvard\\.edu/cgi-bin/nph-bib_query\\?bibcode=([^\\&]+).*': 'P819=\\g<1>',
                     'http://onlinelibrary.wiley.com/doi/([^x]+x).*': 'P356=\\g<1>',
                     'http://online.liebertpub.com/doi/abs/([^\\?]+).*': 'P356=\\g<1>',
                     '.*bn=(\\d{3})(\\d)(\\d{3})(\\d{5})(\\d)': 'P212=\\g<1>-\\g<2>-\\g<3>-\\g<4>-\\g<5>',
                     '.+jstor\\.org/stable/(info/)?': 'P356=',
                     '.*doi=([^&]+)(&.+)?$': 'P356=\\g<1>',
                     '.*/(nature\\d+).html': 'P356=10.1038/\\g<1>'}
        if url and url.strip() and (url := url.split()[0]):
            for pattern in transform:
                if (query := unquote(re.sub(pattern, transform[pattern], url, flags=re.S))).startswith('P'):
                    if query.startswith('P818='):
                        return ArXiv.get_by_id(query.replace('P818=', ''))
                    return WikiData.api_search('haswbstatement:' + query)

    def retrieve(self):
        try:
            if (response := requests.get("http://exoplanet.eu/catalog/" + self.external_id)).status_code != 200:
                self.trace('{}\tresponse: {}'.format(response.url, response.status_code), 40)
                return
        except requests.exceptions.RequestException as e:
            self.trace(e.__str__(), 40)
            return
        page = BeautifulSoup(response.content, 'html.parser')

        for p in page.find_all('p', {'class': 'publication'}):
            if p.get('id') not in ExoplanetEu.sources:
                for a in p.find_all('a', {'target': '_blank'}):
                    if ref_id := ExoplanetEu.parse_url(a.get('href')):
                        ExoplanetEu.sources[p.get('id')] = ref_id
                        break
                if (p.get('id') not in ExoplanetEu.sources) and (ref_id := ExoplanetEu.find_by_title(p.find('b').text)):
                    ExoplanetEu.sources[p.get('id')] = ref_id
        return page

    @staticmethod
    def find_by_title(title: str):
        if title and len(title := ' '.join(title.replace('\n', ' ').rstrip('.').split())) > 32:
            return WikiData.api_search('"{}" -haswbstatement:P31=Q1348305'.format(title))

    @staticmethod
    def create_snak(property_id: str, value, lower=None, upper=None):
        ids = {'Confirmed': 44559, 'MJ': 651336, 'AU': 1811, 'day': 573, 'deg': 28390, 'JD': 14267, 'TTV': 2945337,
               'Radial Velocity': 2273386, 'm/s': 182429, 'RJ': 3421309, 'Imaging': 15279026, 'Candidate': 18611609,
               'Primary Transit': 2069919, 'Microlensing': 1028022, 'Astrometry': 181505, 'Controversial': 18611609,
               'Retracted': 7936582, 'pc': 12129, 'Gyr': 524410, 'RSun': 48440, 'K': 11579, 'MSun': 180892}
        num = '\\d[-\\+.eE\\d]+'
        unit = '\\s*(?P<unit>[A-Za-z]\\S*)?'
        if reg := re.search(
                '(?P<value>' + num + ')\\s*\\(\\s*-+(?P<min>' + num + ')\\s+(?P<max>\\+' + num + ')\\s*\\)' + unit,
                value):
            result = WikiData.create_snak(property_id, reg.group('value'), reg.group('min'), reg.group('max'))
        elif reg := re.search(
                '^(?P<value>' + num + ')\\s*(\\(\\s*±\\s*(?P<bound>' + num + ')\\s*\\))?' + unit + '$', value):
            if reg.group('bound'):
                result = WikiData.create_snak(property_id, reg.group('value'), reg.group('bound'), reg.group('bound'))
            else:
                result = WikiData.create_snak(property_id, reg.group('value'))
        elif len(deg := value.split(':')) == 3:  # coordinates
            try:
                digits = 3 + (len(value) - value.find('.') - 1 if value.find('.') > 0 else 0)
                mult = 15 if property_id == 'P6257' else 1
                if deg[0].startswith('-'):
                    angle = -((float(deg[2]) / 60 + float(deg[1])) / 60 - float(deg[0]))
                else:
                    angle = +((float(deg[2]) / 60 + float(deg[1])) / 60 + float(deg[0]))
                result = WikiData.create_snak(property_id, WikiData.format_float(angle * mult, digits))
                result['datavalue']['value']['unit'] = 'http://www.wikidata.org/entity/Q28390'
            except (ValueError, DecimalException):
                return WikiData.create_snak(property_id, value)
        elif property_id == 'P397':
            query = 'SELECT main_id FROM ident JOIN basic ON oid = oidref WHERE id=\'{}\''.format(value)
            if len(ident := SimbadDAP.tap_query('https://simbad.u-strasbg.fr/simbad/sim-tap', query)) != 1:
                return
            # no_parent = self.entity and 'claims' in self.entity and 'P397' not in self.entity['claims']
            return WikiData.create_snak(property_id, SimbadDAP.get_by_id(list(ident.keys())[0], False))
        elif value in ids:
            return WikiData.create_snak(property_id, 'Q' + str(ids[value]))
        else:
            return WikiData.create_snak(property_id, value)

        if result and reg and reg.group('unit'):
            result['datavalue']['value']['unit'] = 'http://www.wikidata.org/entity/Q' + str(ids[reg.group('unit')])
        return result

    def prepare_data(self, source=None):
        if not source:
            return
        super().prepare_data()
        if not (parsing_planet := ('P1046' in self.properties.values())):
            self.input_snaks = []  # do not write P5356:exoplanet_id for the host star
        current_snak = None
        for td in source.find_all('td'):
            if td.get('id') in self.properties and td.text != '—':
                if current_snak is not None:
                    self.input_snaks.append(current_snak)
                current_snak = ExoplanetEu.create_snak(self.properties[td.get('id')], td.text)
            elif current_snak is not None:
                if 'showArticle' in str(td):
                    if (ref_id := re.sub('.+\'(\\d+)\'.+', '\\g<1>', str(td), flags=re.S)) in self.sources:
                        current_snak['source'] = [] if 'source' not in current_snak else current_snak['source']
                        current_snak['source'].append(self.sources[ref_id])
                    elif ref_id is not None:
                        self.trace("source {} could have been used".format(ref_id), 30)
                elif 'showAllPubs' not in str(td):
                    self.input_snaks.append(current_snak)
                    current_snak = None
            elif parsing_planet and len(td.attrs) == 0 and (td.parent.parent.get('id') == 'table_' + td.text):
                current_snak = ExoplanetEu.create_snak('P397', td.text)

        if current_snak is not None:
            self.input_snaks.append(current_snak)


STAR = {'star_0_stars__distance_0': 'P2583', 'star_0_stars__spec_type_0': 'P215', 'star_0_stars__age_0': 'P7584',
        'star_0_stars__magnitude_v_0': 'P1215', 'star_0_stars__teff_0': 'P6879', 'star_0_stars__radius_0': 'P2120',
        'star_0_stars__metallicity_0': 'P2227', 'star_0_stars__mass_0': 'P2067'}

if argv[0].endswith(basename(__file__)):  # if not imported
    ExoplanetEu.logon(argv[1], argv[2])
    for ex_id, wd_item in ExoplanetEu.get_all_items('SELECT ?id ?item {?item p:P5653/ps:P5653 ?id}').items():
        # ex_id, wd_item = 'HD 190360 b', 'Q1072888'  # uncomment to debug specific item only
        item = ExoplanetEu(ex_id, wd_item)
        if data := item.retrieve():
            item.prepare_data(data)
            item.update()
            if item.entity and 'P397' in item.entity['claims'] and len(item.entity['claims']['P397']) == 1:
                if 'datavalue' in (parent := item.entity['claims']['P397'][0]['mainsnak']):  # parent != "novalue"
                    host = ExoplanetEu(ex_id, parent['datavalue']['value']['id'])
                    host.properties = STAR
                    host.prepare_data(data)
                    host.update()
            data.decompose()
        sleep(4)
