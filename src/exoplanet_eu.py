#!/usr/bin/python3
import json
import re
import sys
import time
import urllib.parse
import requests

from arxiv import ArXiv
from os.path import basename
from simbad_dap import SimbadDAP
from wikidata import WikiData
from bs4 import BeautifulSoup
from astropy import coordinates


class ExoplanetEu(WikiData):
    def __init__(self, login, password):
        super().__init__(login, password)
        self.db_ref = 'Q1385430'
        self.db_property = 'P5653'
        self.offset = 0
        self.constellations = None
        self.source = {}
        self.simbad = None
        self.arxiv = None

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
            ra = entity['claims']['P6257'][0]['mainsnak']['datavalue']['value']['amount']
            dec = entity['claims']['P6258'][0]['mainsnak']['datavalue']['value']['amount']
            tla = coordinates.SkyCoord(ra, dec, frame='icrs', unit='deg').get_constellation(short_name=True)
            if self.constellations is None:
                self.constellations = self.query('SELECT DISTINCT ?n ?i {?i wdt:P31/wdt:P279* wd:Q8928; wdt:P1813 ?n}')
            self.obtain_claim(entity, self.create_snak('P59', self.constellations[tla]))

    def parse_url(self, url):
        patterns = {'https://doi.org/10.48550/arXiv.': 'haswbstatement:P818=',
                    '(http[s]?://)?(dx\\.)?doi\\.org/': 'haswbstatement:P356=',
                    '.*arxiv\\.org/abs/(\\d{4}.\\d+|[a-z\\-]+(\\.[A-Z]{2})?\\/\\d{7}).*': 'haswbstatement:P818=\\g<1>',
                    'http[s]?://www\\.journals\\.uchicago\\.edu/doi/abs/': 'haswbstatement:P356=',
                    'http://iopscience.iop.org/0004-637X/': 'haswbstatement:P356=10.1088/0004-637X/',
                    'http[s]?://(?:ui\\.)?adsabs.harvard.edu/abs/([^/]+).*': 'haswbstatement:P819=\\g<1>',
                    'adsabs\\.harvard\\.edu/cgi-bin/nph-bib_query\\?bibcode=([^\\&]+).*': 'haswbstatement:P819=\\g<1>',
                    'http://onlinelibrary.wiley.com/doi/([^x]+x).*': 'haswbstatement:P356=\\g<1>',
                    'http://online.liebertpub.com/doi/abs/([^\\?]+).*': 'haswbstatement:P356=\\g<1>,',
                    'isbn=(\\d{3})(\\d)(\\d{3})(\\d{5})(\\d)': 'haswbstatement:P212=\\g<1>-\\g<2>-\\g<3>-\\g<4>-\\g<5>',
                    '.+jstor\\.org/stable/(info/)?': 'haswbstatement:P356='}
        for search_pattern in patterns:
            query = re.sub(search_pattern, patterns[search_pattern], url)
            if query.startswith('haswbstatement'):
                if (ref_id := self.api_search(urllib.parse.unquote(query))) is None:
                    if query.startswith('haswbstatement:P818='):
                        self.arxiv = ArXiv(self.login, self.password) if self.arxiv is None else self.arxiv
                        ref_id = self.arxiv.sync(query.replace('haswbstatement:P818=', ''))
                if ref_id is not None:
                    return ref_id

    def parse_sources(self, page):
        publications = page.find_all('p', {'class': 'publication'})
        for p in publications:
            links = p.find_all('a', {'target': '_blank'})
            for a in links:
                if p.get('id') not in self.source and a.get('href') is not None:
                    if (ref_id := self.parse_url(a.get('href').strip())) is not None:
                        self.source[p.get('id')] = ref_id
                        break

    def parse_text(self, property_id, text):
        ids = {'Confirmed': 44559, 'MJ': 651336, 'AU': 1811, 'day': 573, 'deg': 28390, 'JD': 14267, 'TTV': 2945337,
               'Radial Velocity': 2273386, 'm/s': 182429, 'RJ': 3421309, 'Imaging': 15279026, 'Candidate': 18611609,
               'Primary Transit': 2069919, 'Microlensing': 1028022, 'Astrometry': 181505, 'Controversial': 18611609,
               'Retracted': 7936582}
        num = '\\d[-.e\\d]+'
        unit = '\\s*(?P<unit>[A-Za-z]\\S+)?'
        if reg := re.search(
                '(?P<value>' + num + ')\\s*\\(\\s*(?P<min>-' + num + ')\\s+(?P<max>\\+' + num + ')\\s*\\)' + unit,
                text):
            result = self.create_snak(property_id, reg.group('value'), reg.group('min'), reg.group('max'))
        elif reg := re.search(
                '^(?P<value>' + num + ')\\s*(\\(\\s*±\\s*(?P<bound>' + num + ')\\s*\\))?' + unit + '$', text):
            if reg.group('bound'):
                result = self.create_snak(property_id, reg.group('value'), reg.group('bound'), reg.group('bound'))
            else:
                result = self.create_snak(property_id, reg.group('value'))
        elif len(deg := text.split(':')) == 3:
            digits = 3 + (len(text) - text.find('.') - 1 if text.find('.') > 0 else 0)
            mult = 15 if property_id == 'P6257' else 1
            if deg[0].startswith('-'):
                angle = -((float(deg[2]) / 60 + float(deg[1])) / 60 - float(deg[0]))
            else:
                angle = +((float(deg[2]) / 60 + float(deg[1])) / 60 + float(deg[0]))
            result = self.create_snak(property_id, self.format_float(angle * mult, digits))
            result['datavalue']['value']['unit'] = 'http://www.wikidata.org/entity/Q28390'
        elif text in ids:
            return self.create_snak(property_id, 'Q' + str(ids[text]))
        else:
            return self.create_snak(property_id, text)

        if reg is not None and reg.group('unit'):
            result['datavalue']['value']['unit'] = 'http://www.wikidata.org/entity/Q' + str(ids[reg.group('unit')])
        return result

    def load_snaks(self, external_id, force_parent_creation):
        response = requests.Session().get("http://exoplanet.eu/catalog/" + external_id)
        if response.status_code != 200:
            return None
        page = BeautifulSoup(response.content, 'html.parser')
        self.parse_sources(page)
        properties = {'planet_planet_status_string_0': 'P31', 'planet_discovered_0': 'P575', 'planet_mass_0': 'P2067',
                      'planet_mass_sini_0': 'P2051', 'planet_axis_0': 'P2233', 'planet_period_0': 'P2146',
                      'planet_eccentricity_0': 'P1096', 'planet_omega_0': 'P2248', 'planet_radius_0': 'P2120',
                      'planet_detection_type_0': 'P1046', 'planet_inclination_0': 'P2045', 'planet_albedo_0': 'P4501',
                      'star_0_stars__ra_0': 'P6257', 'star_0_stars__dec_0': 'P6258'}
        result = [self.create_snak(self.db_property, external_id)]
        current_snak = None
        for td in page.find_all('td'):
            if td.get('id') in properties and td.text != '—':
                if current_snak is not None:
                    result.append(current_snak)
                current_snak = self.parse_text(properties[td.get('id')], td.text)
            elif current_snak is not None:
                if 'showArticle' in str(td):
                    ref_id = re.sub('.+\'(\\d+)\'.+', '\\g<1>', str(td))
                    if ref_id in self.source:
                        if 'source' not in current_snak:
                            current_snak['source'] = []
                        current_snak['source'].append(self.source[ref_id])
                elif 'showAllPubs' not in str(td) and current_snak is not None:
                    result.append(current_snak)
                    current_snak = None
            elif len(td.attrs) == 0 and td.parent.parent.get('id') == 'table_' + td.text:
                ident = SimbadDAP.tap_query('https://simbad.u-strasbg.fr/simbad/sim-tap',
                                            'SELECT main_id FROM ident JOIN basic ON oid = oidref ' +
                                            'WHERE id=\'' + td.text + '\'')
                if len(ident) != 1:
                    continue
                simbad_id = list(ident.keys())[0]
                if (parent_id := self.api_search('haswbstatement:"P3083=' + simbad_id + '"')) is None:
                    if not force_parent_creation:
                        continue
                    self.simbad = SimbadDAP(self.login, self.password) if self.simbad is None else self.simbad
                    if (parent_id := self.simbad.sync(simbad_id)) != '':
                        continue
                current_snak = self.create_snak('P397', parent_id)

        page.decompose()
        if current_snak is not None:
            result.append(current_snak)
        return result

    def sync(self, external_id, qid=None):
        entity = self.get_items(qid)
        return self.update(self.load_snaks(external_id, 'claims' in entity and 'P397' not in entity['claims']), entity)


if sys.argv[0].endswith(basename(__file__)):  # if not imported
    wd = ExoplanetEu(sys.argv[1], sys.argv[2])
    wd_items = wd.get_all_items('SELECT ?id ?item {?item p:P5653/ps:P5653 ?id}')

    for ex_id in wd_items:
        # ex_id = 'Gaia-ASOI-031 b'
        wd.sync(ex_id, wd_items[ex_id])
        time.sleep(10)
