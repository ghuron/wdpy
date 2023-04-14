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
    config = WikiData.load_config(__file__)
    db_property, db_ref = 'P5653', 'Q1385430'

    def __init__(self, external_id, qid=None):
        super().__init__(external_id, qid)
        self.properties = ExoplanetEu.config['planet']

    @staticmethod
    def get_next_chunk(offset: int) -> tuple[list[str], int]:
        identifiers, offset = [], 0 if offset is None else offset
        params = {**ExoplanetEu.config['post'], **{'iDisplayStart': offset}}
        if (result := requests.post('http://exoplanet.eu/catalog/json/', params)).status_code == 200:
            for record in json.loads(result.content)['aaData']:
                identifiers.append(re.sub('<[^<]+?>', '', record[0]))
        return identifiers, offset + params['iDisplayLength']

    articles = config['sources']

    def retrieve(self) -> BeautifulSoup | None:
        """Load page corresponding to self.external_id and update Exoplanet.articles with parsed sources"""
        try:
            if (response := requests.get("http://exoplanet.eu/catalog/" + self.external_id)).status_code != 200:
                self.trace('{}\tresponse: {}'.format(response.url, response.status_code), 40)
                return
        except requests.exceptions.RequestException as e:
            self.trace(e.__str__(), 40)
            return

        page = BeautifulSoup(response.content, 'html.parser')
        for p in page.find_all('p', {'class': 'publication'}):
            if p.get('id') not in ExoplanetEu.articles:
                for a in p.find_all('a', {'target': '_blank'}):
                    if ref_id := ExoplanetEu.parse_url(a.get('href')):
                        ExoplanetEu.articles[p.get('id')] = ref_id
                        break
                if (p.get('id') not in ExoplanetEu.articles) and (ref_id := ExoplanetEu.find_by(p.find('b').text)):
                    ExoplanetEu.articles[p.get('id')] = ref_id

        return page

    @staticmethod
    def parse_url(url: str) -> str:
        """Try to find qid of the reference based on the url provided"""
        if url and url.strip() and (url := url.split()[0]):  # get text before first whitespace and strip
            for pattern, repl in ExoplanetEu.config['transform'].items():
                if (query := unquote(re.sub(pattern, repl, url, flags=re.S))).startswith('P'):
                    if query.startswith('P818='):
                        return ArXiv.get_by_id(query.replace('P818=', ''))
                    return WikiData.api_search('haswbstatement:' + query)  # fallback

    @staticmethod
    def find_by(title: str) -> str:
        if title and len(title := ' '.join(title.replace('\n', ' ').rstrip('.').split())) > 32:
            return WikiData.api_search('"{}" -haswbstatement:P31=Q1348305'.format(title))

    def prepare_data(self, source: BeautifulSoup = None):
        super().prepare_data()
        if not (parsing_planet := ('P1046' in self.properties.values())):
            self.input_snaks = []  # do not write P5356:exoplanet_id for the host star/planet
        current_snak = None

        if source:
            for td in source.find_all('td'):
                if td.get('id') in self.properties and td.text != '—':
                    if current_snak is not None:
                        self.input_snaks.append(current_snak)
                    current_snak = ExoplanetEu.create_snak(self.properties[td.get('id')], td.text)
                elif current_snak:
                    if 'showArticle' in str(td):
                        if (ref_id := re.sub('.+\'(\\d+)\'.+', '\\g<1>', str(td), flags=re.S)) in self.articles:
                            current_snak['source'] = [] if 'source' not in current_snak else current_snak['source']
                            current_snak['source'].append(self.articles[ref_id])
                        elif ref_id:
                            self.trace("source {} could have been used".format(ref_id), 30)
                    elif 'showAllPubs' not in str(td):
                        self.input_snaks.append(current_snak)
                        current_snak = None
                elif parsing_planet and len(td.attrs) == 0 and (td.parent.parent.get('id') == 'table_' + td.text):
                    current_snak = ExoplanetEu.create_snak('P397', td.text)

        if current_snak is not None:
            self.input_snaks.append(current_snak)

    @staticmethod
    def create_snak(property_id: str, value: str, lower=None, upper=None):
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
        elif value in ExoplanetEu.config['ids']:
            return WikiData.create_snak(property_id, 'Q' + str(ExoplanetEu.config['ids'][value]))
        else:
            return WikiData.create_snak(property_id, value)

        if result and reg and reg.group('unit'):
            result['datavalue']['value']['unit'] = 'http://www.wikidata.org/entity/Q' + str(
                ExoplanetEu.config['ids'][reg.group('unit')])
        return result

    def obtain_claim(self, snak):
        if snak is None:
            return
        if snak['property'] in ['P6257', 'P6258'] and float(snak['datavalue']['value']['amount']).is_integer():
            return  # do not put obviously wrong coordinates
        if self.entity and 'claims' in self.entity and snak['property'] in self.entity['claims']:
            if snak['property'] in ['P6257', 'P6258']:
                return  # do not update coordinates, because exoplanets.eu ra/dec is usually low precision
            if self.db_property not in self.entity['claims']:
                if snak['property'] != 'P1215':
                    return  # TODO: allow adding statements for host star with rank normalizer
                for claim in self.entity['claims']['P1215']:  # Looking for visual magnitude statement
                    if 'qualifiers' in claim and 'P1227' in claim['qualifiers']:
                        if claim['qualifiers']['P1227'][0]['datavalue']['value']['id'] == 'Q4892529':
                            return  # if found - skip provided snak

        if claim := super().obtain_claim(snak):
            if snak['property'] == 'P4501':  # always geomeric albedo
                claim['qualifiers'] = {'P1013': [WikiData.create_snak('P1013', 'Q2832068')]}
            elif snak['property'] == 'P1215':
                claim['qualifiers'] = {'P1227': [WikiData.create_snak('P1227', 'Q4892529')]}
                claim['rank'] = 'preferred'  # V-magnitude is always preferred
        return claim

    constellations = None

    def post_process(self) -> None:
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


if argv[0].endswith(basename(__file__)):  # if just imported - do nothing
    WikiData.logon(argv[1], argv[2])
    for ex_id, wd_item in WikiData.get_all_items('SELECT ?id ?item {?item p:P5653/ps:P5653 ?id}').items():
        # ex_id, wd_item = 'HD 190360 b', 'Q1072888'  # uncomment to debug specific item only
        item = ExoplanetEu(ex_id, wd_item)
        if data := item.retrieve():
            item.prepare_data(data)
            item.update()
            if item.entity and 'P397' in item.entity['claims'] and len(item.entity['claims']['P397']) == 1:
                if 'datavalue' in (parent := item.entity['claims']['P397'][0]['mainsnak']):  # parent != "novalue"
                    host = ExoplanetEu(ex_id, parent['datavalue']['value']['id'])
                    host.properties = ExoplanetEu.config['star']
                    host.prepare_data(data)
                    if ExoplanetEu.db_property not in host.entity['claims']:  # only if host is star
                        host.update()
            data.decompose()
        sleep(4)
