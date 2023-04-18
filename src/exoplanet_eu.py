#!/usr/bin/python3
import json
import re
from decimal import DecimalException
from os.path import basename
from sys import argv
from time import sleep

import requests
from bs4 import BeautifulSoup, element

from adql import ADQL
from simbad_dap import SimbadDAP
from wikidata import WikiData


class ExoplanetEu(ADQL):
    WikiData.load_config(__file__)
    db_property, db_ref = 'P5653', 'Q1385430'

    def __init__(self, external_id, qid=None):
        super().__init__(external_id, qid)
        self.properties = WikiData.config['planet']

    @staticmethod
    def get_next_chunk(offset: int) -> tuple[list[str], int]:
        identifiers, offset = [], 0 if offset is None else offset
        params = {**WikiData.config['post'], **{'iDisplayStart': offset}}
        if (result := requests.post('http://exoplanet.eu/catalog/json/', params)).status_code == 200:
            for record in json.loads(result.content)['aaData']:
                identifiers.append(re.sub('<[^<]+?>', '', record[0]))
        return identifiers, offset + params['iDisplayLength']

    articles = {"2540": "Q54012702", "4966": "Q66424531"}

    def retrieve(self):
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
            if p.get('id') not in ExoplanetEu.articles and (ref_id := ExoplanetEu.parse_publication(p)):
                ExoplanetEu.articles[p.get('id')] = ref_id
        return page

    @staticmethod
    def parse_publication(publication: element.Tag):
        for a in publication.find_all('a'):
            if ref_id := ADQL.parse_url(a.get('href')):
                return ref_id
        if publication.find('b').text and "Data Validation (DV) Report for Kepler" not in publication.find('b').text:
            if len(title := ' '.join(publication.find('b').text.replace('\n', ' ').rstrip('.').split())) > 24:
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
                            self.trace("can use source\t{}".format(ref_id), 30)
                    elif 'showAllPubs' not in str(td):
                        self.input_snaks.append(current_snak)
                        current_snak = None
                elif parsing_planet and len(td.attrs) == 0 and (td.parent.parent.get('id') == 'table_' + td.text):
                    current_snak = ExoplanetEu.create_snak('P397', td.text)

        if current_snak is not None:
            self.input_snaks.append(current_snak)

    @staticmethod
    def create_snak(property_id: str, value: str, lower=None, upper=None):
        prefix = 'http://www.wikidata.org/entity/'
        num = '\\d[-\\+.eE\\d]+'
        unit = '\\s*(?P<unit>[A-Za-z]\\S*)?'
        if property_id == 'P397':
            query = 'SELECT main_id FROM ident JOIN basic ON oid = oidref WHERE id=\'{}\''.format(value)
            if len(ident := ADQL.tap_query('https://simbad.u-strasbg.fr/simbad/sim-tap', query)) != 1:
                return
            # no_parent = self.entity and 'claims' in self.entity and 'P397' not in self.entity['claims']
            return WikiData.create_snak(property_id, SimbadDAP.get_by_id(list(ident.keys())[0], False))
        elif reg := re.search(
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
                deg[1], deg[2] = ('-' + deg[1], '-' + deg[2]) if deg[0].startswith('-') else (deg[1], deg[2])
                angle = (float(deg[2]) / 60 + float(deg[1])) / 60 + float(deg[0])
                digits = 3 + (len(value) - value.find('.') - 1 if value.find('.') > 0 else 0)
                value = WikiData.format_float(15 * angle if property_id == 'P6257' else angle, digits)
                (result := WikiData.create_snak(property_id, value))['datavalue']['value']['unit'] = prefix + 'Q28390'
            except (ValueError, DecimalException):
                return WikiData.create_snak(property_id, value)
        else:
            return WikiData.create_snak(property_id, value)

        if result and reg and reg.group('unit') and reg.group('unit') in WikiData.config['translate']:
            result['datavalue']['value']['unit'] = prefix + WikiData.config['translate'][reg.group('unit')]
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
                if snak['property'] == 'P1215':
                    for claim in self.entity['claims']['P1215']:  # Looking for visual magnitude statement
                        if 'qualifiers' in claim and 'P1227' in claim['qualifiers']:
                            if claim['qualifiers']['P1227'][0]['datavalue']['value']['id'] == 'Q4892529':
                                return  # if found - skip provided snak

        if claim := super().obtain_claim(snak):
            claim['mespos'] = 0
            if snak['property'] == 'P4501':  # always geomeric albedo
                claim['qualifiers'] = {'P1013': [WikiData.create_snak('P1013', 'Q2832068')]}
            elif snak['property'] == 'P1215':
                claim['qualifiers'] = {'P1227': [WikiData.create_snak('P1227', 'Q4892529')]}
                claim['rank'] = 'preferred'  # V-magnitude is always preferred
        return claim


if argv[0].endswith(basename(__file__)):  # if just imported - do nothing
    WikiData.logon(argv[1], argv[2])
    for ex_id, wd_item in WikiData.get_all_items('SELECT ?id ?item {?item p:P5653/ps:P5653 ?id}').items():
        # ex_id, wd_item = 'K03456.02', 'Q21067504'  # uncomment to debug specific item only
        item = ExoplanetEu(ex_id, wd_item)
        if data := item.retrieve():
            item.prepare_data(data)
            item.update()
            if item.entity and 'P397' in item.entity['claims'] and len(item.entity['claims']['P397']) == 1:
                if 'datavalue' in (parent := item.entity['claims']['P397'][0]['mainsnak']):  # parent != "novalue"
                    host = ExoplanetEu(ex_id, parent['datavalue']['value']['id'])
                    host.properties = WikiData.config['star']
                    host.prepare_data(data)
                    if ExoplanetEu.db_property not in host.entity['claims']:  # only if host is star
                        host.update()
            data.decompose()
        sleep(4)
