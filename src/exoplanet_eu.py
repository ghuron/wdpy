#!/usr/bin/python3
import logging
import re
from collections import OrderedDict
from decimal import DecimalException
from time import sleep

import requests
from bs4 import BeautifulSoup, element

from adql import ADQL
from simbad_dap import SimbadDAP
from wd import Wikidata, Model


class ExoplanetEu(ADQL):
    db_property, db_ref, properties = 'P5653', 'Q1385430', None

    def trace(self, message: str, level=20):
        super().trace('http://exoplanet.eu/catalog/{}\t{}'.format(self.external_id.replace(' ', '_'), message), level)

    __session = None

    @staticmethod
    def get_next_chunk(offset: int) -> tuple[list[str], int]:
        if not ExoplanetEu.__session:
            ExoplanetEu.__session = requests.Session()
            Wikidata.request('https://exoplanet.eu/catalog/', ExoplanetEu.__session)  # obtain csrftoken cookie
            ExoplanetEu.__session.headers.update({'X-Csrftoken': ExoplanetEu.__session.cookies.get('csrftoken'),
                                                  'Referer': 'https://exoplanet.eu/catalog/'})

        identifiers, offset = [], 0 if offset is None else offset
        params = {**{'iDisplayStart': offset}, **ExoplanetEu.config['post']}
        if result := Wikidata.request('https://exoplanet.eu/catalog/json/', ExoplanetEu.__session, data=params):
            if (response := result.json()) and (offset < response['iTotalRecords']):
                for record in response['aaData']:
                    identifiers.append(re.findall('catalog/([^/]+)', record[0])[0])
        return identifiers, offset + len(identifiers)

    articles = {'publication_2540': 'Q54012702', 'publication_4966': 'Q66424531', 'publication_3182': 'Q56032677'}

    @staticmethod
    def retrieve(exoplanet_id):
        """Load page corresponding to self.external_id and update Exoplanet.articles with parsed sources"""
        if response := Wikidata.request(url := "https://exoplanet.eu/catalog/" + exoplanet_id):
            ExoplanetEu.page = BeautifulSoup(response.content, 'html.parser')
            for p in ExoplanetEu.page.find_all('li', {'class': 'publication'}):
                try:
                    if p.get('id') not in ExoplanetEu.articles and (ref_id := ExoplanetEu.parse_publication(p)):
                        ExoplanetEu.articles[p.get('id')] = ref_id
                except ValueError as e:
                    logging.info('{}\tFound {} results while looking for source {} by title'.
                                 format(url, e.args[0], p.get('id')))
            return ExoplanetEu.page

    @staticmethod
    def parse_publication(publication: element.Tag):
        for a in publication.find_all('a'):
            if ref_id := ExoplanetEu.parse_url(a.get('href')):
                return ref_id
        if (raw := publication.find('h5').text) and "Data Validation (DV) Report for Kepler" not in raw:
            if len(title := ' '.join(raw.replace('\n', ' ').strip('.').split())) > 24:
                return Wikidata.search('"{}" -haswbstatement:P31=Q1348305'.format(title))

    page = None

    @classmethod
    def prepare_data(cls, external_id) -> []:
        ExoplanetEu.page = ExoplanetEu.page if ExoplanetEu.page else item.retrieve(external_id)
        if ExoplanetEu.page:
            result = {'input': []}
            if 'P215' in ExoplanetEu.properties.values():  # parsing hosting star, not exoplanet
                if star := ExoplanetEu.page.select_one('[id^=star-detail] dd'):
                    result['label'] = star.text.strip()
            else:
                result['input'] = [ExoplanetEu.create_snak(ExoplanetEu.db_property, external_id)]
                result['label'] = ExoplanetEu.page.select_one('#planet-detail-basic-info dd').text.strip()
                if star := ExoplanetEu.page.select_one('[id^=star-detail] dd'):
                    if snak := ExoplanetEu.parse_value('P397', star.text.strip()):
                        result['input'].append(snak)

            if 'label' in result and (snak := ExoplanetEu.parse_value('P528', result['label'])):
                result['input'].append(snak)

            for div_id in ExoplanetEu.properties:
                if (ref := ExoplanetEu.page.find(id=div_id)) and (text := ref.parent.findChild('span').text):
                    if (property_id := ExoplanetEu.properties[div_id]) in ['P397', 'P528']:
                        result['input'] = result['input'] + ExoplanetEu.parse_snaks(property_id, text.split(','), ref)
                    else:
                        result['input'] = result['input'] + ExoplanetEu.parse_snaks(property_id, [text], ref)
            return result

    @staticmethod
    def parse_snaks(property_id: str, values: [], ref_div: any) -> []:
        result = []
        for value in values:
            if snak := ExoplanetEu.parse_value(property_id, value.strip()):
                for a in ref_div.find_all('a'):
                    if (anchor := a.get('href').strip('#')) in ExoplanetEu.articles:
                        snak['source'] = (snak['source'] if 'source' in snak else []) + [ExoplanetEu.articles[anchor]]
                result.append(snak)
        return result

    __p5667 = None

    def update(self, parsed_data):
        if parsed_data and 'label' in parsed_data:
            if not self.qid:  # Try to reuse item from NASA Exoplanet Archive
                if not ExoplanetEu.__p5667:
                    ExoplanetEu.__p5667 = Wikidata.query('SELECT ?c ?i {?i wdt:P5667 ?c MINUS {?i wdt:P5653 []}}')
                if parsed_data['label'] in ExoplanetEu.__p5667:
                    self.qid = ExoplanetEu.__p5667[parsed_data['label']]
            if 'en' not in self.entity['labels']:
                self.entity['labels']['en'] = {'value': parsed_data['label'], 'language': 'en'}
        if parsed_data and 'input' in parsed_data:
            return super().update(parsed_data['input'])

    @staticmethod
    def parse_value(property_id: str, value: str):
        prefix = 'http://www.wikidata.org/entity/'
        num = '\\d[-\\+.eE\\d]+'
        unit = '\\s*(?P<unit>[A-Za-z]\\S*)?'
        if not value or (value := value.strip()) == '—':
            return
        elif property_id == 'P397':
            return ExoplanetEu.create_snak(property_id, SimbadDAP.get_by_any_id(value))
        elif reg := re.search(
                '(?P<value>' + num + ')\\s*\\(\\s*-+(?P<min>' + num + ')\\s+(?P<max>\\+' + num + ')\\s*\\)' + unit,
                value):
            result = ExoplanetEu.create_snak(property_id, reg.group('value'), reg.group('min'), reg.group('max'))
        elif reg := re.search(
                '^(?P<value>' + num + ')\\s*(\\(\\s*±\\s*(?P<bound>' + num + ')\\s*\\))?' + unit + '$', value):
            if bound := reg.group('bound'):
                result = ExoplanetEu.create_snak(property_id, reg.group('value'), bound, bound)
            else:
                result = ExoplanetEu.create_snak(property_id, reg.group('value'))
        elif len(deg := value.split(':')) == 3:  # coordinates
            try:
                deg[1], deg[2] = ('-' + deg[1], '-' + deg[2]) if deg[0].startswith('-') else (deg[1], deg[2])
                angle = (float(deg[2]) / 60 + float(deg[1])) / 60 + float(deg[0])
                digits = 3 + (len(value) - value.find('.') - 1 if value.find('.') > 0 else 0)
                value = Model.format_float(15 * angle if property_id == 'P6257' else angle, digits)
                if result := ExoplanetEu.create_snak(property_id, value):
                    result['datavalue']['value']['unit'] = prefix + 'Q28390'
            except (ValueError, DecimalException):
                return ExoplanetEu.create_snak(property_id, value)
        else:
            value = value[:-2] + value[-1] if property_id == 'P528' and value[-2] == ' ' else value
            result = ExoplanetEu.create_snak(property_id, value)

        if result and reg and (unit := reg.group('unit')) and unit in ExoplanetEu.config['translate']:
            result['datavalue']['value']['unit'] = prefix + ExoplanetEu.config['translate'][unit]
        return ExoplanetEu.enrich_qualifier(result, value)

    def obtain_claim(self, snak):
        if snak:
            snak['mespos'] = 0
            if snak['property'] == 'P4501':
                snak['qualifiers'] = {'P1013': 'Q2832068'}  # always geometric albedo
            if self.entity and 'claims' in self.entity and 'P1215' in self.entity['claims']:
                if snak['property'] == 'P1215' and self.db_property not in self.entity['claims']:
                    for claim in self.entity['claims']['P1215']:  # Looking for visual magnitude statement
                        if 'qualifiers' in claim and 'P1227' in claim['qualifiers']:
                            if claim['qualifiers']['P1227'][0]['datavalue']['value']['id'] == 'Q4892529':
                                return  # if found - skip provided snak
        return super().obtain_claim(snak)

    @staticmethod
    def compare_refs(claim: dict, references: set):
        target = references.copy().union({ExoplanetEu.db_ref})
        try:
            for ref in claim['references']:
                target.remove(ref['snaks']['P248'][0]['datavalue']['value']['id'])
        except KeyError:
            return False
        return len(target) == 0

    def add_refs(self, claim: dict, references: set):
        if references:
            try:
                for candidate in self.entity['claims'][claim['mainsnak']['property']]:
                    if candidate['id'] != claim['id'] and ExoplanetEu.compare_refs(candidate, references):
                        self.trace('{} replace statement'.format(claim['mainsnak']['property']), 30)
                        candidate[
                            'remove'] = 1  # A different claim had exactly the same set of references -> replace it
            except KeyError:
                self.trace('No id')  # ToDo how does this happens?
        super().add_refs(claim, references)


if ExoplanetEu.initialize(__file__):  # if just imported - do nothing
    updated_hosts = []
    wd_items = OrderedDict(sorted(ExoplanetEu.get_all_items('SELECT ?id ?item {?item p:P5653/ps:P5653 ?id}').items()))
    SimbadDAP.cache = Wikidata.query('SELECT DISTINCT ?c ?i { ?i ^ps:P397 []; wdt:P528 ?c }',
                                     lambda row, _: (row[0].lower(), row[1]))
    for ex_id in wd_items:
        # ex_id = '2mass_j0249_0557_ab_c--6790'  # uncomment to debug specific item only
        ExoplanetEu.properties = ExoplanetEu.config['planet']
        (item := ExoplanetEu(ex_id, wd_items[ex_id])).update(ExoplanetEu.prepare_data(ex_id))
        if item.entity and 'P397' in item.entity['claims'] and len(item.entity['claims']['P397']) == 1:
            if 'datavalue' in (parent := item.entity['claims']['P397'][0]['mainsnak']):  # parent != "novalue"
                if (host := ExoplanetEu(ex_id, parent['datavalue']['value']['id'])).qid not in updated_hosts:
                    if ExoplanetEu.db_property not in host.entity['claims']:  # If initial item was not exo-moon
                        ExoplanetEu.properties = ExoplanetEu.config['star']
                        host.update(host.prepare_data(ex_id))
                        updated_hosts.append(host.qid)
        if ExoplanetEu.page:
            ExoplanetEu.page.decompose()
            ExoplanetEu.page = None
        sleep(3)
