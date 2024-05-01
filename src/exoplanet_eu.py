#!/usr/bin/python3
import logging
import re
from collections import OrderedDict
from decimal import DecimalException
from time import sleep

import requests
from bs4 import BeautifulSoup, element

import adql
import wd


class Model(adql.Model):
    property, __session, properties, external_id = 'P5653', None, None, None
    articles = {'publication_2540': 'Q54012702', 'publication_4966': 'Q66424531', 'publication_3182': 'Q56032677'}

    @staticmethod
    def get_next_chunk(offset: int) -> tuple[list[str], int]:
        if not Model.__session:
            Model.__session = requests.Session()
            wd.Wikidata.request('https://exoplanet.eu/catalog/', Model.__session)  # obtain csrftoken cookie
            Model.__session.headers.update({'X-Csrftoken': Model.__session.cookies.get('csrftoken'),
                                            'Referer': 'https://exoplanet.eu/catalog/'})

        identifiers, offset = [], 0 if offset is None else offset
        params = {**{'iDisplayStart': offset}, **Model.config('post')}
        if result := wd.Wikidata.request('https://exoplanet.eu/catalog/json/', Model.__session, data=params):
            if (response := result.json()) and (offset < response['iTotalRecords']):
                for record in response['aaData']:
                    identifiers.append(re.findall('catalog/([^/]+)', record[0])[0])
        return identifiers, offset + len(identifiers)

    @staticmethod
    def retrieve(exoplanet_id):
        """Load page corresponding to self.external_id and update Exoplanet.articles with parsed sources"""
        if response := wd.Wikidata.request(url := "https://exoplanet.eu/catalog/" + exoplanet_id):
            Model.page = BeautifulSoup(response.content, 'html.parser')
            for p in Model.page.find_all('li', {'class': 'publication'}):
                try:
                    if p.get('id') not in Model.articles and (ref_id := Model.parse_publication(p)):
                        Model.articles[p.get('id')] = ref_id
                except ValueError as e:
                    logging.info('{}\tFound {} results while looking for source {} by title'.
                                 format(url, e.args[0], p.get('id')))
            return Model.page

    @staticmethod
    def parse_publication(publication: element.Tag):
        for a in publication.find_all('a'):
            if ref_id := Model.parse_url(a.get('href')):
                return ref_id
        if (raw := publication.find('h5').text) and "Data Validation (DV) Report for Kepler" not in raw:
            if len(title := ' '.join(raw.replace('\n', ' ').strip('.').split())) > 24:
                return wd.Wikidata.search('"{}" -haswbstatement:P31=Q1348305'.format(title))

    page = None

    @classmethod
    def prepare_data(cls, external_id) -> []:
        cls.external_id = external_id
        Model.page = Model.page if Model.page else Model.retrieve(external_id)
        if Model.page:
            result = {'input': []}
            if 'P215' in Model.properties.values():  # parsing hosting star, not exoplanet
                if star := Model.page.select_one('[id^=star-detail] dd'):
                    result['label'] = star.text.strip()
            else:
                result['input'] = [Model.create_snak(Model.property, external_id)]
                result['label'] = Model.page.select_one('#planet-detail-basic-info dd').text.strip()
                if star := Model.page.select_one('[id^=star-detail] dd'):
                    result['input'] = result['input'] + Model.parse_snaks('P397', [star.text.strip()])
                elif Model.page.select_one('[id=system-detail-basic-header]'):
                    result['input'].append({'property': 'P397', 'datatype': 'wikibase-item', 'snaktype': 'novalue'})

            if 'label' in result:
                result['input'] = result['input'] + Model.parse_snaks('P528', [result['label']])

            for div_id in Model.properties:
                if (ref := Model.page.find(id=div_id)) and (text := ref.parent.findChild('span').text):
                    if (property_id := Model.properties[div_id]) in ['P397', 'P528']:
                        result['input'] = result['input'] + Model.parse_snaks(property_id, text.split(','), ref)
                    else:
                        result['input'] = result['input'] + Model.parse_snaks(property_id, [text], ref)
                        if property_id in ['P6257', 'P6258']:  # add J2000 epoch
                            result['input'].append(Model.create_snak('P6259', 'Q1264450'))
            return result

    @classmethod
    def parse_snaks(cls, property_id: str, values: [], ref_div=None) -> []:
        result = []
        for value in values:
            if snak := Model.parse_value(property_id, value.strip()):
                if 'P215' in Model.properties.values():  # it is the host star
                    snak['decorators'] = snak['decorators'] if 'decorators' in snak else {}
                    snak['decorators'][Model.property] = cls.external_id  # Ugly hack
                for a in (ref_div.find_all('a') if ref_div else []):
                    if (anchor := a.get('href').strip('#')) in Model.articles:
                        snak['source'] = (snak['source'] if 'source' in snak else []) + [Model.articles[anchor]]
                        break  # 2nd and subsequent sources are usually specified incorrectly
                result.append(snak)
        return result

    @staticmethod
    def parse_value(property_id: str, value: str):
        if not value or (value := value.strip()) == '—':
            return

        if wd.Wikidata.type_of(property_id) == 'quantity':
            num = '\\d[-\\+.eE\\d]+'
            unit = '\\s*(?P<unit>[A-Za-z]\\S*)?'
            if reg := re.search(
                    '(?P<value>{})\\s*\\(\\s*-+(?P<min>{})\\s+(?P<max>\\+{})\\s*\\){}'.format(num, num, num, unit),
                    value):
                result = Model.create_snak(property_id, reg.group('value'), reg.group('min'), reg.group('max'))
            elif reg := re.search(  # ToDo: Add source circumstance qualifier if > or < found
                    '^[<>]?\\s*(?P<value>' + num + ')\\s*(\\(\\s*±\\s*(?P<bound>' + num + ')\\s*\\))?' + unit + '$',
                    value):
                if bound := reg.group('bound'):
                    result = Model.create_snak(property_id, reg.group('value'), bound, bound)
                else:
                    result = Model.create_snak(property_id, reg.group('value'))
            elif len(deg := value.split(':')) == 3:  # coordinates
                try:
                    deg[1], deg[2] = ('-' + deg[1], '-' + deg[2]) if deg[0].startswith('-') else (deg[1], deg[2])
                    angle = (float(deg[2]) / 60 + float(deg[1])) / 60 + float(deg[0])
                    digits = 3 + (len(value) - value.find('.') - 1 if value.find('.') > 0 else 0)
                    value = Model.format_float(15 * angle if property_id == 'P6257' else angle, digits)
                    if result := Model.create_snak(property_id, value):
                        result['datavalue']['value']['unit'] = 'http://www.wikidata.org/entity/Q28390'
                except (ValueError, DecimalException):
                    return Model.create_snak(property_id, value)
            else:
                return Model.create_snak(property_id, value)

            if result and reg and (unit := Model.lut(reg.group('unit'))):
                result['datavalue']['value']['unit'] = 'http://www.wikidata.org/entity/' + unit
            return result

        if property_id == 'P397':
            return Model.get_parent_snak(value)
        value = value[:-2] + value[-1] if (property_id == 'P528') and (value[-2] == ' ') else value
        return Model.enrich_qualifier(Model.create_snak(property_id, value), value)


class Element(adql.Element):
    __p5667, __cache, _model, _claim = None, {}, Model, type('Claim', (wd.Claim,), {'db_ref': 'Q1385430'})

    def trace(self, message: str, level=20):
        super().trace('http://exoplanet.eu/catalog/{}\t{}'.format(self.external_id.replace(' ', '_'), message), level)

    def update(self, parsed_data):
        if parsed_data and 'label' in parsed_data:
            if not self.qid:  # Try to reuse item from NASA Exoplanet Archive
                if not Element.__p5667:
                    Element.__p5667 = wd.Wikidata.query('SELECT ?c ?i {?i wdt:P5667 ?c MINUS {?i wdt:P5653 []}}')
                if parsed_data['label'] in Element.__p5667:
                    self.qid = Element.__p5667[parsed_data['label']]
            if 'en' not in self.entity['labels']:
                self.entity['labels']['en'] = {'value': parsed_data['label'], 'language': 'en'}
        if parsed_data and 'input' in parsed_data:
            return super().update(parsed_data['input'])

    def obtain_claim(self, snak):
        if snak:
            snak['mespos'] = 0
            if snak['property'] == 'P4501':
                snak['qualifiers'] = {'P1013': 'Q2832068'}  # always geometric albedo
            if self.entity and 'claims' in self.entity and 'P1215' in self.entity['claims']:
                if snak['property'] == 'P1215' and self._model.property not in self.entity['claims']:
                    for claim in self.entity['claims']['P1215']:  # Looking for visual magnitude statement
                        if 'qualifiers' in claim and 'P1227' in claim['qualifiers']:
                            if claim['qualifiers']['P1227'][0]['datavalue']['value']['id'] == 'Q4892529':
                                return  # if found - skip provided snak
        if claim := super().obtain_claim(snak):
            if self._claim(claim).find_more_precise_claim(self.entity['claims'][snak['property']]):
                if 'hash' not in claim['mainsnak']:  # Do not delete already saved claim
                    self.delete_claim(claim)
                    return
        return claim


if Model.initialize(__file__):  # if just imported - do nothing
    updated_hosts = []
    wd_items = Model.get_all_items('SELECT ?id ?item {?item p:P5653/ps:P5653 ?id}')
    for ex_id in OrderedDict(sorted(wd_items.items())):
        # ex_id = '2mass_j0249_0557_ab_c--6790'  # uncomment to debug specific item only
        Model.properties = Model.config('planet')
        (item := Element(ex_id, wd_items[ex_id])).update(Model.prepare_data(ex_id))
        if item.entity and 'P397' in item.entity['claims'] and len(item.entity['claims']['P397']) == 1:
            if 'datavalue' in (parent := item.entity['claims']['P397'][0]['mainsnak']):  # parent != "novalue"
                if (host := Element(ex_id, parent['datavalue']['value']['id'])).qid not in updated_hosts:
                    if Model.property not in host.entity['claims']:  # If initial item was not exo-moon
                        Model.properties = Model.config('star')
                        host.update(Model.prepare_data(ex_id))
                        updated_hosts.append(host.qid)
        if Model.page:
            Model.page.decompose()
            Model.page = None
        sleep(2)
