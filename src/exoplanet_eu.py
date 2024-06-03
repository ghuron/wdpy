#!/usr/bin/python3
from __future__ import annotations

import logging
import re
from decimal import DecimalException

import requests
from bs4 import BeautifulSoup, element

import wd


class Element(wd.AstroItem):
    __cache = None

    def apply(self, parsed_data: Model):
        super().apply(parsed_data)
        if ('en' not in self.entity['labels']) and parsed_data and parsed_data.label:
            self.entity['labels']['en'] = {'value': parsed_data.label, 'language': 'en'}

    def obtain_claim(self, snak):
        if snak:
            snak['mespos'] = 0
            if snak['property'] == 'P4501':
                snak['qualifiers'] = {'P1013': 'Q2832068'}  # always geometric albedo
            if self.entity and 'claims' in self.entity and 'P1215' in self.entity['claims']:
                if snak['property'] == 'P1215' and self.property_id not in self.entity['claims']:
                    for claim in self.entity['claims']['P1215']:  # Looking for visual magnitude statement
                        if 'qualifiers' in claim and 'P1227' in claim['qualifiers']:
                            if claim['qualifiers']['P1227'][0]['datavalue']['value']['id'] == 'Q4892529':
                                return  # if found - skip provided snak
        if claim := super().obtain_claim(snak):
            if wd.Claim(claim).find_more_precise_claim(self.entity['claims'][snak['property']]):
                if 'hash' not in claim['mainsnak']:  # Do not delete already saved claim
                    self.delete_claim(claim)
                    return
        return claim


class Model(wd.AstroModel):
    property, __session, __offset, __page, __ids = 'P5653', None, 0, None, None
    db_ref, item = 'Q1385430', Element
    articles = {'publication_2540': 'Q54012702', 'publication_4966': 'Q66424531', 'publication_3182': 'Q56032677'}

    def __init__(self, external_id: str, snaks: list = None):
        super().__init__(external_id, snaks)
        self.external_id, self.label = external_id, ''

    @classmethod
    def next(cls):
        if not Model.__session:
            Model.__session = requests.Session()
            wd.Wikidata.request('https://exoplanet.eu/catalog/', Model.__session)  # obtain csrftoken cookie
            Model.__session.headers.update({'X-Csrftoken': Model.__session.cookies.get('csrftoken'),
                                            'Referer': 'https://exoplanet.eu/catalog/'})

        identifiers = []
        params = {**{'iDisplayStart': cls.__offset}, **Model.config('post')}
        if result := wd.Wikidata.request('https://exoplanet.eu/catalog/json/', Model.__session, data=params):
            if (response := result.json()) and (cls.__offset < response['iTotalRecords']):
                for record in response['aaData']:
                    identifiers.append(re.findall('catalog/([^/]+)', record[0])[0])
        cls.__offset += len(identifiers)
        return identifiers

    @staticmethod
    def retrieve(exoplanet_id):
        """Load page corresponding to self.external_id and update Exoplanet.articles with parsed sources"""
        if response := wd.Wikidata.request(url := 'https://exoplanet.eu/catalog/' + exoplanet_id):
            Model.__page = BeautifulSoup(response.content, 'html.parser')
            for p in Model.__page.find_all('li', {'class': 'publication'}):
                try:
                    if p.get('id') not in Model.articles and (ref_id := Model.parse_publication(p)):
                        Model.articles[p.get('id')] = ref_id
                except ValueError as e:
                    logging.info('{}\tFound {} results while looking for source {} by title'.
                                 format(url, e.args[0], p.get('id')))
            return Model(response.url.removeprefix('https://exoplanet.eu/catalog/').removesuffix('/'))

    @staticmethod
    def parse_publication(publication: element.Tag):
        for a in publication.find_all('a'):
            if ref_id := wd.AstroModel.parse_url(a.get('href')):
                return ref_id
        if (raw := publication.find('h5').text) and "Data Validation (DV) Report for Kepler" not in raw:
            if len(title := ' '.join(raw.replace('\n', ' ').strip('.').split())) > 24:
                return wd.Wikidata.search('"{}" -haswbstatement:P31=Q1348305'.format(title))

    @classmethod
    def prepare_data(cls, external_id, host_star: bool = False) -> []:
        result = Model(external_id, []) if host_star else Model.retrieve(external_id)
        if cls.__page:
            template = {'decorators': {'P12132': cls.db_ref}, 'source': [cls.db_ref]}
            if host_star:
                template['decorators'][cls.property] = result.external_id
                if star := cls.__page.select_one('[id^=star-detail] dd'):
                    result.label = star.text.strip()
            else:
                result.label = cls.__page.select_one('#planet-detail-basic-info dd').text.strip()
                if star := cls.__page.select_one('[id^=star-detail] dd'):
                    result.append_multiple('P397', [star.text.strip()], template)
                elif cls.__page.select_one('[id=system-detail-basic-header]'):
                    result.input_snaks.append({'property': 'P397', 'datatype': 'wikibase-item', 'snaktype': 'novalue'})

            if result.label:
                result.append_multiple('P528', [result.label], template)

            for div_id, property_id in cls.config('star' if host_star else 'planet').items():
                if (ref := Model.__page.find(id=div_id)) and (text := ref.parent.findChild('span').text):
                    template['source'] = cls.parse_ref(ref)
                    if property_id in ['P397', 'P528']:
                        result.append_multiple(property_id, text.split(','), template)
                    else:
                        result.append_multiple(property_id, [text], template)
                        if property_id in ['P6257', 'P6258']:  # add J2000 epoch
                            result.input_snaks.append(result.transform('P6259', 'Q1264450'))
            return result

    @classmethod
    def parse_ref(cls, ref_div) -> list[str]:
        for a in ref_div.find_all('a'):
            if (anchor := a.get('href').strip('#')) in Model.articles:
                return [Model.articles[anchor]]
        return [cls.db_ref]

    def append_multiple(self, property_id: str, values: list[str], template: dict):
        for value in values:
            if snak := self.transform(property_id, value.strip()):
                self.input_snaks.append({**snak, **template})

    @classmethod
    def transform(cls, property_id: str, value, **kwargs):
        if not value or (value := value.strip()) == '—':
            return

        if wd.Wikidata.type_of(property_id) == 'quantity':
            num = '\\d[-\\+.eE\\d]+'
            unit = '\\s*(?P<unit>[A-Za-z]\\S*)?'
            if reg := re.search(
                    '(?P<value>{})\\s*\\(\\s*(?P<max>\\+{})\\s+-+(?P<min>{})\\s*\\){}'.format(num, num, num, unit),
                    value):
                result = super().transform(property_id, reg.group('value'), reg.group('min'), reg.group('max'))
            elif reg := re.search(  # ToDo: Add source circumstance qualifier if > or < found
                    '^[<>]?\\s*(?P<value>' + num + ')\\s*(\\(\\s*±\\s*(?P<bound>' + num + ')\\s*\\))?' + unit + '$',
                    value):
                if bound := reg.group('bound'):
                    result = super().transform(property_id, reg.group('value'), bound, bound)
                else:
                    result = super().transform(property_id, reg.group('value'))
            elif len(deg := value.split(':')) == 3:  # coordinates
                try:
                    deg[1], deg[2] = ('-' + deg[1], '-' + deg[2]) if deg[0].startswith('-') else (deg[1], deg[2])
                    angle = (float(deg[2]) / 60 + float(deg[1])) / 60 + float(deg[0])
                    digits = 3 + (len(value) - value.find('.') - 1 if value.find('.') > 0 else 0)
                    value = wd.Wikidata.format_float(15 * angle if property_id == 'P6257' else angle, digits)
                    if result := super().transform(property_id, value):
                        result['datavalue']['value']['unit'] = 'http://www.wikidata.org/entity/Q28390'
                except (ValueError, DecimalException):
                    return super().transform(property_id, value)
            else:
                return super().transform(property_id, value)

            if result and reg and (unit := cls.lut(reg.group('unit'))):
                result['datavalue']['value']['unit'] = 'http://www.wikidata.org/entity/' + unit
            return cls.enrich_qualifier(result, value)

        value = value[:-2] + value[-1] if (property_id == 'P528') and (value[-2] == ' ') else value
        return cls.enrich_qualifier(super().transform(property_id, value), value)

    def get_qid(self):
        if not Model.__ids:
            Model.__ids = wd.Wikidata.query('SELECT ?c ?i {?i wdt:P5667 ?c MINUS {?i wdt:P5653 []}}')
        return Model.__ids[self.label] if self.label in Model.__ids else None


if Model.initialize(__file__):  # if just imported - do nothing
    def process(external_id):
        (item := Model.get_by_id(external_id, forced=True)).save()
        if 'P397' in item.entity['claims'] and len(item.entity['claims']['P397']) == 1:
            if 'datavalue' in (parent := item.entity['claims']['P397'][0]['mainsnak']):  # parent != "novalue"
                if item.set_qid(parent['datavalue']['value']['id']) not in updated_hosts:
                    if Model.property not in item.entity['claims']:  # If initial item was not exo-moon
                        item.apply(Model.prepare_data(external_id, host_star=True))
                        item.save()
                        updated_hosts.append(item.qid)


    updated_hosts = []
    # process('51_peg_b--12')  # uncomment to debug specific item only
    logging.info('Start updating {} existing items'.format(len(Model.item.get_cache())))
    for ex_id in sorted(Model.item.get_cache().keys()):
        process(ex_id)
    logging.info('Finish updating existing items')
    while chunk := Model.next():
        for ex_id in sorted(chunk):
            if ex_id not in Model.item.get_cache():
                process(ex_id)
