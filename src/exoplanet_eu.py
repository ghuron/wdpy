#!/usr/bin/python3
import json
import re
from collections import OrderedDict
from decimal import DecimalException
from os.path import basename
from sys import argv
from time import sleep

import requests
from bs4 import BeautifulSoup, element

from adql import ADQL
from simbad_dap import SimbadDAP


class ExoplanetEu(ADQL):
    config = ADQL.load_config(__file__)
    db_property, db_ref = 'P5653', 'Q1385430'

    def __init__(self, external_id, qid=None):
        super().__init__(external_id, qid)
        self.properties = ExoplanetEu.config['planet']

    def trace(self, message: str, level=20):
        super().trace('http://exoplanet.eu/catalog/{}\t{}'.format(self.external_id.replace(' ', '_'), message), level)

    @staticmethod
    def get_next_chunk(offset: int) -> tuple[list[str], int]:
        identifiers, offset = [], 0 if offset is None else offset
        params = {**ExoplanetEu.config['post'], **{'iDisplayStart': offset}}
        if (result := requests.post('http://exoplanet.eu/catalog/json/', params)).status_code == 200:
            for record in json.loads(result.content)['aaData']:
                identifiers.append(re.sub('<[^<]+?>', '', record[0]))
        return identifiers, offset + params['iDisplayLength']

    articles = {'2540': 'Q54012702', '4966': 'Q66424531', '3182': 'Q56032677'}

    def retrieve(self):
        """Load page corresponding to self.external_id and update Exoplanet.articles with parsed sources"""
        try:
            if (response := requests.get("http://exoplanet.eu/catalog/" + self.external_id)).status_code != 200:
                self.trace('http response: {}'.format(response.status_code), 40)
                return
        except requests.exceptions.RequestException as e:
            self.trace(e.__str__(), 40)
            return

        page = BeautifulSoup(response.content, 'html.parser')
        for p in page.find_all('p', {'class': 'publication'}):
            try:
                if p.get('id') not in ExoplanetEu.articles and (ref_id := ExoplanetEu.parse_publication(p)):
                    ExoplanetEu.articles[p.get('id')] = ref_id
            except ValueError as e:
                self.trace('Found {} results while looking for source {} by title'.format(e.args[0], p.get('id')))
        return page

    @staticmethod
    def parse_publication(publication: element.Tag):
        for a in publication.find_all('a'):
            if ref_id := ADQL.parse_url(a.get('href')):
                return ref_id
        if publication.find('b').text and "Data Validation (DV) Report for Kepler" not in publication.find('b').text:
            if len(title := ' '.join(publication.find('b').text.replace('\n', ' ').rstrip('.').split())) > 24:
                return ADQL.api_search('"{}" -haswbstatement:P31=Q1348305'.format(title))

    def prepare_data(self, source: BeautifulSoup = None):
        if source:
            super().prepare_data(source)
            self.input_snaks, current_snak = [], None

            for td in source.find_all('td'):
                if td.get('id') in self.properties and td.text != '—':
                    self.input_snaks += [current_snak] if current_snak else []
                    current_snak = self.parse_value(self.properties[td.get('id')], td.text)
                elif current_snak:
                    if 'showArticle' in str(td):
                        if (ref_id := re.sub('.+\'(\\d+)\'.+', '\\g<1>', str(td), flags=re.S)) in self.articles:
                            current_snak['source'] = [] if 'source' not in current_snak else current_snak['source']
                            current_snak['source'].append(self.articles[ref_id])
                        elif ref_id:
                            self.trace("{} source missing".format(ref_id))
                    elif 'showAllPubs' not in str(td):
                        self.input_snaks += [current_snak] if current_snak else []
                        current_snak = None
                elif (row := td.parent.text.strip()).startswith('Name') and row.endswith(td.text) and td.text:
                    if 'P1046' in self.properties.values():  # parsing exoplanet, not hosting star
                        if td.parent.parent.get('id') == 'table_' + td.text:
                            current_snak = self.parse_value('P397', td.text)
                        else:
                            current_snak = self.create_snak(self.db_property, td.text)

            self.input_snaks += [current_snak] if current_snak else []

    @staticmethod
    def parse_value(property_id: str, value: str):
        prefix = 'http://www.wikidata.org/entity/'
        num = '\\d[-\\+.eE\\d]+'
        unit = '\\s*(?P<unit>[A-Za-z]\\S*)?'
        if property_id == 'P397':
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
                value = ADQL.format_float(15 * angle if property_id == 'P6257' else angle, digits)
                if result := ExoplanetEu.create_snak(property_id, value):
                    result['datavalue']['value']['unit'] = prefix + 'Q28390'
            except (ValueError, DecimalException):
                return ExoplanetEu.create_snak(property_id, value)
        else:
            return ExoplanetEu.create_snak(property_id, value.strip())

        if result and reg and (unit := reg.group('unit')) and unit in ExoplanetEu.config['translate']:
            result['datavalue']['value']['unit'] = prefix + ExoplanetEu.config['translate'][unit]
        return result

    def obtain_claim(self, snak):
        if snak and self.entity and 'claims' in self.entity and snak['property'] in self.entity['claims']:
            if self.db_property not in self.entity['claims'] and snak['property'] == 'P1215':
                for claim in self.entity['claims']['P1215']:  # Looking for visual magnitude statement
                    if 'qualifiers' in claim and 'P1227' in claim['qualifiers']:
                        if claim['qualifiers']['P1227'][0]['datavalue']['value']['id'] == 'Q4892529':
                            return  # if found - skip provided snak

        if claim := super().obtain_claim(snak):
            claim['mespos'] = 0
            if snak['property'] == 'P4501':  # always geometric albedo
                claim['qualifiers'] = {'P1013': [ExoplanetEu.create_snak('P1013', 'Q2832068')]}
            elif snak['property'] == 'P1215':
                claim['qualifiers'] = {'P1227': [ExoplanetEu.create_snak('P1227', 'Q4892529')]}
                claim['rank'] = 'preferred'  # V-magnitude is always preferred
        return claim

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
        try:
            for candidate in self.entity['claims'][claim['mainsnak']['property']]:
                if candidate['id'] != claim['id'] and ExoplanetEu.compare_refs(candidate, references):
                    self.trace('{} replace statement'.format(claim['mainsnak']['property']), 30)
                    candidate['remove'] = 1  # A different claim had exactly the same set of references -> replace it
        except KeyError:
            self.trace('No id')  # ToDo how does this happens?
        super().add_refs(claim, references)


if argv[0].endswith(basename(__file__)):  # if just imported - do nothing
    ADQL.logon(argv[1], argv[2])
    updated_hosts = []
    wd_items = OrderedDict(sorted(ExoplanetEu.get_all_items('SELECT ?id ?item {?item p:P5653/ps:P5653 ?id}').items()))
    for ex_id in wd_items:
        # ex_id = 'K03456.02'  # uncomment to debug specific item only
        item = ExoplanetEu(ex_id, wd_items[ex_id])
        if data := item.retrieve():
            item.prepare_data(data)
            item.update()
            if item.entity and 'P397' in item.entity['claims'] and len(item.entity['claims']['P397']) == 1:
                if 'datavalue' in (parent := item.entity['claims']['P397'][0]['mainsnak']):  # parent != "novalue"
                    host = ExoplanetEu(ex_id, parent['datavalue']['value']['id'])
                    host.properties = ExoplanetEu.config['star']
                    host.prepare_data(data)
                    if ExoplanetEu.db_property not in host.entity['claims'] and host.qid not in updated_hosts:
                        host.update()
                        updated_hosts.append(host.qid)
            data.decompose()
        sleep(4)
