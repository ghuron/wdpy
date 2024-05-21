#!/usr/bin/python3
from pathlib import Path
from urllib.parse import quote_plus

import requests

import wd


class Model(wd.TAPClient):
    property, db_ref, __offset, __ads = 'P819', 'Q752099', 0, requests.session()
    URL = 'https://api.adsabs.harvard.edu/v1/search/query?q={}&fl={}'
    __ads.headers.update({'Authorization': 'Bearer ' + (
        __p.read_text().strip() if (__p := Path(__file__.replace('ads.py', '.ads'))).exists() else '')})

    @classmethod
    def next(cls):
        cls._dataset = cls.load('oidbib BETWEEN {} AND {}'.format(cls.__offset, cls.__offset + 5000))
        cls.__offset = cls.__offset + 5000
        return cls._dataset.keys()

    @classmethod
    def prepare_data(cls, external_id):
        if ((response := wd.Wikidata.request(Model.URL.format(quote_plus(external_id),
                                                              quote_plus(','.join(Model.config('properties')))),
                                             Model.__ads)) is None) or (len(response.json()['response']['docs']) == 0):
            return

        result = Model(external_id)
        result.input_snaks.append(Model.create_snak('P31', 'Q13442814'))

        for idx in range(0, len((data := response.json()['response']['docs'][0])['author'])):
            if author_id := Element.haswbstatement(data['orcid_pub'][idx], 'P496'):
                (snak := Model.create_snak('P50', author_id))['qualifiers'] = {'P1932': data['author'][idx]}
            else:
                (snak := Model.create_snak('P2093', data['author'][idx]))['qualifiers'] = {}
            snak['qualifiers']['P1545'] = str(idx + 1)
            result.input_snaks.append(snak)

        for ident in data['identifier']:
            if ident.startswith('arXiv:'):
                result.input_snaks.append(Model.create_snak('P818', ident.replace('arXiv:', '')))
            elif ident.startswith('10.') and not ('ARXIV' in ident.upper()):
                result.input_snaks.append(Model.create_snak('P356', ident.upper()))

        for field in Model.config('properties'):
            if (p := Model.config('properties')[field]) and (field in data):
                if s := Model.create_snak(p, data[field][0] if isinstance(data[field], list) else data[field]):
                    result.input_snaks.append(s)
                    if p == 'P304' and 'page_count' in data:
                        try:
                            s['datavalue']['value'] += '-' + str(int(s['datavalue']['value']) + data['page_count'] - 1)
                        except ValueError:
                            pass
                    elif p == 'P1476':
                        result.label = s['datavalue']['value']['text']

        return result

    def __init__(self, external_id: str, snaks: list = None):
        super().__init__(external_id, snaks)
        self.label = ''


class Element(wd.Article):
    _model, __cache = Model, None

    @classmethod
    def get_cache(cls, reset=None) -> dict:
        if (reset is None) and (Element.__cache is None):
            query, o, Element.__cache = 'SELECT ?c ?i {{ ?i p:P819/ps:P819 ?c }} LIMIT 400000 OFFSET {}', -1, {}
            while (o < len(Element.__cache)) and (r := wd.Wikidata.query(query.format(o := len(Element.__cache)))):
                Element.__cache = Element.__cache | r
        return super().get_cache(reset)

    def apply(self, parsed_data: Model):
        super().apply(parsed_data)
        if ('en' not in self.entity['labels']) and parsed_data and parsed_data.label:
            self.entity['labels']['en'] = {'value': parsed_data.label, 'language': 'en'}


if Model.initialize(__file__):  # if not imported
    # Element.get_by_id('2018MNRAS.474.3557B', forced=True).save()
    NO_DOI = 'SELECT ?c ?i {{VALUES ?c {{\'{}\'}} ?i p:P819/ps:P819 ?c FILTER NOT EXISTS {{?i p:P356 []}}}}'
    NO_ADS = 'SELECT ?c ?i {{VALUES ?c {{\'{}\'}} ?i p:P356/ps:P356 ?c FILTER NOT EXISTS {{?i p:P819 []}}}}'
    while bibcodes := Model.next():
        if wd_items := wd.Wikidata.query(NO_DOI.format('\' \''.join(bibcodes))):
            for ex_id, qid in wd_items.items():
                (item := Element(ex_id, qid)).apply(Model.prepare_data(ex_id))
                item.save()

        doi = {}
        for ex_id in bibcodes:
            doi[Model._dataset[ex_id][0]['p356'].upper()] = ex_id
        if wd_items := wd.Wikidata.query(NO_ADS.format('\' \''.join(doi.keys()))):
            for ex_id, qid in wd_items.items():
                (item := Element(doi[ex_id], qid)).apply(Model.prepare_data(doi[ex_id]))
                item.save()
