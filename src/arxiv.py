#!/usr/bin/python3
from __future__ import annotations

import http.client
import logging
import socket
import time
from urllib import request, error
from xml.etree import ElementTree

import wd


class Element(wd.Element):
    """When called get_by_id() for a new pre-print, fill as many properties as possible via regular ArXiv API"""
    __cache = None

    def apply(self, parsed_data: Model):
        super().apply(parsed_data)
        if ('en' not in self.entity['labels']) and parsed_data and parsed_data.label:
            self.entity['labels']['en'] = {'value': parsed_data.label, 'language': 'en'}

    def obtain_claim(self, snak: dict):
        if snak is not None:
            if snak['property'] == 'P2093' and 'qualifiers' in snak and 'P1545' in snak['qualifiers']:
                for property_id in ['P50', 'P2093']:
                    if property_id in self.entity['claims']:
                        for claim in self.entity['claims'][property_id]:
                            if 'qualifiers' in claim and 'P1545' in claim['qualifiers']:
                                if claim['qualifiers']['P1545'][0]['datavalue']['value'] == snak['qualifiers']['P1545']:
                                    return
            elif snak['property'] in self.entity['claims']:
                return
            return super().obtain_claim(snak)


class Model(wd.Model):
    property, db_ref, item, chunk, suffix = 'P818', 'Q118398', Element, {}, 'metadataPrefix=arXiv'

    def __init__(self, external_id: str, snaks: list = None):
        super().__init__(external_id, snaks)
        self.label, self.__doi = '', ''

    @staticmethod
    def arxiv_xml(query: str) -> ElementTree:
        for retries in range(5):
            try:
                with request.urlopen((url := 'https://export.arxiv.org/' + query), timeout=180) as file:
                    return ElementTree.fromstring(file.read())
            except error.HTTPError as e:
                if e.status == 503 and 'Retry-After' in e.headers:
                    time.sleep(int(e.headers['Retry-After']))
                    continue
                logging.error('While fetching {} got error: {}'.format(url, e.__str__()))
            except (http.client.IncompleteRead, ConnectionResetError, socket.timeout) as e:
                logging.error('While fetching {} got error: {}'.format(url, e.__str__()))
            time.sleep(1800)

    @classmethod
    def prepare_data(cls, external_id: str) -> []:
        if tree := Model.arxiv_xml('api/query?id_list=' + external_id):
            model = super().prepare_data(external_id)
            model.input_snaks.append(cls.create_snak('P31', 'Q13442814'))
            model.label = ' '.join(tree.findall('*/w3:title', Model.config('ns'))[0].text.split())
            model.input_snaks.append(cls.create_snak('P1476', model.label))
            author_num = 0
            for author in tree.findall('*/*/w3:name', Model.config('ns')):
                if len(author.text.strip()) > 3:
                    snak = cls.create_snak('P2093', author.text.strip())
                    snak['qualifiers'] = {'P1545': str(author_num := author_num + 1)}
                    model.input_snaks.append(snak)
            if len(doi_list := tree.findall('*/arxiv:doi', Model.config('ns'))) == 1:
                model.__doi = doi_list[0].text.upper()
                model.input_snaks.append(cls.create_snak('P356', model.__doi))
            return model

    @classmethod  # -------------------- Arxiv Bulk Data Access part --------------------
    def next(cls):
        cls.chunk = {}
        if tree := cls.arxiv_xml('oai2?verb=ListRecords&' + cls.suffix):
            for pp in tree.findall('.//oa:arXiv', Model.config('ns')):
                if len(lst := pp.findall('oa:doi', Model.config('ns'))) > 0:
                    doi = lst[0].text.split()[0].replace('\\', '').upper()
                    Model.chunk[pp.find('oa:id', Model.config('ns')).text] = doi
            if (element := tree.find('.//oai:resumptionToken', Model.config('ns'))) is not None and element.text:
                cls.suffix = 'resumptionToken=' + element.text
            else:
                return None
        return cls.chunk.keys()

    def get_qid(self):
        if self.__doi and (result := Element.haswbstatement(self.__doi, 'P356')):  # Found by DOI
            self.input_snaks = [self.create_snak(self.property, self.external_id)]  # only ArXiv-ID need to be set
            return result


if Model.initialize(__file__):  # if not imported
    # Model.get_by_id('2405.00850', forced=True)  # Uncomment to debug processing single preprint
    SUMMARY = 'extracted from [[Q118398]] based on [[Property:{}]]: {}'
    QUERY = 'SELECT ?c ?i {{VALUES ?c {{\'{}\'}} ?i p:P356/ps:P356 ?c MINUS {{?i p:P818 []; p:P356 []}}}}'
    no_doi_items = wd.Wikidata.query('SELECT ?c ?i {?i p:P818/ps:P818 ?c MINUS {?i p:P818 []; p:P356 []}}')

    while Model.next() is not None:
        no_arxiv_items = wd.Wikidata.query(QUERY.format('\' \''.join(Model.chunk.values())))
        for p818 in Model.chunk:
            if p356 := Model.chunk[p818]:
                if no_doi_items and (qid := no_doi_items.pop(p818, 0)):  # DOI is absent
                    wd.Claim.construct(Model.create_snak('P356', p356), qid).save(SUMMARY.format('P818', p818))
                elif no_arxiv_items and (qid := no_arxiv_items.pop(p356, 0)):  # Arxiv is absent
                    wd.Claim.construct(Model.create_snak('P818', p818), qid).save(SUMMARY.format('P356', p356))
