#!/usr/bin/python3
import http.client
import logging
import socket
import time
from urllib import request, error
from xml.etree import ElementTree

from wd import Wikidata, Model, Element


class ArXiv(Element):
    dataset = {}
    db_property, db_ref = 'P818', 'Q118398'

    @staticmethod
    def arxiv_xml(query):
        retries = 0
        while retries < 5:
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
            retries += 1
            time.sleep(1800)
        return None

    @staticmethod
    def get_next_chunk(suffix):
        NS = {'oa': 'http://arxiv.org/OAI/arXiv/', 'oai': 'http://www.openarchives.org/OAI/2.0/'}
        ArXiv.dataset, token = {}, None
        if tree := ArXiv.arxiv_xml('oai2?verb=ListRecords&' + (suffix if suffix else 'metadataPrefix=arXiv')):
            for preprint in tree.findall('.//oa:arXiv', NS):
                if len(doi_list := preprint.findall('oa:doi', NS)) > 0:
                    ArXiv.dataset[preprint.find('oa:id', NS).text] = doi_list[0].text.split()[0].replace('\\', '')
            if (element := tree.find('.//oai:resumptionToken', NS)) is not None and element.text:
                token = 'resumptionToken=' + element.text
        return ArXiv.dataset.keys(), token

    @classmethod
    def prepare_data(cls, external_id) -> []:
        if tree := ArXiv.arxiv_xml('api/query?id_list=' + external_id):
            snaks = super().prepare_data(external_id) + [cls.create_snak('P31', 'Q13442814')]
            ns = {'w3': 'http://www.w3.org/2005/Atom', 'arxiv': 'http://arxiv.org/schemas/atom'}
            title = ' '.join(tree.findall('*/w3:title', ns)[0].text.split())
            snaks.append(cls.create_snak('P1476', {'text': title, 'language': 'en'}))
            author_num = 0
            for author in tree.findall('*/*/w3:name', ns):
                if len(author.text.strip()) > 3:
                    snak = cls.create_snak('P2093', author.text.strip())
                    snak['qualifiers'] = {'P1545': str(author_num := author_num + 1)}
                    snaks.append(snak)
            doi = ''
            if len(doi_list := tree.findall('*/arxiv:doi', ns)) == 1:
                snaks.append(cls.create_snak('P356', doi := doi_list[0].text.upper()))
            return {'input': snaks, 'label': title, 'doi': doi}

    def update(self, parsed_data):
        if parsed_data:
            if self.qid is None and parsed_data['doi']:
                self.qid = ArXiv.haswbstatement(parsed_data['doi'], 'P356')
            if 'en' not in self.entity['labels']:
                self.entity['labels']['en'] = {'value': parsed_data['label'], 'language': 'en'}
            return super().update(parsed_data['input'])

    def obtain_claim(self, snak):
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


if ArXiv.initialize(__file__):  # if not imported
    # ArXiv.get_by_id('2311.05349')
    SUMMARY = 'extracted from [[Q118398]] based on [[Property:{}]]: {}'
    QUERY = 'SELECT ?c ?i {{VALUES ?c {{\'{}\'}} ?i p:P356/ps:P356 ?c MINUS {{?i p:P818 []; p:P356 []}}}}'
    no_doi_items = Wikidata.query('SELECT ?c ?i {?i p:P818/ps:P818 ?c MINUS {?i p:P818 []; p:P356 []}}')
    offset = 'metadataPrefix=arXiv'
    while offset is not None:
        _, offset = ArXiv.get_next_chunk(offset)
        no_arxiv_items = Wikidata.query(QUERY.format('\' \''.join(ArXiv.dataset.values())))
        for p818 in ArXiv.dataset:
            if p356 := ArXiv.dataset[p818]:
                if no_doi_items and (p818 in no_doi_items):  # DOI is absent
                    Model.set_id(no_doi_items.pop(p818), 'P356', p356.upper(), SUMMARY.format('P818', p818))
                elif no_arxiv_items and (p356 in no_arxiv_items):  # Arxiv is absent
                    Model.set_id(no_arxiv_items.pop(p356), 'P818', p818, SUMMARY.format('P356', p356))
