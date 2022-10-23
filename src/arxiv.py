#!/usr/bin/python3
import http.client
import logging
import os.path
import random
import sys
import time
from urllib import request, error
from xml.etree import ElementTree

from wikidata import WikiData


class ArXiv(WikiData):
    arxiv = {}
    db_property = 'P818'
    db_ref = 'Q118398'

    @staticmethod
    def get_xml(url):
        for retries in range(1, 5):
            try:
                with request.urlopen(url, timeout=100) as file:
                    return ElementTree.fromstring(file.read())
            except (error.HTTPError, http.client.IncompleteRead, ConnectionResetError, TimeoutError):
                logging.error('Error while fetching ' + url)
                time.sleep(600)
        return None

    @staticmethod
    def get_next_chunk(suffix):
        result = {}
        suffix = '&metadataPrefix=arXiv' if suffix is None else suffix
        if (tree := ArXiv.get_xml('http://export.arxiv.org/oai2?verb=ListRecords' + suffix)) is not None:
            ns = {'oa': 'http://arxiv.org/OAI/arXiv/', 'oai': 'http://www.openarchives.org/OAI/2.0/'}
            for preprint in tree.findall('.//oa:arXiv', ns):
                if len(doi := preprint.findall('oa:doi', ns)) > 0:
                    result[preprint.find('oa:id', ns).text] = doi[0].text.split()[0].replace('\\', '')
                else:
                    result[preprint.find('oa:id', ns).text] = None
            ArXiv.arxiv = ArXiv.arxiv | result
            if suffix.endswith('arXiv'):  # randomise the starting offset
                random.seed()
                suffix = '&resumptionToken=' + tree.find('.//oai:resumptionToken', ns).text.split('|')[0]
                suffix = suffix + '|' + str(random.randint(0, 2000)) + '001'
            else:
                suffix = '&resumptionToken=' + tree.find('.//oai:resumptionToken', ns).text
        return result.keys(), suffix

    def __init__(self, external_id, qid=None):
        super().__init__(external_id, qid)
        self.doi = None

    def prepare_data(self, source=None):
        if self.external_id in self.arxiv:
            super().prepare_data()
            if self.arxiv[self.external_id] is not None:
                self.doi = self.arxiv[self.external_id].upper()
                self.input_snaks.append(self.create_snak('P356', self.doi))
        elif tree := self.get_xml('https://export.arxiv.org/api/query?id_list=' + self.external_id):
            super().prepare_data()
            ns = {'w3': 'http://www.w3.org/2005/Atom', 'arxiv': 'http://arxiv.org/schemas/atom'}
            title = ' '.join(tree.findall('*/w3:title', ns)[0].text.split())
            self.input_snaks.append(self.create_snak('P1476', {'text': title, 'language': 'en'}))
            author_num = 0
            for author in tree.findall('*/*/w3:name', ns):
                if len(author.text.strip()) > 3:
                    snak = self.create_snak('P2093', author.text.strip())
                    snak['qualifiers'] = {'P1545': str(author_num := author_num + 1)}
                    self.input_snaks.append(snak)
            if len(doi := tree.findall('*/arxiv:doi', ns)) == 1:
                self.doi = doi[0].text.upper()
                self.input_snaks.append(self.create_snak('P356', self.doi))

        if self.doi and self.entity is None and (qid := ArXiv.api_search('haswbstatement:"P356={}"'.format(self.doi))):
            if result := ArXiv.load_items([qid]):
                self.entity = result[qid]

    def obtain_claim(self, snak):
        if snak is not None:
            if snak['property'] == 'P1476' and 'P1476' in self.entity['claims']:
                return
            if snak['property'] == 'P2093' and 'qualifiers' in snak and 'P1545' in snak['qualifiers']:
                for property_id in ['P50', 'P2093']:
                    if property_id in self.entity['claims']:
                        for claim in self.entity['claims'][property_id]:
                            if 'qualifiers' in claim and 'P1545' in claim['qualifiers']:
                                if claim['qualifiers']['P1545'][0]['datavalue']['value'] == snak['qualifiers']['P1545']:
                                    return
            if snak['property'] == 'P356':
                if 'P356' in self.entity['claims']:
                    doi = self.entity['claims']['P356'][0]['mainsnak']['datavalue']['value']
                    if snak['datavalue']['value'] != doi:
                        self.trace('has DOI {}, but arxiv {} contains {}'.format(doi, self.external_id, self.doi))
                        return
                elif qid := ArXiv.api_search('haswbstatement:"P356={}"'.format(self.doi)):
                    self.trace('arxiv {}, DOI {} already assigned to {}'.format(self.external_id, self.doi, qid))
                    return

        claim = super().obtain_claim(snak)
        return claim

    def post_process(self):
        if 'labels' not in self.entity:
            self.entity['labels'] = {}
        if 'en' not in self.entity['labels'] and 'P1476' in self.entity['claims']:
            self.entity['labels']['en'] = {
                'value': self.entity['claims']['P1476'][0]['mainsnak']['datavalue']['value']['text'],
                'language': 'en'}
        if 'P31' not in self.entity['claims']:
            self.obtain_claim(self.create_snak('P31', 'Q13442814'))


if sys.argv[0].endswith(os.path.basename(__file__)):  # if not imported
    ArXiv.logon(sys.argv[1], sys.argv[2])
    wd_items = ArXiv.query('SELECT ?id (SAMPLE(?i) AS ?a) {?i wdt:P818 ?id} GROUP BY ?id')
    offset = None
    while True:
        chunk, offset = ArXiv.get_next_chunk(offset)
        if len(chunk) == 0:
            break
        for ex_id in chunk:
            item = ArXiv(ex_id, wd_items[ex_id] if ex_id in wd_items else None)  # ToDo: find items with invalid arxiv
            item.prepare_data()
            if item.entity is not None:
                if 'P818' not in item.entity['claims'] or item.doi is not None and 'P356' not in item.entity['claims']:
                    item.update()
