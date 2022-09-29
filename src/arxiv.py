#!/usr/bin/python3
import http.client
import os.path
import sys
import time
from urllib import request, error
from wikidata import WikiData
from xml.etree import ElementTree


class ArXiv(WikiData):
    arxiv = {}

    def __init__(self, external_id):
        super().__init__(external_id, 'P818', 'Q118398')
        self.author_num = 0

    @staticmethod
    def get_xml(url):
        for retries in range(1, 3):
            try:
                with request.urlopen(url) as file:
                    return ElementTree.fromstring(file.read())
            except (error.HTTPError, http.client.IncompleteRead):
                print('Error while fetching ' + url)
                time.sleep(60)
        return None

    @staticmethod
    def get_next_chunk(suffix):
        suffix = '&metadataPrefix=arXiv' if suffix is None else suffix
        if len(ArXiv.arxiv) > 100:
            return [], 0
        result = {}
        if (tree := ArXiv.get_xml('http://export.arxiv.org/oai2?verb=ListRecords' + suffix)) is not None:
            ns = {'oa': 'http://arxiv.org/OAI/arXiv/', 'oai': 'http://www.openarchives.org/OAI/2.0/'}
            for preprint in tree.findall('.//oa:arXiv', ns):
                if len(doi := preprint.findall('oa:doi', ns)) > 0:
                    result[preprint.find('oa:id', ns).text] = doi[0].text.upper().split()[0].replace('\\', '')
                else:
                    result[preprint.find('oa:id', ns).text] = None
            ArXiv.arxiv = ArXiv.arxiv | result
            suffix = '&resumptionToken=' + tree.find('.//oai:resumptionToken', ns).text
        return result.keys(), suffix

    def post_process(self):
        if 'labels' not in self.entity:
            self.entity['labels'] = {}
        if 'en' not in self.entity['labels'] and 'P1476' in self.entity['claims']:
            self.entity['labels']['en'] = {
                'value': self.entity['claims']['P1476'][0]['mainsnak']['datavalue']['value']['text'],
                'language': 'en'}
        if 'P31' not in self.entity['claims']:
            self.obtain_claim(self.create_snak('P31', 'Q13442814'))

    def obtain_claim(self, snak):
        if snak is not None:
            if snak['property'] == 'P1476' and 'P1476' in self.entity['claims']:
                return
            if snak['property'] == 'P2093':
                self.author_num += 1
                for property_id in ['P50', 'P2093']:
                    if property_id in self.entity['claims']:
                        for claim in self.entity['claims'][property_id]:
                            if 'qualifiers' in claim and 'P1545' in claim['qualifiers']:
                                if claim['qualifiers']['P1545'][0]['datavalue']['value'] == str(self.author_num):
                                    return None
        claim = super().obtain_claim(snak)
        if claim is not None and snak is not None and snak['property'] == 'P2093':
            claim['qualifiers'] = {'P1545': [self.create_snak('P1545', str(self.author_num))]}
        return claim

    def get_snaks(self):
        result = super().get_snaks()
        if self.external_id in self.arxiv:
            if self.arxiv[self.external_id] is not None:
                result.append(self.create_snak('P356', self.arxiv[self.external_id]))
        elif tree := self.get_xml('https://export.arxiv.org/api/query?id_list=' + self.external_id):
            ns = {'w3': 'http://www.w3.org/2005/Atom', 'arxiv': 'http://arxiv.org/schemas/atom'}
            if len(doi := tree.findall('*/arxiv:doi', ns)) == 1:
                result.append(self.create_snak('P356', doi[0].text.upper()))
            result.append(self.create_snak('P1476', {'text': ' '.join(tree.findall('*/w3:title', ns)[0].text.split()),
                                                     'language': 'en'}))
            self.author_num = 0
            for author in tree.findall('*/*/w3:name', ns):
                result.append(self.create_snak('P2093', author.text))
        return result

    @staticmethod
    def get_doi(input_data):
        if input_data is not None:
            for snak in input_data:
                if snak['property'] == 'P356':
                    return snak['datavalue']['value']

    def load_article(self, qid, doi):
        if qid is not None or (doi is not None) and (qid := self.api_search('haswbstatement:"P356={}"'.format(doi))):
            self.entity = self.get_item(qid)

    def sync(self, qid=None):
        self.load_article(qid, self.get_doi(input_data := self.get_snaks()))
        return self.update(input_data)


if sys.argv[0].endswith(os.path.basename(__file__)):  # if not imported
    ArXiv.logon(sys.argv[1], sys.argv[2])
    wd_items = ArXiv.get_all_items('SELECT ?id (SAMPLE(?i) AS ?a) {?i wdt:P818 ?id} GROUP BY ?id')
    for arxiv_id in wd_items:
        item = ArXiv(arxiv_id)
        if arxiv_id not in ArXiv.arxiv:  # if it not comes from OAI, it must come from sparql
            print(wd_items[arxiv_id] + 'contains arxiv value ' + arxiv_id + ' that is not found in batch OAI')
        else:
            new_doi = item.get_doi(snaks := item.get_snaks())
            item.load_article(wd_items[arxiv_id], new_doi)
            if item.entity is not None:
                if 'P818' not in item.entity['claims'] or new_doi is not None and 'P356' not in item.entity['claims']:
                    item.update(snaks)
