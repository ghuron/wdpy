#!/usr/bin/python3
import http.client
import json
import os.path
import sys
import urllib.request
import urllib.error
import xml.etree.ElementTree as ElementTree
import time

from wikidata import WikiData


class ArXiv(WikiData):
    def __init__(self, login, password):
        super().__init__(login, password)
        self.db_ref = 'Q118398'
        self.db_property = 'P818'
        self.suffix = '&metadataPrefix=arXiv'
        self.arxiv = {}

    def get_next_chunk(self):
        # if len(self.arxiv) > 100000:
        #     return []
        while True:
            try:
                file = urllib.request.urlopen('http://export.arxiv.org/oai2?verb=ListRecords' + self.suffix)
                data = file.read()
                file.close()
                break
            except (urllib.error.HTTPError, http.client.IncompleteRead):
                print('Error while fetching ' + self.suffix)
                time.sleep(60)
                continue

        tree = ElementTree.fromstring(data)
        ns = {'oa': 'http://arxiv.org/OAI/arXiv/'}
        result = {}
        for preprint in tree.findall('.//oa:arXiv', ns):
            if len(doi := preprint.findall('oa:doi', ns)) > 0:
                result[preprint.find('oa:id', ns).text] = doi[0].text.upper().split()[0].replace('\\', '')
            else:
                result[preprint.find('oa:id', ns).text] = None
        self.arxiv = self.arxiv | result
        self.suffix = '&resumptionToken=' + tree.find('.//oa:resumptionToken',
                                                      {'oa': 'http://www.openarchives.org/OAI/2.0/'}).text
        return result


if sys.argv[0].endswith(os.path.basename(__file__)):  # if not imported
    wd = ArXiv(sys.argv[1], sys.argv[2])
    wd_items = wd.get_all_items('SELECT ?id (SAMPLE(?i) AS ?a) {?i wdt:P818 ?id} GROUP BY ?id')
    for arxiv_id in wd_items:
        if arxiv_id not in wd.arxiv:  # if it not comes from OAI, it must comes from sparql
            wd.trace(wd_items[arxiv_id], 'contains arxiv value ' + arxiv_id + ' that is not found in batch OAI')
            continue

        if wd_items[arxiv_id] is None:
            if (qid := wd.api_search('haswbstatement:"P356=' + wd.arxiv[arxiv_id] + '"')) is None:
                continue
        else:
            qid = wd_items[arxiv_id]

        info = json.loads(wd.api_call('wbgetentities', {'props': 'claims|info|labels', 'ids': qid}))
        if 'entities' not in info:
            continue
        item = info['entities'][qid]

        if 'P356' not in item['claims'] or 'P818' not in item['claims']:
            wd.sync(item, [wd.create_snak('P356', wd.arxiv[arxiv_id]), wd.create_snak('P818', arxiv_id)], arxiv_id)
            wd.save(item)
