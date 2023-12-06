#!/usr/bin/python3
import logging
from collections import OrderedDict
from decimal import Decimal, InvalidOperation
from time import sleep

from adql import ADQL
from wd import Wikidata


class ExoArchive(ADQL):
    db_property, db_ref = 'P5667', 'Q5420639'
    redirect = {}

    @staticmethod
    def resolve_redirects(new, _):
        norm_id = new[0].replace('KOI-', 'K0')
        return (ExoArchive.redirect[norm_id][0]['pl_name'] if norm_id in ExoArchive.redirect else new[0]), new[1]

    @staticmethod
    def get_next_chunk(offset):
        if not offset and not ExoArchive.dataset:  # load only confirmed non-controversial exoplanets
            ExoArchive.load('P31 = \'CONFIRMED0\'')
            return ExoArchive.dataset.keys(), None
        elif offset and ExoArchive.dataset:  # try to load specific exoplanet ignoring its status
            ExoArchive.load('id = \'{}\''.format(offset))
        return [], None

    @classmethod
    def construct_snak(cls, row, col, new_col=None):
        def count_digits(idx):
            return len(str(Decimal(row[idx]).normalize()))

        if not col.startswith('p'):
            try:
                if count_digits('j' + col[1:]) > 2 + count_digits('e' + col[1:]):
                    if col.startswith('j'):
                        return
                elif col.startswith('e'):
                    return
            except InvalidOperation:
                return
        return super().construct_snak(row, col, 'p' + col[1:])

    missing = None

    @classmethod
    def prepare_data(cls, external_id) -> []:
        if input_snaks := super().prepare_data(external_id):
            prefix = 'https://exoplanetarchive.ipac.caltech.edu/cgi-bin/Lookup/nph-aliaslookup.py?objname='
            if (response := Wikidata.request(prefix + external_id)) and 'system' in response.json():
                if external_id in (data := response.json()['system']['objects']['planet_set']['planets']):
                    for code in data[external_id]['alias_set']['aliases']:
                        if snak := cls.construct_snak({'p528': code.replace(' ', '')}, 'p528'):
                            input_snaks.append(snak)
                else:
                    logging.info('{} appears to have redirect'.format(prefix + external_id))
            return input_snaks

    __p5653 = None

    def update(self, parsed_data):
        if not self.qid:  # Try to reuse item from Exoplanet.eu
            if not ExoArchive.__p5653:
                ExoArchive.__p5653 = Wikidata.query('SELECT ?c ?i {?i wdt:P5653 ?c MINUS {?i wdt:P5667 []}}')
            if self.external_id in ExoArchive.__p5653:
                self.qid = ExoArchive.__p5653[self.external_id]
        return super().update(parsed_data)

    def obtain_claim(self, snak):
        if snak and snak['property'] == 'P528':  # All catalogue codes for exoplanets should be aliases
            self.entity['aliases'] = {} if 'aliases' not in self.entity else self.entity['aliases']
            self.entity['aliases']['en'] = [] if 'en' not in self.entity['aliases'] else self.entity['aliases']['en']
            self.entity['aliases']['en'].append({'value': snak['datavalue']['value'], 'language': 'en'})
        return super().obtain_claim(snak)


if ExoArchive.initialize(__file__):  # if not imported
    ExoArchive.redirect = ExoArchive.tap_query(ExoArchive.config('endpoint'), ExoArchive.config('redirects'))
    wd_items = ExoArchive.get_all_items('SELECT ?id ?item {?item p:P5667/ps:P5667 ?id}', ExoArchive.resolve_redirects)
    for ex_id in OrderedDict(sorted(wd_items.items())):
        # ex_id = 'eps Tau b'
        ExoArchive(ex_id, wd_items[ex_id]).update(ExoArchive.prepare_data(ex_id))
        sleep(1)
