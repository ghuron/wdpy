#!/usr/bin/python3
import logging
from collections import OrderedDict
from decimal import Decimal, InvalidOperation
from time import sleep
from urllib.parse import quote_plus

import adql
import wd


class Model(adql.Model):
    property, redirect = 'P5667', {}

    @staticmethod
    def resolve_redirects(new, _):
        norm_id = new[0].replace('KOI-', 'K0')
        return (Model.redirect[norm_id][0]['pl_name'] if norm_id in Model.redirect else new[0]), new[1]

    @staticmethod
    def get_next_chunk(offset):
        if not offset and not Model.dataset:  # load only confirmed non-controversial exoplanets
            Model.load('P31 = \'CONFIRMED0\'')
            return Model.dataset.keys(), None
        elif offset and Model.dataset:  # try to load specific exoplanet ignoring its status
            Model.load('id = \'{}\''.format(offset))
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
        prefix, response = 'https://exoplanetarchive.ipac.caltech.edu/cgi-bin/Lookup/nph-aliaslookup.py?objname=', None
        if content := wd.Wikidata.request(prefix + quote_plus(external_id)):
            if 'resolved_name' not in (response := content.json())['manifest']:
                logging.error('"{}" not found'.format(external_id))
                return
            if (response_id := response['manifest']['resolved_name']) != external_id:
                logging.info('"{}" will be replaced with {}'.format(external_id, external_id := response_id))

        if (input_snaks := super().prepare_data(external_id)) and response:
            try:  # ToDo: code below works for planets only, we need to add stars as well
                for code in response['system']['objects']['planet_set']['planets'][external_id]['alias_set']['aliases']:
                    if snak := cls.construct_snak({'p528': code[:-2] + code[-1] if code[-2] == ' ' else code}, 'p528'):
                        input_snaks.append(snak)
            except KeyError:
                pass
        return input_snaks


class Element(adql.Element):
    __p5653, __cache, _model, _claim = None, {}, Model, type('Claim', (wd.Claim,), {'db_ref': 'Q5420639'})

    def update(self, parsed_data):
        if not self.qid:
            if not Element.__p5653:
                Element.__p5653 = wd.Wikidata.query('SELECT ?c ?i {?i wdt:P5653 ?c MINUS {?i wdt:P5667 []}}')
            if self.external_id in Element.__p5653:  # Try to reuse item from Exoplanet.eu
                self.qid = Element.__p5653[self.external_id]
        return super().update(parsed_data)

    def obtain_claim(self, snak):
        if snak and snak['property'] == 'P528':  # All catalogue codes for exoplanets should be aliases
            self.entity['aliases'] = {} if 'aliases' not in self.entity else self.entity['aliases']
            self.entity['aliases']['en'] = [] if 'en' not in self.entity['aliases'] else self.entity['aliases']['en']
            self.entity['aliases']['en'].append({'value': snak['datavalue']['value'], 'language': 'en'})
        return super().obtain_claim(snak)


if Model.initialize(__file__):  # if not imported
    Model.redirect = Model.tap_query(Model.config('endpoint'), Model.config('redirects'))
    wd_items = Model.get_all_items('SELECT ?id ?item {?item p:P5667/ps:P5667 ?id}', Model.resolve_redirects)
    for ex_id in OrderedDict(sorted(wd_items.items())):
        # ex_id = 'eps Tau b'
        if wd_items[ex_id]:  # Temporary disable creation of new items
            Element(ex_id, wd_items[ex_id]).update(Model.prepare_data(ex_id))
            sleep(1)
