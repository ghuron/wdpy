#!/usr/bin/python3
import logging
from decimal import Decimal, InvalidOperation
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
                raise ValueError()
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
    __ids, __cache, __existing, _model, _claim = None, {}, None, Model, type('Claim', (wd.Claim,),
                                                                             {'db_ref': 'Q5420639'})

    def update(self, parsed_data):
        if self.qid:
            if not Element.__ids:
                Element.__ids = wd.Wikidata.query('SELECT ?iLabel ?i {?i wdt:P5653 [] MINUS {?i wdt:P5667 []} ' +
                                                  'SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }}')
            if self.external_id in Element.__ids:  # Try to reuse item from Exoplanet.eu
                self.qid = Element.__ids[self.external_id]
        return super().update(parsed_data)

    def obtain_claim(self, snak):
        if snak and snak['property'] == 'P528':  # All catalogue codes for exoplanets should be aliases
            self.entity['aliases'] = {} if 'aliases' not in self.entity else self.entity['aliases']
            self.entity['aliases']['en'] = [] if 'en' not in self.entity['aliases'] else self.entity['aliases']['en']
            self.entity['aliases']['en'].append({'value': snak['datavalue']['value'], 'language': 'en'})
        return super().obtain_claim(snak)

    @classmethod
    def is_bad_id(cls, external_id: str, reset=None) -> bool:
        if cls.__existing is None:
            Model.redirect = Model.tap_query(Model.config('endpoint'), Model.config('redirects'))
            cls.__existing = Model.get_all_items('SELECT ?id ?item {?item p:P5667/ps:P5667 ?id}',
                                                 Model.resolve_redirects)
        return super().is_bad_id(external_id, reset)


if Model.initialize(__file__):  # if not imported
    # Element.get_by_id('eps Tau b', forced=True)
    postponed, start_at = [], None
    while True:
        chunk, start_at = Model.get_next_chunk(start_at)
        if len(chunk) == 0:
            break
        logging.info('Start updating of {} mandatory items'.format(len(chunk)))
        for ex_id in sorted(chunk):
            Element.get_by_id(ex_id, forced=True)

    logging.info('Start updating of {} optional items'.format(len(Element.get_remaining())))
    for ex_id in Element.get_remaining():
        Element.get_by_id(ex_id, forced=True)

    logging.info('Start processing of {} presumably new items'.format(len(postponed)))
