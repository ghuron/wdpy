#!/usr/bin/python3
import logging
from decimal import Decimal, InvalidOperation
from urllib.parse import quote_plus

import adql
import wd


class Model(adql.Model):
    property, __ids = 'P5667', None

    @classmethod
    def next(cls):
        cls._dataset = cls.load('P31 = \'CONFIRMED0\'') if not cls._dataset else {}
        return cls._dataset.keys()

    def construct_snak(self, row, col, new_col=None):
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
        super().construct_snak(row, col, 'p' + col[1:])

    @classmethod
    def prepare_data(cls, external_id):
        prefix, response = 'https://exoplanetarchive.ipac.caltech.edu/cgi-bin/Lookup/nph-aliaslookup.py?objname=', None
        if content := wd.Wikidata.request(prefix + quote_plus(external_id)):
            if 'resolved_name' not in (response := content.json())['manifest']:
                raise ValueError()
            external_id = response['manifest']['resolved_name']

        if (model := super().prepare_data(external_id)) and response:
            try:  # ToDo: code below works for planets only, we need to add stars as well
                for code in response['system']['objects']['planet_set']['planets'][external_id]['alias_set']['aliases']:
                    model.construct_snak({'p528': code[:-2] + code[-1] if code[-2] == ' ' else code}, 'p528')
            except KeyError:
                pass
        return model

    def get_qid(self):
        if not Model.__ids:
            Model.__ids = wd.Wikidata.query('SELECT ?iLabel ?i {?i wdt:P5653 [] MINUS {?i wdt:P5667 []} ' +
                                            'SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }}')
        return Model.__ids[self.external_id] if self.external_id in Model.__ids else None


class Element(adql.Element):
    _model, _claim, __cache = Model, type('Claim', (wd.Claim,), {'db_ref': 'Q5420639'}), None

    def obtain_claim(self, snak):
        if snak and snak['property'] == 'P528':  # All catalogue codes for exoplanets should be aliases
            self.entity['aliases'] = {} if 'aliases' not in self.entity else self.entity['aliases']
            self.entity['aliases']['en'] = [] if 'en' not in self.entity['aliases'] else self.entity['aliases']['en']
            self.entity['aliases']['en'].append({'value': snak['datavalue']['value'], 'language': 'en'})
        return super().obtain_claim(snak)

    @classmethod
    def get_cache(cls, reset=None) -> dict:
        def resolve_redirects(new, _):
            norm_id = new[0].replace('KOI-', 'K0')
            return (redirect[norm_id][0]['pl_name'] if norm_id in redirect else new[0]), new[1]

        if cls.__cache is None:
            redirect = Model.tap_query(Model.config('endpoint'), Model.config('redirects'))
            cls.__cache = wd.Wikidata.query('SELECT ?id ?item {?item p:P5667/ps:P5667 ?id}', resolve_redirects)
        return super().get_cache(reset)


if Model.initialize(__file__):  # if not imported
    # Element.get_by_id('EPIC 210754593b', forced=True)  # uncomment to debug specific item only
    wd_items, ex_items = sorted(Element.get_cache().keys()), sorted(Model.next())  # Preload both
    logging.info('Start updating {} existing items'.format(len(wd_items)))
    for ex_id in wd_items:
        Element.get_by_id(ex_id, forced=True)
    logging.info('Finish updating existing items')
    for ex_id in ex_items:
        if ex_id not in Element.get_cache():  # not wd_items!
            Element.get_by_id(ex_id, forced=True)
