#!/usr/bin/python3
from decimal import Decimal, InvalidOperation
from os.path import basename
from sys import argv
from time import sleep

from adql import ADQL


class ExoArchive(ADQL):
    config = ADQL.load_config(__file__)
    db_property, db_ref = 'P5667', 'Q5420639'
    redirect = {}

    @staticmethod
    def resolve_redirects(new, _):
        norm_id = new[0].replace('KOI-', 'K0')
        return (ExoArchive.redirect[norm_id][0]['pl_name'] if norm_id in ExoArchive.redirect else new[0]), new[1]

    @staticmethod
    def get_next_chunk(offset):
        if not offset and not ExoArchive.cache:  # load only confirmed non-controversial exoplanets
            ExoArchive.load('P31 = \'CONFIRMED0\'')
            return ExoArchive.cache.keys(), None
        elif offset and ExoArchive.cache:  # try to load specific exoplanet ignoring its status
            ExoArchive.load('id = \'{}\''.format(offset))
        return [], None

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
        return super().construct_snak(row, col, 'p' + col[1:])

    missing = None

    def prepare_data(self, source=None):
        if not self.qid:  # Try to reuse item from exoplanet.eu
            if not ExoArchive.missing:  # Lazy load
                ExoArchive.missing = ADQL.query('SELECT ?c ?i {?i wdt:P5653 ?c FILTER NOT EXISTS {?i wdt:P5667 []}}')
            self.qid = ExoArchive.missing[self.external_id] if self.external_id in ExoArchive.missing else self.qid
        super().prepare_data()


if argv[0].endswith(basename(__file__)):  # if not imported
    ExoArchive.logon(argv[1], argv[2])
    ExoArchive.redirect = ExoArchive.tap_query(ExoArchive.config['endpoint'], ExoArchive.config['redirects'])
    wd_items = ExoArchive.get_all_items('SELECT ?id ?item {?item p:P5667/ps:P5667 ?id}', ExoArchive.resolve_redirects)
    for ex_id in wd_items:
        # ex_id = 'eps Tau b'
        item = ExoArchive(ex_id, wd_items[ex_id])
        item.prepare_data()
        item.update()
        sleep(1)
