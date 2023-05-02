#!/usr/bin/python3
from decimal import Decimal
from os.path import basename
from sys import argv
from time import sleep

from adql import ADQL


class ExoArchive(ADQL):
    config = ADQL.load_config(__file__)
    db_property, db_ref = 'P5667', 'Q5420639'

    @staticmethod
    def get_next_chunk(offset):
        if len(ExoArchive.cache) > 0:
            return [], None
        ExoArchive.load()
        return ExoArchive.cache.keys(), None

    def construct_snak(self, row, col, new_col=None):
        def count_digits(idx):
            return len(str(Decimal(row[idx]).normalize()))

        if not col.startswith('p'):
            if count_digits('j' + col[1:]) > 2 + count_digits('e' + col[1:]):
                if col.startswith('j'):
                    return
            elif col.startswith('e'):
                return
        return super().construct_snak(row, col, 'p' + col[1:])


if argv[0].endswith(basename(__file__)):  # if not imported
    ExoArchive.logon(argv[1], argv[2])
    wd_items = ExoArchive.get_all_items('SELECT ?id ?item {?item p:P5667/ps:P5667 ?id}')
    for ex_id in wd_items:
        # ex_id = 'eps Tau b'
        item = ExoArchive(ex_id, wd_items[ex_id])
        item.prepare_data()
        item.update()
        sleep(1)
