#!/usr/bin/python3
import adql
import wd


class Model(adql.Model):
    property = 'P819'

    def __init__(self, external_id: str, snaks: list = None):
        super().__init__(external_id, snaks)
        self.__doi = None

    def construct_snak(self, row, col, new_col=None):
        if col == 'p356':
            self.__doi = row[col] = row[col].upper()
        super().construct_snak(row, col, new_col)

    def get_qid(self):
        return Element.haswbstatement(self.__doi, 'P356') if self.__doi else None


class Element(adql.Element):
    _model, _claim, __cache = Model, type('Claim', (wd.Claim,), {'db_ref': 'Q654724'}), None

    def save(self):
        return super().save() if self.qid else None  # ToDo: item creation


Model.initialize(__file__)  # in order to load config
# Element.get_by_id('2023A&A...669A..24M')
