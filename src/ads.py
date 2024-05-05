#!/usr/bin/python3
import adql
import wd


class Model(adql.Model):
    property = 'P819'

    def __init__(self, external_id: str, snaks: list = None):
        super().__init__(external_id, snaks)
        self.doi = None

    def construct_snak(self, row, col, new_col=None):
        if col == 'p356':
            self.doi = row[col] = row[col].upper()
        super().construct_snak(row, col, new_col)


class Element(adql.Element):
    _model, _claim, __cache, __existing = Model, type('Claim', (wd.Claim,), {'db_ref': 'Q654724'}), {}, None

    def update(self, parsed_data: Model):
        if parsed_data:
            if self.qid is None and parsed_data.doi:
                self.qid = Element.haswbstatement(parsed_data.doi, 'P356')
            if self.qid:  # ToDo: create a new source if necessary
                return super().update(parsed_data)


Model.initialize(__file__)  # in order to load config
# Element.get_by_id('2023A&A...669A..24M')
