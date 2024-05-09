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

    def apply(self, parsed_data: Model):
        if parsed_data and parsed_data.doi and (self.qid is None):
            self.set_qid(Element.haswbstatement(parsed_data.doi, 'P356'))
        super().apply(parsed_data)

    def save(self):
        return super().save() if self.qid else None  # ToDo: item creation


Model.initialize(__file__)  # in order to load config
# Element.get_by_id('2023A&A...669A..24M')
