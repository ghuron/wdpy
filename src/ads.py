#!/usr/bin/python3
import adql
import wd


class Model(adql.Model):
    property, __doi = 'P819', None  # ToDo make instance field

    @staticmethod
    def get_next_chunk(offset: any):
        Model.load('id = \'{}\''.format(offset))
        return [], None

    @classmethod
    def prepare_data(cls, external_id) -> []:
        Model.__doi = None
        if (result := super().prepare_data(external_id)) and Model.__doi:
            return {'input': result, 'doi': Model.__doi}

    @classmethod
    def construct_snak(cls, row, col, new_col=None):
        if (result := super().construct_snak(row, col, new_col)) and (col == 'p356'):
            Model.__doi = result['datavalue']['value'].upper()
        return result


class Element(adql.Element):
    _model, _claim = Model, type('Claim', (wd.Claim,), {'db_ref': 'Q654724'})

    def update(self, parsed_data):
        if parsed_data:
            if self.qid is None and parsed_data['doi']:
                self.qid = Element.haswbstatement(parsed_data['doi'], 'P356')
            if self.qid:  # ToDo: create a new source if necessary
                return super().update(parsed_data['input'])


Model.initialize(__file__)  # in order to load config
# Element.get_by_id('2023ApJ...943...15W')
