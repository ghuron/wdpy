#!/usr/bin/python3
import wd


class Model(wd.TAPClient):
    property, db_ref = 'P819', 'Q654724'

    def __init__(self, external_id: str, snaks: list = None):
        super().__init__(external_id, snaks)
        self.__doi = None

    def construct_snak(self, row, col, new_col=None):
        if col == 'p356':
            self.__doi = row[col] = row[col].upper()
        super().construct_snak(row, col, new_col)

    def get_qid(self):
        return Element.haswbstatement(self.__doi, 'P356') if self.__doi else None


class Element(wd.AstroItem):
    _model, __cache = Model, None

    @classmethod
    def get_cache(cls, reset=None) -> dict:
        if reset is None and Element.__cache is None:
            query = 'SELECT ?c ?i {{ ?i p:P819/ps:P819 ?c }} OFFSET {} LIMIT {}'
            offset, Element.__cache = 0, {}
            while True:
                result = wd.Wikidata.query(query.format(offset, 400000))
                if result:
                    Element.__cache = Element.__cache | result
                    offset += 400000
                else:
                    break
        return super().get_cache(reset)

    def save(self):
        return super().save() if self.qid else None  # ToDo: item creation


Model.initialize(__file__)  # in order to load config
# Element.get_by_id('2023A&A...669A..24M')
