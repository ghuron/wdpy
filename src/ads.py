#!/usr/bin/python3
from wikidata import WikiData


class ADS(WikiData):
    db_property, db_ref = 'P819', 'Q654724'

    def prepare_data(self, source=None) -> None:
        if self.qid:
            super().prepare_data()
            if source and source['p577'] and 'P577' not in self.entity['claims']:
                self.input_snaks.append(ADS.create_snak('P577', source['p577']))

    @classmethod
    def get_by_id(cls, external_id: str, create=True):
        if qid := WikiData.api_search('haswbstatement:"{}={}"'.format(cls.db_property, external_id)):
            return qid

        query = 'SELECT bibcode, doi AS P356, "year" AS P577 FROM ref WHERE bibcode=\'{}\''.format(external_id)
        from adql import ADQL  # to avoid circular import
        if len(result := ADQL.tap_query('https://simbad.u-strasbg.fr/simbad/sim-tap', query)) == 1:
            if qid := WikiData.api_search('haswbstatement:"P356={}"'.format(result[external_id][0]['p356'])):
                instance = ADS(external_id, qid)
                instance.prepare_data(result[external_id][0])
                return instance.update()
