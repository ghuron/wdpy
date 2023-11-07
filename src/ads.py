#!/usr/bin/python3
import logging

from wd import Element


class ADS(Element):
    db_property, db_ref = 'P819', 'Q654724'

    def prepare_data(self, source=None) -> None:
        if self.qid:
            super().prepare_data()
            if source and source['p577'] and 'P577' not in self.entity['claims']:
                self.input_snaks.append(ADS.create_snak('P577', source['p577']))

    @classmethod
    def get_by_id(cls, external_id: str, create=True):
        try:
            if qid := cls.haswbstatement(external_id):
                return qid

            query = 'SELECT bibcode, doi AS P356, "year" AS P577 FROM ref WHERE bibcode=\'{}\''.format(external_id)
            from adql import ADQL  # to avoid circular import
            if (result := ADQL.tap_query('https://simbad.u-strasbg.fr/simbad/sim-tap', query)) and len(result) == 1:
                if qid := cls.haswbstatement(result[external_id][0]['p356'], 'P356'):
                    instance = ADS(external_id, qid)
                    instance.prepare_data(result[external_id][0])
                    return instance.update()
                # ToDo: create a new source
        except ValueError as e:
            logging.warning('Found {} instances of {}="{}", skipping'.format(e.args[0], cls.db_property, external_id))
