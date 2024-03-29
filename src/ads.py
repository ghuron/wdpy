#!/usr/bin/python3
from adql import ADQL


class ADS(ADQL):
    db_property, db_ref = 'P819', 'Q654724'

    @staticmethod
    def get_next_chunk(offset: any):
        ADS.load('id = \'{}\''.format(offset))
        return [], None

    __doi = None

    @classmethod
    def prepare_data(cls, external_id) -> []:
        ADS.__doi = None
        if (result := super().prepare_data(external_id)) and ADS.__doi:
            return {'input': result, 'doi': ADS.__doi}  # {**result, 'doi': ADS.__doi}

    @classmethod
    def create_snak(cls, property_id: str, value, lower: str = None, upper: str = None):
        if (property_id == 'P356') and value:
            ADS.__doi = (value := value.upper())
        return super().create_snak(property_id, value, lower, upper)

    def update(self, parsed_data):
        if parsed_data:
            if self.qid is None and parsed_data['doi']:
                self.qid = ADS.haswbstatement(parsed_data['doi'], 'P356')
            if self.qid:  # ToDo: create a new source if necessary
                return super().update(parsed_data['input'])


ADS.initialize(__file__)
# ADS.get_by_id('2023ApJ...943...15W')
