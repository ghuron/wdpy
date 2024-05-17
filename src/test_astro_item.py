from unittest import TestCase, mock

from wd import Model, AstroItem


class TestObtainClaim(TestCase):
    @mock.patch('wd.Wikidata.query', return_value={'P1215': 'quantity', 'P1227': 'wikibase-item'})
    @mock.patch('wd.Wikidata.load', return_value=None)
    def test_preferred_rank_for_v_mag(self, mock_load, _):
        (v_mag := Model.create_snak('P1215', 0))['qualifiers'] = {'P1227': 'Q4892529'}
        self.assertEqual('preferred', AstroItem('test', 'Q1').obtain_claim(v_mag)['rank'])
