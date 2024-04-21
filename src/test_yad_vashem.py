from unittest import TestCase, mock

from yad_vashem import Model, Element


class TestYadVashem(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.yv = Element('Succi Luigi (1882 - 1945 )', None)

    def test_obtain_claim_no_qualifiers_for_award(self):
        self.yv.obtain_claim(Model.create_snak('P166', 'Q112197'))
        self.yv.obtain_claim(Model.create_snak('P27', 'Q36'))

    def test_process_sparql_row(self):
        def get_value(new, result):
            key, value = Model.process_sparql_row(new, result)
            self.assertEqual(new[0], key)
            return value

        self.assertEqual('Q5', get_value([9, 'John Doe', 'Q5'], {})['John Doe'])
        self.assertEqual('Q5', get_value([9, 'John Doe', 'Q5'], {9: {'Jane Smith': 'Q1'}})['John Doe'])
        self.assertEqual(6, get_value([9, 'John Doe', 'Q5'], {9: {'John Doe': 5}})['John Doe'])
        self.assertEqual(2, get_value([9, 'John Doe', 'Q5'], {9: {'John Doe': 'Q5'}})['John Doe'])

    @mock.patch('wd.Wikidata.request', return_value=mock.MagicMock(json=lambda: {'d': 0}))
    def test_post(self, _):
        self.assertIn('d', Model.post('BuildQuery'))
        self.assertIn('d', Model.post('GetRighteousList', 0))
        self.assertIn('d', Model.post('GetPersonDetailsBySession', 6658068))
