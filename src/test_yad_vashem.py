from unittest import TestCase

from yad_vashem import YadVashem


class TestYadVashem(TestCase):
    @classmethod
    def setUpClass(cls):
        yv = YadVashem('10061418', 'Succi Luigi (1882 - 1945 )')
        yv.entity = {'claims': {}}
        cls.yv = yv

    def test_obtain_claim_no_qualifiers_for_award(self):
        self.yv.obtain_claim(self.yv.create_snak('P166', 'Q112197'))
        self.yv.obtain_claim(self.yv.create_snak('P27', 'Q36'))

    def test_process_sparql_row(self):
        def get_value(new, result):
            key, value = YadVashem.process_sparql_row(new, result)
            self.assertEqual(new[0], key)
            return value

        self.assertEqual('Q5', get_value([9, 'John Doe', 'Q5'], {})['John Doe'])
        self.assertEqual('Q5', get_value([9, 'John Doe', 'Q5'], {9: {'Jane Smith': 'Q1'}})['John Doe'])
        self.assertEqual(6, get_value([9, 'John Doe', 'Q5'], {9: {'John Doe': 5}})['John Doe'])
        self.assertEqual(2, get_value([9, 'John Doe', 'Q5'], {9: {'John Doe': 'Q5'}})['John Doe'])
