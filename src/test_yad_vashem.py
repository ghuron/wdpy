from unittest import TestCase

from yad_vashem import Model


class TestYadVashem(TestCase):
    def test_process_sparql_row(self):
        def get_value(new, result):
            key, value = Model.process_sparql_row(new, result)
            self.assertEqual(new[0], key)
            return value

        self.assertEqual('Q5', get_value([9, 'John Doe', 'Q5'], {})['John Doe'])
        self.assertEqual('Q5', get_value([9, 'John Doe', 'Q5'], {9: {'Jane Smith': 'Q1'}})['John Doe'])
        self.assertEqual(6, get_value([9, 'John Doe', 'Q5'], {9: {'John Doe': 5}})['John Doe'])
        self.assertEqual(2, get_value([9, 'John Doe', 'Q5'], {9: {'John Doe': 'Q5'}})['John Doe'])
