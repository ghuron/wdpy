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
