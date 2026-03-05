from unittest import TestCase
from wdpy import Snak, Statement
from wdpy.connectors import ADS


class TestExtract(TestCase):
    def test_doi_returns_result(self):
        result = ADS.extract(Statement(Snak('P356', ('10.1088/2041-8205/763/1/L1',))))
        self.assertTrue(result.patch)

    def test_bibcode_returns_result(self):
        result = ADS.extract(Statement(Snak('P819', ('2013ApJ...763L...1M',))))
        self.assertTrue(result.patch)

    def test_arxiv_returns_result(self):
        result = ADS.extract(Statement(Snak('P818', ('1212.1162',))))
        self.assertTrue(result.patch)

    def test_nonexistent_bibcode_sets_prior_ident(self):
        result = ADS.extract(Statement(Snak('P819', ('X',))))
        self.assertIsNotNone(result.prior_ident)
        self.assertIsNone(result.patch)
        self.assertIsNone(result.new_ident)

    def test_nonexistent_doi_returns_empty(self):
        result = ADS.extract(Statement(Snak('P356', ('X',))))
        self.assertIsNone(result.prior_ident)
        self.assertIsNone(result.patch)
        self.assertIsNone(result.new_ident)

    def test_nonexistent_arxiv_returns_empty(self):
        result = ADS.extract(Statement(Snak('P818', ('X',))))
        self.assertIsNone(result.prior_ident)
        self.assertIsNone(result.patch)
        self.assertIsNone(result.new_ident)
