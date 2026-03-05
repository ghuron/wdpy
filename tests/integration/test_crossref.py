from unittest import TestCase
from wdpy import Snak, Statement
from wdpy.connectors.crossref import Crossref


class TestExtract(TestCase):
    def test_doi_returns_result(self):
        result = Crossref.extract(Statement(Snak('P356', ('10.1088/2041-8205/763/1/L1',))))
        self.assertTrue(result.patch)

    def test_nonexistent_doi_sets_prior_ident(self):
        result = Crossref.extract(Statement(Snak('P356', ('X',))))
        self.assertIsNotNone(result.prior_ident)
        self.assertIsNone(result.patch)
        self.assertIsNone(result.new_ident)
