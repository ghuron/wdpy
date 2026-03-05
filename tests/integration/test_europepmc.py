from unittest import TestCase
from wdpy import Snak, Statement
from wdpy.connectors.europepmc import EuropePMC


class TestExtract(TestCase):
    def test_pmid_returns_result(self):
        result = EuropePMC.extract(Statement(Snak('P698', ('23300498',))))
        self.assertTrue(result.patch)

    def test_doi_returns_result(self):
        result = EuropePMC.extract(Statement(Snak('P356', ('10.1038/s41586-021-03819-2',))))
        self.assertTrue(result.patch)

    def test_nonexistent_pmid_sets_prior_ident(self):
        result = EuropePMC.extract(Statement(Snak('P698', ('X',))))
        self.assertIsNotNone(result.prior_ident)
        self.assertIsNone(result.patch)
        self.assertIsNone(result.new_ident)

    def test_nonexistent_doi_returns_empty(self):
        result = EuropePMC.extract(Statement(Snak('P356', ('X',))))
        self.assertIsNone(result.prior_ident)
        self.assertIsNone(result.patch)
        self.assertIsNone(result.new_ident)
