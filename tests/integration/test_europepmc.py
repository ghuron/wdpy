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

    def test_nonexistent_pmid_sets_deprecated_patch(self):
        ident = Statement(Snak('P698', ('X',)))
        result = EuropePMC.extract(ident)
        self.assertIsNotNone(result.patch)
        deprecated = [s for s in result.patch if s.rank == 'deprecated']
        self.assertTrue(deprecated)
        self.assertIs(deprecated[0].mainsnak, ident.mainsnak)
        self.assertFalse(hasattr(result, 'prior_ident'))

    def test_nonexistent_doi_returns_empty(self):
        result = EuropePMC.extract(Statement(Snak('P356', ('X',))))
        self.assertIsNone(result.patch)
        self.assertFalse(hasattr(result, 'prior_ident'))
