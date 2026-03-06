from unittest import TestCase
from wdpy import Snak, Statement
from wdpy.connectors.crossref import Crossref


class TestExtract(TestCase):
    def test_doi_returns_result(self):
        result = Crossref.extract(Statement(Snak('P356', ('10.1088/2041-8205/763/1/L1',))))
        self.assertTrue(result.patch)

    def test_nonexistent_doi_sets_deprecated_patch(self):
        ident = Statement(Snak('P356', ('X',)))
        result = Crossref.extract(ident)
        self.assertIsNotNone(result.patch)
        deprecated = [s for s in result.patch if s.rank == 'deprecated']
        self.assertTrue(deprecated)
        self.assertIs(deprecated[0].mainsnak, ident.mainsnak)
        self.assertFalse(hasattr(result, 'prior_ident'))
