from unittest import TestCase
from wdpy import Snak, Statement
from wdpy.connectors.orcid import ORCID


class TestExtract(TestCase):
    def test_orcid_returns_result(self):
        result = ORCID.extract(Statement(Snak('P496', ('0000-0001-5109-3700',))))
        self.assertTrue(result.patch)

    def test_nonexistent_orcid_sets_deprecated_patch(self):
        ident = Statement(Snak('P496', ('0000-0000-0000-0000',)))
        result = ORCID.extract(ident)
        self.assertIsNotNone(result.patch)
        deprecated = [s for s in result.patch if s.rank == 'deprecated']
        self.assertTrue(deprecated)
        self.assertIs(deprecated[0].mainsnak, ident.mainsnak)
        self.assertFalse(hasattr(result, 'prior_ident'))
