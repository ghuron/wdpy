from unittest import TestCase
from wdpy import Snak, Statement
from wdpy.connectors.orcid import ORCID


class TestExtract(TestCase):
    def test_orcid_returns_result(self):
        result = ORCID.extract(Statement(Snak('P496', ('0000-0001-5109-3700',))))
        self.assertTrue(result.patch)

    def test_nonexistent_orcid_sets_prior_ident(self):
        result = ORCID.extract(Statement(Snak('P496', ('0000-0000-0000-0000',))))
        self.assertIsNotNone(result.prior_ident)
        self.assertIsNone(result.patch)
        self.assertIsNone(result.new_ident)
