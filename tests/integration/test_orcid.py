from unittest import TestCase
from wdpy import Snak, Statement
from wdpy.connectors.orcid import ORCID


class TestExtract(TestCase):
    def test_orcid_returns_result(self):
        result = ORCID.extract(Statement(Snak('P496', ('0000-0001-5109-3700',))))
        self.assertEqual(result.patch[0].mainsnak.property, 'P496')

    def test_nonexistent_orcid_returns_empty_patch(self):
        result = ORCID.extract(Statement(Snak('P496', ('0000-0000-0000-0000',))))
        self.assertEqual(result.patch, [])
