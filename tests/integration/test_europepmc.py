from unittest import TestCase
from wdpy import Snak, Statement
from wdpy.connectors.europepmc import EuropePMC


class TestExtract(TestCase):
    def test_pmid_returns_result(self):
        result = EuropePMC.extract(Statement(Snak('P698', ('23300498',))))
        self.assertEqual(result.patch[0].mainsnak.property, 'P698')

    def test_doi_returns_result(self):
        result = EuropePMC.extract(Statement(Snak('P356', ('10.1038/s41586-021-03819-2',))))
        self.assertEqual(result.patch[0].mainsnak.property, 'P356')

    def test_nonexistent_pmid_returns_none(self):
        self.assertIsNone(EuropePMC.extract(Statement(Snak('P698', ('X',)))))

    def test_nonexistent_doi_returns_empty_patch(self):
        result = EuropePMC.extract(Statement(Snak('P356', ('X',))))
        self.assertEqual(result.patch, [])
