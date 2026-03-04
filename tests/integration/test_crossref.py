from unittest import TestCase
from wdpy import Snak, Statement
from wdpy.connectors.crossref import Crossref


class TestExtract(TestCase):
    def test_doi_returns_result(self):
        result = Crossref.extract(Statement(Snak('P356', ('10.1088/2041-8205/763/1/L1',))))
        self.assertEqual(result.patch[0].mainsnak.property, 'P356')

    def test_nonexistent_doi_returns_empty_patch(self):
        result = Crossref.extract(Statement(Snak('P356', ('X',))))
        self.assertEqual(result.patch, [])
