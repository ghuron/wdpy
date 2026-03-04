from unittest import TestCase

from wdpy import Snak, Statement
from wdpy.connectors import ArxivItem


class TestExtract(TestCase):
    def test_arxiv_id_returns_result(self):
        result = ArxivItem.extract(Statement(Snak('P818', ('1309.0951',))))
        self.assertEqual(result.patch[0].mainsnak.property, 'P818')

    def test_doi_returns_result(self):
        result = ArxivItem.extract(Statement(Snak('P356', ('10.4171/161',))))
        self.assertEqual(result.patch[0].mainsnak.property, 'P356')

    def test_nonexistent_arxiv_id_returns_none(self):
        self.assertIsNone(ArxivItem.extract(Statement(Snak('P818', ('X',)))))

    def test_nonexistent_doi_returns_empty_patch(self):
        result = ArxivItem.extract(Statement(Snak('P356', ('10.99999/nonexistent',))))
        self.assertEqual(result.patch, [])
