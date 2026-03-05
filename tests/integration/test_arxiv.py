from unittest import TestCase

from wdpy import Snak, Statement
from wdpy.connectors import ArxivItem


class TestExtract(TestCase):
    def test_arxiv_id_returns_result(self):
        result = ArxivItem.extract(Statement(Snak('P818', ('1309.0951',))))
        self.assertTrue(result.patch)

    def test_doi_returns_result(self):
        result = ArxivItem.extract(Statement(Snak('P356', ('10.4171/161',))))
        self.assertTrue(result.patch)

    def test_nonexistent_arxiv_id_sets_prior_ident(self):
        result = ArxivItem.extract(Statement(Snak('P818', ('X',))))
        self.assertIsNotNone(result.prior_ident)
        self.assertIsNone(result.patch)
        self.assertIsNone(result.new_ident)

    def test_nonexistent_doi_returns_empty(self):
        result = ArxivItem.extract(Statement(Snak('P356', ('10.99999/nonexistent',))))
        self.assertIsNone(result.prior_ident)
        self.assertIsNone(result.patch)
        self.assertIsNone(result.new_ident)
