from unittest import TestCase
from wdpy import Snak, Statement
from wdpy.connectors import ADS


class TestExtract(TestCase):
    def test_doi_returns_result(self):
        result = ADS.extract(Statement(Snak('P356', ('10.1088/2041-8205/763/1/L1',))))
        self.assertEqual(result.patch[0].mainsnak.property, 'P356')

    def test_bibcode_returns_result(self):
        result = ADS.extract(Statement(Snak('P819', ('2013ApJ...763L...1M',))))
        self.assertEqual(result.patch[0].mainsnak.property, 'P819')

    def test_arxiv_returns_result(self):
        result = ADS.extract(Statement(Snak('P818', ('1212.1162',))))
        self.assertEqual(result.patch[0].mainsnak.property, 'P818')

    def test_nonexistent_bibcode_returns_empty_patch(self):
        result = ADS.extract(Statement(Snak('P819', ('X',))))
        self.assertEqual(result.patch, [])

    def test_nonexistent_doi_returns_none(self):
        self.assertIsNone(ADS.extract(Statement(Snak('P356', ('X',)))))

    def test_nonexistent_arxiv_returns_none(self):
        self.assertIsNone(ADS.extract(Statement(Snak('P818', ('X',)))))
