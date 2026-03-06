from unittest import TestCase
from wdpy import Snak, Statement
from wdpy.connectors import ADS


class TestExtract(TestCase):
    def test_doi_returns_result(self):
        result = ADS.extract(Statement(Snak('P356', ('10.1088/2041-8205/763/1/L1',))))
        self.assertTrue(result.patch)

    def test_bibcode_returns_result(self):
        result = ADS.extract(Statement(Snak('P819', ('2013ApJ...763L...1M',))))
        self.assertTrue(result.patch)

    def test_arxiv_returns_result(self):
        result = ADS.extract(Statement(Snak('P818', ('1212.1162',))))
        self.assertTrue(result.patch)

    def test_nonexistent_bibcode_sets_deprecated_patch(self):
        ident = Statement(Snak('P819', ('X',)))
        result = ADS.extract(ident)
        self.assertIsNotNone(result.patch)
        deprecated = [s for s in result.patch if s.rank == 'deprecated']
        self.assertTrue(deprecated)
        self.assertIs(deprecated[0].mainsnak, ident.mainsnak)
        self.assertFalse(hasattr(result, 'prior_ident'))

    def test_nonexistent_doi_returns_empty(self):
        result = ADS.extract(Statement(Snak('P356', ('X',))))
        self.assertIsNone(result.patch)

    def test_nonexistent_arxiv_returns_empty(self):
        result = ADS.extract(Statement(Snak('P818', ('X',))))
        self.assertIsNone(result.patch)

    def test_bibcode_redirect(self):
        expected = {
            '2023A&A...673A.114H':          ('P819', False),
            '2023arXiv230313424H':          ('P819', True),
            '2303.13424':                   ('P818', False),
            '10.1051/0004-6361/202346285':  ('P356', False),
            '10.48550/arXiv.2303.13424':    ('P356', True),
        }
        result = ADS.extract(Statement(Snak('P819', ('2023arXiv230313424H',))))
        for s in (result.patch or []):
            if Snak.type_of(s.mainsnak.property) != 'external-id':
                continue
            value = (s.mainsnak.value or (None,))[0]
            self.assertIn(value, expected)
            prop, deprecated = expected.pop(value)
            self.assertEqual(s.mainsnak.property, prop, value)
            self.assertEqual(s.rank, 'deprecated' if deprecated else None, value)
        self.assertFalse(expected, f'Missing from patch: {list(expected)}')

    def test_arxiv_only_bibcode(self):
        expected = {
            '2008arXiv0812.5116B':          ('P819', False),
            '0812.5116':                    ('P818', False),
            '10.48550/arXiv.0812.5116':     ('P356', False),
        }
        result = ADS.extract(Statement(Snak('P819', ('2008arXiv0812.5116B',))))
        for s in (result.patch or []):
            if Snak.type_of(s.mainsnak.property) != 'external-id':
                continue
            value = (s.mainsnak.value or (None,))[0]
            self.assertIn(value, expected)
            prop, deprecated = expected.pop(value)
            self.assertEqual(s.mainsnak.property, prop, value)
            self.assertEqual(s.rank, 'deprecated' if deprecated else None, value)
        self.assertFalse(expected, f'Missing from patch: {list(expected)}')
