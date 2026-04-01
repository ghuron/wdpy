from unittest import TestCase
from wdpy import Snak, Statement
from wdpy.connectors.crossref import Crossref


class TestExtract(TestCase):
    def test_doi_returns_result(self):
        result = Crossref.extract(Statement(Snak('P356', ('10.1088/2041-8205/763/1/L1',))))
        self.assertTrue(result.patch)

    def test_published_online_maps_to_p577(self):
        result = Crossref.extract(Statement(Snak('P356', ('10.1088/2041-8205/763/1/L1',))))
        p577 = [s for s in (result.patch or []) if s.mainsnak.property == 'P577']
        self.assertTrue(p577, 'expected P577 from published-online')

    def test_nonexistent_doi_sets_deprecated_patch(self):
        ident = Statement(Snak('P356', ('X',)))
        result = Crossref.extract(ident)
        self.assertIsNotNone(result.patch)
        deprecated = [s for s in result.patch if s.rank == 'deprecated']
        self.assertTrue(deprecated)
        self.assertIs(deprecated[0].mainsnak, ident.mainsnak)
        self.assertFalse(hasattr(result, 'prior_ident'))

    def test_author_with_orcid_maps_to_p50(self):
        # 10.3847/2041-8213/AD037D has author David Watanabe with ORCID 0000-0002-3555-8464
        result = Crossref.extract(Statement(Snak('P356', ('10.3847/2041-8213/AD037D',))))
        self.assertIsNotNone(result.patch)
        p50 = [s for s in (result.patch or []) if s.mainsnak.property == 'P50']
        self.assertTrue(p50, 'expected P50 author from ORCID lookup')
