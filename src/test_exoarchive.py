from unittest import TestCase

from exoarchive import Model


class TestExoArchive(TestCase):
    def test_resolve_redirects(self):
        Model.redirect = {'K02812.01': [{'pl_name': 'Kepler-1354 b'}]}
        key, _ = Model.resolve_redirects(['id', 'Q1'], None)
        self.assertEqual('id', key)
        key, _ = Model.resolve_redirects(['KOI-2812.01', 'Q1'], None)
        self.assertEqual('Kepler-1354 b', key)
