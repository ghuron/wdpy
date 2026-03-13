import json
from typing import Optional
from unittest import TestCase, mock
from wdpy import Snak, References
from wdpy.references import _REDIRECTS, _PUB_DATES, _update_redirect, _update_pub_date

@mock.patch('wdpy.Snak.type_of', return_value='string')
class Json(TestCase):
    def test_empty(self, *_):
        self.assertEqual(References([]).json(), '[]')

    def test_single(self, *_):
        refs = References([{'snaks': {'P433': [Snak('P433', ('1',))]}}])
        expected = [{'snaks': {'P433': [{'datatype': 'string', 'snaktype': 'value',
                     'property': 'P433', 'datavalue': {'type': 'string', 'value': '1'}}]}}]
        self.assertEqual(json.loads(refs.json()), expected)

    def test_multiple(self, *_):
        refs = References([{'snaks': {'P433': [Snak('P433', ('1',))]}},
                           {'snaks': {'P304': [Snak('P304', ('10',))]}}])
        expected = [{'snaks': {'P433': [{'datatype': 'string', 'snaktype': 'value',
                     'property': 'P433', 'datavalue': {'type': 'string', 'value': '1'}}]}},
                    {'snaks': {'P304': [{'datatype': 'string', 'snaktype': 'value',
                     'property': 'P304', 'datavalue': {'type': 'string', 'value': '10'}}]}}]
        self.assertEqual(json.loads(refs.json()), expected)

    def test_sort_keys(self, *_):
        s = References([{'snaks': {'P433': [Snak('P433', ('1',))],
                                   'P304': [Snak('P304', ('10',))]}}]).json()
        self.assertLess(s.find('"P304"'), s.find('"P433"'))

    def test_filters_empty(self, *_):
        refs = References([{'snaks': {'P433': [Snak('P433', ('1',))], 'P304': []}}])
        expected = [{'snaks': {'P433': [{'datatype': 'string', 'snaktype': 'value',
                     'property': 'P433', 'datavalue': {'type': 'string', 'value': '1'}}]}}]
        self.assertEqual(json.loads(refs.json()), expected)

class Compress(TestCase):
    def _ref_snaks(self, qid='QX'):
        return {'P248': [Snak('P248', (qid,))]}

    @mock.patch('wdpy.references._preload')
    @mock.patch.object(Snak, 'resolve_redirect')
    def test_non_matching_item_kept(self, *_):
        refs = References([
            {'snaks': {'P248': [{'snaktype': 'value', 'property': 'P248',
                                 'datavalue': {'type': 'wikibase-entityid', 'value': {'id': 'QY'}}}]}}
        ])
        result = refs.compress(self._ref_snaks('QX'))
        self.assertFalse(result)
        self.assertEqual(len(refs._items), 1)

    @mock.patch('wdpy.references._preload')
    @mock.patch.object(Snak, 'resolve_redirect')
    def test_stale_matching_item_deleted(self, *_):
        refs = References([
            {'snaks': {'P248': [{'snaktype': 'value', 'property': 'P248',
                                 'datavalue': {'type': 'wikibase-entityid', 'value': {'id': 'QX'}}}]}}
        ])
        result = refs.compress(self._ref_snaks('QX'))
        self.assertTrue(result)
        self.assertEqual(len(refs._items), 0)

    @mock.patch('wdpy.references._preload')
    @mock.patch.object(Snak, 'resolve_redirect')
    @mock.patch.object(Snak, 'retrieved', return_value=Snak('P813', ('2026-03-13',)))
    def test_fresh_matching_item_loses_p813(self, *_):
        today = ('2026-03-13',)
        refs = References([
            {'snaks': {'P248': [{'snaktype': 'value', 'property': 'P248',
                                 'datavalue': {'type': 'wikibase-entityid', 'value': {'id': 'QX'}}}],
                       'P813': [{'snaktype': 'value', 'property': 'P813',
                                 'datavalue': {'type': 'string', 'value': '2026-03-13'}}]}}
        ])
        result = refs.compress(self._ref_snaks('QX'))
        self.assertFalse(result)
        self.assertEqual(len(refs._items), 1)
        self.assertFalse(any(s.property == 'P813' for s in refs._items[0]))

    def test_returns_false_on_empty_ref_snaks(self):
        refs = References([{'snaks': {'P433': [Snak('P433', ('1',))]}}])
        self.assertFalse(refs.compress({}))

    def test_returns_false_when_already_empty(self):
        self.assertFalse(References([]).compress(self._ref_snaks()))


class Include(TestCase):
    @mock.patch('wdpy.references._preload')
    @mock.patch.object(Snak, 'try_to_merge_references', return_value=True)
    def test_merges(self, mock: mock.MagicMock, *_):
        refs = References([{'snaks': {}}])
        refs.upsert({'P248': [Snak('P248', ('Q1',))]})
        mock.assert_called_once()
        self.assertEqual(len(refs._items), 1)
        self.assertTrue(any(s.property == 'P813' for s in refs._items[0]))

    @mock.patch('wdpy.references._preload')
    @mock.patch.object(Snak, 'try_to_merge_references', return_value=False)
    def test_appends(self, mock: mock.MagicMock, *_):
        refs = References([{'snaks': {}}])
        refs.upsert({'P248': [Snak('P248', ('Q1',))]})
        mock.assert_called_once()
        self.assertEqual(len(refs._items), 2)
        self.assertTrue(any(s.property == 'P813' for s in refs._items[1]))

    @mock.patch('wdpy.references._preload')
    def test_preload_all(self, mock: mock.MagicMock):
        refs = References([{'snaks': {'P248': [{'snaktype': 'value', 'property': 'P248', 'datavalue': {'type': 'wikibase-entityid', 'value': {'id': 'Q1'}}}]}}])
        refs.upsert({'P248': [Snak('P248', ('Q2',))]})
        # Check that it preloads both Q2 (new) and Q1 (existing)
        # The first argument to _preload is the list of items
        items = mock.call_args[0][0]
        self.assertEqual(len(items), 2)
        qids = {s.value[0] for it in items for s in it if s.property == 'P248' and s.value}
        self.assertEqual(qids, {'Q1', 'Q2'})

class UpdateRedirect(TestCase):
    def setUp(self): _REDIRECTS.clear()

    def test_basic(self):
        _update_redirect({'Q1': {'id': 'Q1', 'redirects': {'from': 'Q2', 'to': 'Q1'}}})
        self.assertEqual((_REDIRECTS.get('Q2'), _REDIRECTS.get('Q1')), ('Q1', 'Q1'))

    def test_canonical(self):
        _update_redirect({'Q1': {'id': 'Q1'}})
        self.assertEqual(_REDIRECTS.get('Q1'), 'Q1')

    def test_missing_id(self):
        _update_redirect({'Q1': {}})
        self.assertEqual(_REDIRECTS.get('Q1'), 'Q1')

    def test_no_to(self):
        _update_redirect({'Q1': {'id': 'Q1', 'redirects': {'from': 'Q2'}}})
        self.assertEqual((_REDIRECTS.get('Q2'), _REDIRECTS.get('Q1')), ('Q1', 'Q1'))

    def test_multiple(self):
        _update_redirect({'Q1': {'id': 'Q1'},
                          'Q3': {'id': 'Q3', 'redirects': {'from': 'Q4', 'to': 'Q3'}}})
        self.assertEqual((_REDIRECTS.get('Q1'), _REDIRECTS.get('Q3'), _REDIRECTS.get('Q4')),
                         ('Q1', 'Q3', 'Q3'))

class UpdatePubDate(TestCase):
    def setUp(self): _PUB_DATES.clear()

    def test_basic(self):
        _update_pub_date({'Q1': {'id': 'Q1', 'claims': {'P577': [{'mainsnak': {
            'datavalue': {'value': {'time': '+2024-05-26T00:00:00Z'}}}}]}}})
        self.assertEqual(_PUB_DATES.get('Q1'), 20240526)

    def test_missing(self):
        _update_pub_date({'Q1': {'id': 'Q1', 'claims': {}}})
        self.assertNotIn('Q1', _PUB_DATES)

    def test_invalid(self):
        _update_pub_date({'Q1': {'id': 'Q1', 'claims': {'P577': [{'mainsnak': {
            'datavalue': {'value': {'time': 'invalid'}}}}]}}})
        self.assertNotIn('Q1', _PUB_DATES)

    def test_redirect(self):
        _update_pub_date({'Q1': {'id': 'Q1', 'redirects': {'from': 'Q2'}, 'claims': {'P577': [
            {'mainsnak': {'datavalue': {'value': {'time': '+2024-05-26T00:00:00Z'}}}}]}}})
        self.assertEqual((_PUB_DATES.get('Q1'), _PUB_DATES.get('Q2')), (20240526, 20240526))

class PublicationDate(TestCase):
    def setUp(self):
        _PUB_DATES.clear()
        _REDIRECTS.clear()

    def test_empty(self):
        self.assertIsNone(References([]).publication_date)

    @mock.patch('wdpy.references.get_entities', return_value={'Q1': {'id': 'Q1'}})
    def test_none(self, *_):
        self.assertIsNone(References([{'snaks': {'P248': [{'snaktype': 'value', 'property': 'P248',
                          'datavalue': {'type': 'wikibase-entityid', 'value': {'id': 'Q1'}}}]}}]).publication_date)

    @mock.patch('wdpy.references.get_entities', return_value={
        'Q1': {'id': 'Q1', 'claims': {'P577': [{'mainsnak': {'datavalue': {'value': {'time': '+2020-01-01T00:00:00Z'}}}}]}},
        'Q2': {'id': 'Q2', 'claims': {'P577': [{'mainsnak': {'datavalue': {'value': {'time': '+2022-12-31T00:00:00Z'}}}}]}}
    })
    def test_max(self, *_):
        self.assertEqual(References([
            {'snaks': {'P248': [{'snaktype': 'value', 'property': 'P248', 'datavalue': {'type': 'wikibase-entityid', 'value': {'id': 'Q1'}}}]}},
            {'snaks': {'P248': [{'snaktype': 'value', 'property': 'P248', 'datavalue': {'type': 'wikibase-entityid', 'value': {'id': 'Q2'}}}]}}
        ]).publication_date, '20221231')

    @mock.patch('wdpy.references.get_entities')
    def test_cache(self, mock: Optional[mock.MagicMock] = None):
        _PUB_DATES['Q1'] = 20210615
        self.assertEqual(References([{'snaks': {'P248': [{'snaktype': 'value', 'property': 'P248',
                         'datavalue': {'type': 'wikibase-entityid', 'value': {'id': 'Q1'}}}]}}]).publication_date, '20210615')
        if mock: mock.assert_not_called()
