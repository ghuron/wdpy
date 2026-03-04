import json
import unittest
from unittest.mock import patch

import wdpy


class DummyModel(wdpy.SourceItem):
    @classmethod
    def get_properties(cls):
        return ['P31']

    @classmethod
    def get_db_ref(cls):
        return 'QX'


@patch('wdpy.Snak.type_of', return_value='string')
class ItemTestCase(unittest.TestCase):
    def test_transform_compresses_existing_statements(self, *_):
        item = wdpy.Item()
        # Mock References that becomes empty after compress
        reference = wdpy.References([{'snaks': {'P248': [{'snaktype': 'value', 'property': 'P248', 'datavalue': {'type': 'wikibase-entityid', 'value': {'id': 'QX'}}}]}}])
        statement = wdpy.Statement(
            wdpy.Snak('P31', ('Q5',)), references=reference
        )
        item.claims = {'P31': [statement]}
        item._loaded = True
        
        with patch.object(wdpy.References, 'compress') as compress_mock:
            def side_effect(qid):
                reference._items = []
            compress_mock.side_effect = side_effect
            
            model = DummyModel(patch=[])
            item.transform(model)
            
            compress_mock.assert_called_once_with('QX')
            self.assertIsNone(item.claims['P31'][0].references)


    def test_json_serialization_includes_labels_and_claims(self, *_):
        item = wdpy.Item('Q1')
        item.labels = {'en': 'Name'}
        statement = wdpy.Statement(wdpy.Snak('P31', ('Q5',)))
        item.claims = {'P31': [statement], 'P999': []}

        payload = json.loads(item.json())

        self.assertEqual('Q1', payload['id'])
        self.assertEqual('Name', payload['labels']['en']['value'])
        self.assertIn('P31', payload['claims'])
        self.assertIn('P999', payload['claims'])
        self.assertEqual([], payload['claims']['P999'])

    @patch('wdpy.item.api_write', return_value={'entity': {'id': 'Q9'}})
    def test_write_creates_item(self, api_write_mock, *_):
        item = wdpy.Item()

        result = item.write()

        self.assertEqual('Q9', result)
        self.assertEqual('Q9', item.qid)
        args, kwargs = api_write_mock.call_args
        self.assertEqual('wbeditentity', args[0])
        self.assertEqual('item', kwargs.get('new'))
        self.assertIn('data', kwargs)

class Transform(unittest.TestCase):

    def setUp(self):
        self.item = wdpy.Item('Q1')
        self.item._loaded = True

    def _refs(self, qid='QX'):
        return wdpy.References([{'snaks': {'P248': [{'snaktype': 'value', 'property': 'P248',
                                  'datavalue': {'type': 'wikibase-entityid', 'value': {'id': qid}}}]}}])

    # ── guard conditions ──────────────────────────────────────────────────────

    def test_none_patch_is_no_op(self):
        self.item.claims['P31'] = [wdpy.Statement(wdpy.Snak('P31', ('Q5',)))]
        self.item.transform(DummyModel(patch=None))
        self.assertEqual(len(self.item.claims['P31']), 1)

    def test_non_source_item_is_no_op(self):
        self.item.claims['P31'] = [wdpy.Statement(wdpy.Snak('P31', ('Q5',)))]
        self.item.transform(object())
        self.assertEqual(len(self.item.claims['P31']), 1)

    # ── empty patch (compress-only path) ──────────────────────────────────────

    def test_empty_patch_compresses_existing_statements(self):
        ref = self._refs()
        self.item.claims['P31'] = [wdpy.Statement(wdpy.Snak('P31', ('Q5',)), references=ref)]
        with unittest.mock.patch.object(wdpy.References, 'compress') as m:
            self.item.transform(DummyModel(patch=[]))
        m.assert_called_once_with('QX')

    def test_empty_patch_sets_references_to_none_when_emptied(self):
        ref = self._refs()
        stmt = wdpy.Statement(wdpy.Snak('P31', ('Q5',)), references=ref)
        self.item.claims['P31'] = [stmt]
        with unittest.mock.patch.object(wdpy.References, 'compress',
                                        side_effect=lambda _: ref._items.clear()):
            self.item.transform(DummyModel(patch=[]))
        self.assertIsNone(stmt.references)

    def test_empty_patch_ignores_properties_not_in_model(self):
        ref = self._refs()
        stmt = wdpy.Statement(wdpy.Snak('P21', ('Q6',)), references=ref)
        self.item.claims['P21'] = [stmt]
        with unittest.mock.patch.object(wdpy.References, 'compress') as m:
            self.item.transform(DummyModel(patch=[]))
        m.assert_not_called()

    # ── non-empty patch (merge path) ──────────────────────────────────────────

    def test_new_statement_added_to_item(self):
        self.item.transform(DummyModel(patch=[wdpy.Statement(wdpy.Snak('P31', ('Q5',)))]))
        self.assertEqual(len(self.item.claims['P31']), 1)

    def test_matching_statement_not_duplicated(self):
        existing = wdpy.Statement(wdpy.Snak('P31', ('Q5',)))
        self.item.claims['P31'] = [existing]
        self.item.transform(DummyModel(patch=[wdpy.Statement(wdpy.Snak('P31', ('Q5',)))]))
        self.assertEqual(len(self.item.claims['P31']), 1)

    def test_non_matching_statement_appended_alongside_existing(self):
        existing = wdpy.Statement(wdpy.Snak('P31', ('Q5',)))
        self.item.claims['P31'] = [existing]
        self.item.transform(DummyModel(patch=[wdpy.Statement(wdpy.Snak('P31', ('Q99',)))]))
        self.assertEqual(len(self.item.claims['P31']), 2)

    def test_compress_called_on_affected_property_after_merge(self):
        ref = self._refs()
        existing = wdpy.Statement(wdpy.Snak('P31', ('Q5',)), references=ref)
        self.item.claims['P31'] = [existing]
        with unittest.mock.patch.object(wdpy.References, 'compress') as m:
            self.item.transform(DummyModel(patch=[wdpy.Statement(wdpy.Snak('P31', ('Q5',)))]))
        m.assert_called_once_with('QX')

    def test_compress_not_called_on_unaffected_property(self):
        ref = self._refs()
        self.item.claims['P21'] = [wdpy.Statement(wdpy.Snak('P21', ('Q6',)), references=ref)]
        with unittest.mock.patch.object(wdpy.References, 'compress') as m:
            self.item.transform(DummyModel(patch=[wdpy.Statement(wdpy.Snak('P31', ('Q5',)))]))
        m.assert_not_called()

    def test_compress_sets_references_to_none_when_emptied(self):
        ref = self._refs()
        existing = wdpy.Statement(wdpy.Snak('P31', ('Q5',)), references=ref)
        self.item.claims['P31'] = [existing]
        with unittest.mock.patch.object(wdpy.References, 'compress',
                                        side_effect=lambda _: ref._items.clear()):
            self.item.transform(DummyModel(patch=[wdpy.Statement(wdpy.Snak('P31', ('Q5',)))]))
        self.assertIsNone(existing.references)


class Merge(unittest.TestCase):

    def setUp(self):
        self.item = wdpy.Item('Q1')
        self.item._loaded = True

    def _s(self, val='Q5', prop='P31'):
        return wdpy.Statement(wdpy.Snak(prop, (val,)))

    # ── insert ────────────────────────────────────────────────────────────────

    def test_new_property_bucket_created(self):
        s = self._s()
        self.item.merge(s)
        self.assertIn('P31', self.item.claims)

    def test_new_statement_appended(self):
        s = self._s()
        result = self.item.merge(s)
        self.assertIn(s, self.item.claims['P31'])
        self.assertIs(result, s)

    def test_id_assigned_when_qid_known(self):
        self.item.merge(self._s())
        self.assertTrue(self.item.claims['P31'][0].id.startswith('Q1$'))

    def test_no_id_assigned_without_qid(self):
        self.item.qid = None
        s = self._s()
        self.item.merge(s)
        self.assertIsNone(s.id)

    def test_second_distinct_value_also_inserted(self):
        self.item.merge(self._s('Q5'))
        self.item.merge(self._s('Q6'))
        self.assertEqual(len(self.item.claims['P31']), 2)

    def test_multiple_properties_independent(self):
        self.item.merge(self._s('Q5', 'P31'))
        self.item.merge(self._s('Q5', 'P21'))
        self.assertEqual(len(self.item.claims['P31']), 1)
        self.assertEqual(len(self.item.claims['P21']), 1)

    # ── upsert ────────────────────────────────────────────────────────────────

    def test_duplicate_returns_existing(self):
        existing = self._s()
        self.item.claims['P31'] = [existing]
        result = self.item.merge(self._s())
        self.assertIs(result, existing)
        self.assertEqual(len(self.item.claims['P31']), 1)

    def test_duplicate_does_not_overwrite_existing_id(self):
        existing = self._s()
        existing.id = 'Q1$original'
        self.item.claims['P31'] = [existing]
        self.item.merge(self._s())
        self.assertEqual(self.item.claims['P31'][0].id, 'Q1$original')

    # ── _ensure_loaded called ─────────────────────────────────────────────────

    @patch('wdpy.item.get_entities', return_value={})
    def test_triggers_load_if_not_loaded(self, mock_get):
        item = wdpy.Item('Q99')
        item.merge(self._s())
        mock_get.assert_called_once()

    @patch('wdpy.item.api_write', return_value={'entity': {'id': 'Q7'}})
    def test_write_updates_existing_item(self, api_write_mock, *_):
        item = wdpy.Item('Q7')

        result = item.write()

        self.assertEqual('Q7', result)
        self.assertEqual('Q7', item.qid)
        args, kwargs = api_write_mock.call_args
        self.assertEqual('wbeditentity', args[0])
        self.assertIsNone(kwargs.get('new'))
        self.assertEqual('Q7', kwargs.get('id'))
        self.assertIn('data', kwargs)
