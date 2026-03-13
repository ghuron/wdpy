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

        result = item.write('test summary')

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

    # ── empty patch ───────────────────────────────────────────────────────────

    def test_empty_patch_is_no_op(self):
        existing = wdpy.Statement(wdpy.Snak('P31', ('Q5',)))
        self.item.claims['P31'] = [existing]
        with unittest.mock.patch.object(wdpy.References, 'compress') as m:
            self.item.transform(DummyModel(patch=[]))
        m.assert_not_called()
        self.assertIn(existing, self.item.claims['P31'])

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
        with unittest.mock.patch.object(wdpy.References, 'compress', return_value=False) as m:
            self.item.transform(DummyModel(patch=[wdpy.Statement(wdpy.Snak('P31', ('Q5',)))]))
        m.assert_called_once()
        (ref_snaks,), _ = m.call_args
        self.assertEqual(ref_snaks['P248'][0].value, ('QX',))

    def test_compress_not_called_on_unaffected_property(self):
        ref = self._refs()
        self.item.claims['P21'] = [wdpy.Statement(wdpy.Snak('P21', ('Q6',)), references=ref)]
        with unittest.mock.patch.object(wdpy.References, 'compress') as m:
            self.item.transform(DummyModel(patch=[wdpy.Statement(wdpy.Snak('P31', ('Q5',)))]))
        m.assert_not_called()

    def test_compress_deletes_statement_when_references_emptied(self):
        ref = self._refs()
        existing = wdpy.Statement(wdpy.Snak('P31', ('Q5',)), references=ref)
        self.item.claims['P31'] = [existing]
        with unittest.mock.patch.object(wdpy.References, 'compress', return_value=True):
            self.item.transform(DummyModel(patch=[wdpy.Statement(wdpy.Snak('P31', ('Q5',)))]))
        self.assertNotIn(existing, self.item.claims['P31'])

    # ── rank propagation via patch ────────────────────────────────────────────

    def test_deprecated_rank_in_patch_propagates_to_existing(self):
        existing = wdpy.Statement(wdpy.Snak('P31', ('Q5',)), rank='normal')
        self.item.claims['P31'] = [existing]
        patch_stmt = wdpy.Statement(wdpy.Snak('P31', ('Q5',)), rank='deprecated')
        self.item.transform(DummyModel(patch=[patch_stmt]))
        self.assertEqual(existing.rank, 'deprecated')

    def test_new_ident_via_patch_inserts_canonical_and_deprecates_old(self):
        old = wdpy.Statement(wdpy.Snak('P31', ('Q5',)), rank='normal')
        self.item.claims['P31'] = [old]
        deprecated_old = wdpy.Statement(wdpy.Snak('P31', ('Q5',)), rank='deprecated')
        new_canonical = wdpy.Statement(wdpy.Snak('P31', ('Q99',)), rank='normal')
        self.item.transform(DummyModel(patch=[new_canonical, deprecated_old]))
        self.assertEqual(old.rank, 'deprecated')
        self.assertEqual(len(self.item.claims['P31']), 2)
        values = {s.mainsnak.value for s in self.item.claims['P31']}
        self.assertIn(('Q99',), values)


class DeleteClaim(unittest.TestCase):

    def setUp(self):
        self.item = wdpy.Item('Q1')
        self.item._loaded = True

    def _stmt(self, prop='P31', val='Q5', stmt_id=None, snak_hash=None):
        s = wdpy.Statement(wdpy.Snak(prop, (val,)))
        s.id = stmt_id
        s.mainsnak.hash = snak_hash
        return s

    def test_unsaved_statement_removed_from_claims_no_removal_queued(self):
        stmt = self._stmt(snak_hash=None)
        self.item.claims['P31'] = [stmt]
        self.item.delete_claim(stmt)
        self.assertEqual(self.item.claims['P31'], [])
        self.assertEqual(self.item._removals, [])

    def test_persisted_statement_removed_and_queued(self):
        stmt = self._stmt(stmt_id='Q1$abc-123', snak_hash='abc123')
        self.item.claims['P31'] = [stmt]
        self.item.delete_claim(stmt)
        self.assertEqual(self.item.claims['P31'], [])
        self.assertEqual(self.item._removals, [('P31', 'Q1$abc-123')])

    def test_unknown_property_does_not_raise(self):
        stmt = self._stmt(prop='P999', stmt_id='Q1$xyz', snak_hash='xyz')
        # P999 not in claims at all
        self.item.delete_claim(stmt)  # should not raise
        self.assertEqual(self.item._removals, [('P999', 'Q1$xyz')])

    def test_json_includes_removal_entry(self):
        stmt = self._stmt(stmt_id='Q1$abc-123')
        self.item.claims = {}
        self.item._removals = [('P31', 'Q1$abc-123')]
        payload = json.loads(self.item.json())
        self.assertIn('P31', payload['claims'])
        removal = payload['claims']['P31'][0]
        self.assertEqual(removal['id'], 'Q1$abc-123')
        self.assertEqual(removal['remove'], '')


@patch('wdpy.Snak.type_of', return_value='external-id')
class Postprocess(unittest.TestCase):

    def setUp(self):
        self.item = wdpy.Item('Q1')
        self.item._loaded = True

    def _ref(self, *props):
        """Build a References with one item containing snaks for each prop."""
        return wdpy.References([{'snaks': {p: [{'snaktype': 'value', 'property': p,
            'datavalue': {'type': 'string', 'value': 'x'}}] for p in props}}])

    def test_strips_external_id_snak_from_references(self, *_):
        ref = self._ref('P819', 'P248')
        stmt = wdpy.Statement(wdpy.Snak('P31', ('Q5',)), references=ref)
        self.item.claims = {
            'P819': [wdpy.Statement(wdpy.Snak('P819', ('abc',)))],
            'P31': [stmt],
        }
        self.item.postprocess()
        props = {s.property for s in stmt.references._items[0]}
        self.assertNotIn('P819', props)
        self.assertIn('P248', props)

    def test_does_not_strip_when_multiple_non_deprecated(self, *_):
        ref = self._ref('P819')
        stmt = wdpy.Statement(wdpy.Snak('P31', ('Q5',)), references=ref)
        self.item.claims = {
            'P819': [wdpy.Statement(wdpy.Snak('P819', ('abc',))),
                     wdpy.Statement(wdpy.Snak('P819', ('def',)))],
            'P31': [stmt],
        }
        self.item.postprocess()
        props = {s.property for s in stmt.references._items[0]}
        self.assertIn('P819', props)

    def test_does_not_strip_when_only_deprecated(self, *_):
        ref = self._ref('P819')
        stmt = wdpy.Statement(wdpy.Snak('P31', ('Q5',)), references=ref)
        self.item.claims = {
            'P819': [wdpy.Statement(wdpy.Snak('P819', ('abc',)), rank='deprecated')],
            'P31': [stmt],
        }
        self.item.postprocess()
        props = {s.property for s in stmt.references._items[0]}
        self.assertIn('P819', props)

    def test_empty_ref_item_removed_after_strip(self, *_):
        ref = self._ref('P819')  # ref item contains only P819
        stmt = wdpy.Statement(wdpy.Snak('P31', ('Q5',)), references=ref)
        self.item.claims = {
            'P819': [wdpy.Statement(wdpy.Snak('P819', ('abc',)))],
            'P31': [stmt],
        }
        self.item.postprocess()
        self.assertEqual(stmt.references._items, [])


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

        result = item.write('test summary')

        self.assertEqual('Q7', result)
        self.assertEqual('Q7', item.qid)
        args, kwargs = api_write_mock.call_args
        self.assertEqual('wbeditentity', args[0])
        self.assertIsNone(kwargs.get('new'))
        self.assertEqual('Q7', kwargs.get('id'))
        self.assertIn('data', kwargs)
