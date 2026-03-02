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

    @patch('wdpy.References.compress')
    def test_transform_replaces_property_with_patch(self, compress_mock, *_):
        item = wdpy.Item()
        existing = wdpy.Statement(wdpy.Snak('P31', ('Q1',)))
        item.claims = {'P31': [existing]}
        item._loaded = True
        reference = wdpy.References([{'snaks': {'P248': [{'snaktype': 'value', 'property': 'P248', 'datavalue': {'type': 'wikibase-entityid', 'value': {'id': 'QX'}}}]}}])
        fresh = wdpy.Statement(wdpy.Snak('P31', ('Q2',)), references=reference)
        model = DummyModel(patch=[fresh])

        item.transform(model)

        compress_mock.assert_called_once_with('QX')
        self.assertEqual([fresh], item.claims['P31'])

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
