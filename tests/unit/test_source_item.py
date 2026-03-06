import json
import sys
from unittest import TestCase, mock

import wdpy
from wdpy import SourceItem, Statement, Snak


class TestRequest(TestCase):
    @mock.patch('wdpy.source_item.build_opener')
    def test_success(self, build_mock):
        resp_mock = mock.MagicMock(read=lambda: b'ok', geturl=lambda: 'url', getcode=lambda: 200)
        build_mock.return_value.open.return_value = resp_mock
        r = wdpy.request('http://ex.com')
        self.assertEqual(r.read(), b'ok')
        self.assertEqual(r.geturl(), 'url')

    @mock.patch('wdpy.source_item.build_opener')
    def test_error(self, build_mock):
        build_mock.return_value.open.side_effect = Exception('boom')
        self.assertIsNone(wdpy.request('http://ex.com'))

class TestConfig(TestCase):
    def test_load_config_success(self):
        mock_module = mock.MagicMock()
        mock_module.__file__ = '/path/to/module.py'
        
        with mock.patch.dict(sys.modules, {'test_module': mock_module}):
            with mock.patch('wdpy.source_item.Path') as MockPath:
                mock_json_path = MockPath.return_value.with_suffix.return_value
                mock_json_path.exists.return_value = True
                
                config_data = {'key': 'value'}
                with mock.patch('builtins.open', mock.mock_open(read_data=json.dumps(config_data))):
                    result = SourceItem._load_config('test_module')
                    self.assertEqual(result, config_data)

    def test_load_config_no_file(self):
        mock_module = mock.MagicMock()
        mock_module.__file__ = '/path/to/module.py'
        
        with mock.patch.dict(sys.modules, {'test_module': mock_module}):
            with mock.patch('wdpy.source_item.Path') as MockPath:
                mock_json_path = MockPath.return_value.with_suffix.return_value
                mock_json_path.exists.return_value = False
                
                result = SourceItem._load_config('test_module')
                self.assertEqual(result, {})

    def test_merging(self):
        class Base(SourceItem):
            _config = {'base_key': 'base_val', 'common_key': 'base_common'}
        
        with mock.patch.object(SourceItem, '_load_config') as mock_load:
            mock_load.return_value = {'derived_key': 'derived_val', 'common_key': 'derived_common'}
            class Derived(Base):
                pass
            
        expected_config = {
            'base_key': 'base_val',
            'derived_key': 'derived_val',
            'common_key': 'derived_common'
        }
        
        self.assertEqual(Derived._config, expected_config)
        self.assertIsNot(Derived._config, Base._config)
        self.assertEqual(Base._config['common_key'], 'base_common')

    def test_no_child_json(self):
        class Base(SourceItem):
            _config = {'a': 1}

        with mock.patch.object(SourceItem, '_load_config', return_value={}):
            class Derived(Base):
                pass

        self.assertEqual(Derived._config, {'a': 1})
        self.assertIsNot(Derived._config, Base._config)


class TestLookup(TestCase):
    def setUp(self):
        SourceItem._lookup_cache.clear()

    @mock.patch('wdpy.source_item.haswbstatement')
    def test_hit_returns_qid(self, mock_hws):
        mock_hws.return_value = ['Q42']
        self.assertEqual(SourceItem.lookup('P31', 'some-id'), 'Q42')

    @mock.patch('wdpy.source_item.haswbstatement')
    def test_miss_returns_none(self, mock_hws):
        mock_hws.return_value = None
        self.assertIsNone(SourceItem.lookup('P31', 'unknown'))

    @mock.patch('wdpy.source_item.haswbstatement')
    def test_second_call_uses_cache(self, mock_hws):
        mock_hws.return_value = ['Q42']
        SourceItem.lookup('P31', 'some-id')
        SourceItem.lookup('P31', 'some-id')
        mock_hws.assert_called_once()

    @mock.patch('wdpy.source_item.haswbstatement')
    def test_negative_result_cached(self, mock_hws):
        mock_hws.return_value = None
        SourceItem.lookup('P31', 'unknown')
        SourceItem.lookup('P31', 'unknown')
        mock_hws.assert_called_once()

    @mock.patch('wdpy.source_item.haswbstatement')
    def test_different_ids_queried_separately(self, mock_hws):
        mock_hws.side_effect = [['Q1'], ['Q2']]
        self.assertEqual(SourceItem.lookup('P31', 'id-1'), 'Q1')
        self.assertEqual(SourceItem.lookup('P31', 'id-2'), 'Q2')
        self.assertEqual(mock_hws.call_count, 2)

    @mock.patch('wdpy.source_item.haswbstatement')
    def test_different_properties_queried_separately(self, mock_hws):
        mock_hws.side_effect = [['Q1'], ['Q2']]
        SourceItem.lookup('P31', 'same-id')
        SourceItem.lookup('P496', 'same-id')
        self.assertEqual(mock_hws.call_count, 2)

    @mock.patch('wdpy.source_item.haswbstatement')
    def test_multiple_results_warns_and_returns_first(self, mock_hws):
        mock_hws.return_value = ['Q1', 'Q2', 'Q3']
        with self.assertLogs('root', level='WARNING') as cm:
            result = SourceItem.lookup('P31', 'dup-id')
        self.assertEqual(result, 'Q1')
        self.assertTrue(any('3 instances' in line for line in cm.output))


def _ext_id_statement(property_id: str, value: str, qid: str) -> Statement:
    """Build a minimal external-id Statement with a GUID-style id."""
    return Statement(mainsnak=Snak(property_id, (value,)), id=f'{qid}$abc-def')


class TestRegisterNewItem(TestCase):
    def setUp(self):
        SourceItem._lookup_cache.clear()

    def _register(self, statements, expected_qid=None, expected_value=None,
                  expected_property=None):
        """Helper: patch Snak.type_of to return 'external-id' and call register."""
        with mock.patch.object(Snak, 'type_of', return_value='external-id'):
            SourceItem.register_new_item(statements)

    # --- happy path ---

    def test_registers_qid_after_lookup_miss(self):
        """Normal flow: lookup recorded None, item created, cache updated."""
        SourceItem._lookup_cache['P496'] = {'0000-0001': None}
        s = _ext_id_statement('P496', '0000-0001', 'Q42')
        with mock.patch.object(Snak, 'type_of', return_value='external-id'):
            SourceItem.register_new_item([s])
        self.assertEqual(SourceItem._lookup_cache['P496']['0000-0001'], 'Q42')

    def test_qid_extracted_from_statement_id(self):
        """QID is the part of statement.id before '$'."""
        SourceItem._lookup_cache['P496'] = {'orcid-1': None}
        s = Statement(mainsnak=Snak('P496', ('orcid-1',)), id='Q999$some-guid')
        with mock.patch.object(Snak, 'type_of', return_value='external-id'):
            SourceItem.register_new_item([s])
        self.assertEqual(SourceItem._lookup_cache['P496']['orcid-1'], 'Q999')

    def test_multiple_external_id_statements(self):
        """All external-id statements in the list are registered."""
        SourceItem._lookup_cache['P496'] = {'id-a': None}
        SourceItem._lookup_cache['P213'] = {'isni-b': None}
        statements = [
            _ext_id_statement('P496', 'id-a', 'Q10'),
            _ext_id_statement('P213', 'isni-b', 'Q10'),
        ]
        with mock.patch.object(Snak, 'type_of', return_value='external-id'):
            SourceItem.register_new_item(statements)
        self.assertEqual(SourceItem._lookup_cache['P496']['id-a'], 'Q10')
        self.assertEqual(SourceItem._lookup_cache['P213']['isni-b'], 'Q10')

    # --- error conditions ---

    def test_property_not_in_cache_logs_error_and_registers(self):
        """Property never looked up: logs error but still writes to cache."""
        s = _ext_id_statement('P496', 'new-id', 'Q7')
        with mock.patch.object(Snak, 'type_of', return_value='external-id'):
            with self.assertLogs('root', level='ERROR') as cm:
                SourceItem.register_new_item([s])
        self.assertTrue(any('No lookup performed for P496' in line for line in cm.output))
        self.assertEqual(SourceItem._lookup_cache['P496']['new-id'], 'Q7')

    def test_value_not_in_cache_logs_error_and_registers(self):
        """Property in cache but value never looked up: logs error, writes to cache."""
        SourceItem._lookup_cache['P496'] = {}  # property known, but value absent
        s = _ext_id_statement('P496', 'unseen-id', 'Q8')
        with mock.patch.object(Snak, 'type_of', return_value='external-id'):
            with self.assertLogs('root', level='ERROR') as cm:
                SourceItem.register_new_item([s])
        self.assertTrue(any('No lookup performed for P496:unseen-id' in line for line in cm.output))
        self.assertEqual(SourceItem._lookup_cache['P496']['unseen-id'], 'Q8')

    def test_duplicate_logs_error_and_overwrites(self):
        """Cached value already non-None: logs duplicate error, still updates cache."""
        SourceItem._lookup_cache['P496'] = {'dup-id': 'Q1'}
        s = _ext_id_statement('P496', 'dup-id', 'Q2')
        with mock.patch.object(Snak, 'type_of', return_value='external-id'):
            with self.assertLogs('root', level='ERROR') as cm:
                SourceItem.register_new_item([s])
        self.assertTrue(any('Duplicate discovered' in line for line in cm.output))
        self.assertEqual(SourceItem._lookup_cache['P496']['dup-id'], 'Q2')

    # --- filtering ---

    def test_non_external_id_statement_is_skipped(self):
        """Statements whose property type is not external-id are ignored."""
        s = Statement(mainsnak=Snak('P31', ('Q5',)), id='Q42$abc')
        with mock.patch.object(Snak, 'type_of', return_value='wikibase-item'):
            SourceItem.register_new_item([s])
        self.assertEqual(SourceItem._lookup_cache, {})

    def test_statement_without_id_is_skipped(self):
        """Statement with no id (not yet saved) is silently ignored."""
        s = Statement(mainsnak=Snak('P496', ('some-val',)), id=None)
        with mock.patch.object(Snak, 'type_of', return_value='external-id'):
            SourceItem.register_new_item([s])
        self.assertEqual(SourceItem._lookup_cache, {})

    def test_statement_without_value_is_skipped(self):
        """Snak with no value (novalue/somevalue) is silently ignored."""
        s = Statement(mainsnak=Snak('P496', None), id='Q42$abc')
        with mock.patch.object(Snak, 'type_of', return_value='external-id'):
            SourceItem.register_new_item([s])
        self.assertEqual(SourceItem._lookup_cache, {})

    # --- interaction with lookup() ---

    def test_register_makes_subsequent_lookup_use_cache(self):
        """After registering, lookup() returns cached QID without calling haswbstatement."""
        SourceItem._lookup_cache['P496'] = {'0000-0002': None}
        s = _ext_id_statement('P496', '0000-0002', 'Q55')
        with mock.patch.object(Snak, 'type_of', return_value='external-id'):
            SourceItem.register_new_item([s])
        with mock.patch('wdpy.source_item.haswbstatement') as mock_hws:
            result = SourceItem.lookup('P496', '0000-0002')
        self.assertEqual(result, 'Q55')
        mock_hws.assert_not_called()


class TestProposedLabel(TestCase):
    def _item(self):
        return SourceItem()

    def test_proposed_label_initially_none(self):
        self.assertIsNone(self._item().proposed_label)

    def test_p1476_sets_proposed_label(self):
        item = self._item()
        item.add_claim('P1476', 'Some Title', 'en')
        self.assertEqual(item.proposed_label, 'Some Title')

    def test_first_p1476_wins(self):
        item = self._item()
        item.add_claim('P1476', 'First Title', 'en')
        item.add_claim('P1476', 'Second Title', 'fr')
        self.assertEqual(item.proposed_label, 'First Title')

    def test_other_property_does_not_set_label(self):
        item = self._item()
        item.add_claim('P31', 'Q5')
        self.assertIsNone(item.proposed_label)

    def test_p1476_without_value_does_not_set_label(self):
        item = self._item()
        item.add_claim('P1476')
        self.assertIsNone(item.proposed_label)

    def test_p1476_claim_still_added_to_patch(self):
        item = self._item()
        s = item.add_claim('P1476', 'A Title', 'en')
        self.assertIn(s, item.patch)
        self.assertEqual(item.patch[0].mainsnak.property, 'P1476')


def _make_resp_mock(code, url, body=b'{}'):
    m = mock.MagicMock()
    m.getcode.return_value = code
    m.geturl.return_value = url
    m.read.return_value = body
    m.__enter__ = lambda s: s
    m.__exit__ = mock.Mock(return_value=False)
    return m


class _BaseConnector(SourceItem):
    _config = {
        'extract': [404, 301],
        'properties': {
            'P999': 'http://example.com/{}',   # first → authoritative
            'P998': 'http://example2.com/{}',  # second → non-authoritative
        },
    }

    def parse(self, text: str, ident) -> None:
        pass

    @classmethod
    def update_ident(cls, ident, url):
        return Statement(Snak('P999', ('new-id',)))


class TestExtract(TestCase):
    def _ident(self, prop='P999'):
        return Statement(Snak(prop, ('old-id',)))

    @mock.patch('wdpy.source_item.build_opener')
    def test_authoritative_404_sets_deprecated_patch(self, build_mock):
        build_mock.return_value.open.return_value = _make_resp_mock(404, 'http://example.com/old-id')
        ident = self._ident('P999')
        result = _BaseConnector.extract(ident)
        self.assertIsNotNone(result.patch)
        self.assertEqual(len(result.patch), 1)
        self.assertEqual(result.patch[0].rank, 'deprecated')
        self.assertIs(result.patch[0].mainsnak, ident.mainsnak)
        self.assertFalse(hasattr(result, 'prior_ident'))

    @mock.patch('wdpy.source_item.build_opener')
    def test_non_authoritative_404_returns_empty(self, build_mock):
        build_mock.return_value.open.return_value = _make_resp_mock(404, 'http://example2.com/old-id')
        result = _BaseConnector.extract(self._ident('P998'))
        self.assertIsNone(result.patch)

    @mock.patch('wdpy.source_item.build_opener')
    def test_redirect_sets_patch_with_new_and_deprecated_ident(self, build_mock):
        build_mock.return_value.open.return_value = _make_resp_mock(
            200, 'http://example.com/new-id'
        )
        ident = self._ident()
        result = _BaseConnector.extract(ident)
        self.assertIsNotNone(result.patch)
        self.assertEqual(result.patch[0].mainsnak.value, ('new-id',))
        self.assertEqual(result.patch[-1].rank, 'deprecated')
        self.assertIs(result.patch[-1].mainsnak, ident.mainsnak)

    @mock.patch('wdpy.source_item.build_opener')
    def test_no_redirect_patch_is_none(self, build_mock):
        build_mock.return_value.open.return_value = _make_resp_mock(
            200, 'http://example.com/old-id'
        )
        result = _BaseConnector.extract(self._ident())
        self.assertIsNotNone(result)
        self.assertIsNone(result.patch)


def _make_snak(prop, val):
    return Snak(prop, (val,))


def _statement(prop, val, rank='normal'):
    return Statement(mainsnak=_make_snak(prop, val), rank=rank)


class _RefConnector(SourceItem):
    """Minimal connector with a db-ref source for _apply_references tests."""
    _config = {
        'source': 'Q180736',  # db_ref QID
        'properties': {'P356': 'http://example.com/{}'},
    }

    def parse(self, text, ident):
        pass


class TestApplyReferences(TestCase):
    """Unit tests for SourceItem._apply_references."""

    # --- no-op conditions ---

    def test_no_db_ref_is_noop(self):
        item = _RefConnector()
        item.patch = [_statement('P356', '10.1234/x')]
        with mock.patch.object(_RefConnector, 'get_db_ref', return_value=None):
            item._apply_references()
        self.assertIsNone(item.patch[0].references)

    def test_empty_patch_is_noop(self):
        item = _RefConnector()
        item.patch = []
        with mock.patch.object(Snak, 'create', return_value=mock.MagicMock()):
            item._apply_references()
        # no crash, nothing to assert beyond that

    # --- primary ident case (patch contains primary-prop statement) ---

    @mock.patch.object(Snak, 'create')
    def test_primary_ident_ref_contains_p248_and_primary(self, mock_create):
        p248_snak = mock.MagicMock(name='p248')
        id_snak = mock.MagicMock(name='id_snak')
        mock_create.side_effect = lambda prop, val: p248_snak if prop == 'P248' else id_snak

        item = _RefConnector()
        item.patch = [_statement('P356', '10.1234/x'), _statement('P31', 'Q13442814')]

        item._apply_references()

        mock_create.assert_any_call('P248', 'Q180736')
        mock_create.assert_any_call('P356', '10.1234/x')
        for s in item.patch:
            self.assertIsNotNone(s.references)

    # --- secondary ident case (patch has NO primary-prop statement) ---

    @mock.patch.object(Snak, 'create')
    def test_secondary_ident_ref_contains_only_p248(self, mock_create):
        p248_snak = mock.MagicMock(name='p248')
        mock_create.side_effect = lambda prop, val: p248_snak if prop == 'P248' else None

        item = _RefConnector()
        # patch has no P356 statement → patch_ident is None
        item.patch = [_statement('P31', 'Q13442814')]

        item._apply_references()

        # P356 snak must NOT be created
        for call in mock_create.call_args_list:
            self.assertNotEqual(call.args[0], 'P356')
        self.assertIsNotNone(item.patch[0].references)

    # --- deprecated primary-prop statement is not used as ident ---

    @mock.patch.object(Snak, 'create')
    def test_deprecated_primary_prop_not_used_as_ident(self, mock_create):
        p248_snak = mock.MagicMock(name='p248')
        mock_create.side_effect = lambda prop, val: p248_snak if prop == 'P248' else None

        item = _RefConnector()
        normal = _statement('P31', 'Q13442814')
        deprecated_p356 = _statement('P356', 'old-doi', rank='deprecated')
        item.patch = [deprecated_p356, normal]

        item._apply_references()

        # P356 snak must NOT be created (deprecated ident skipped)
        for call in mock_create.call_args_list:
            self.assertNotEqual(call.args[0], 'P356')
        self.assertIsNotNone(normal.references)
        self.assertIsNotNone(deprecated_p356.references)

    # --- all statements receive the reference regardless of rank ---

    @mock.patch.object(Snak, 'create')
    def test_deprecated_statement_gets_reference(self, mock_create):
        mock_create.return_value = mock.MagicMock()

        item = _RefConnector()
        normal = _statement('P31', 'Q13442814')
        deprecated = _statement('P356', 'old-id', rank='deprecated')
        item.patch = [normal, deprecated]

        item._apply_references()

        self.assertIsNotNone(normal.references)
        self.assertIsNotNone(deprecated.references)

    # --- all normal statements receive the reference ---

    @mock.patch.object(Snak, 'create')
    def test_primary_prop_statement_receives_reference(self, mock_create):
        """patch[0] with property == primary_prop must get the reference (Bug 1 fix)."""
        mock_create.return_value = mock.MagicMock()

        item = _RefConnector()
        doi_stmt = _statement('P356', '10.1234/x')   # the primary-prop statement
        item.patch = [doi_stmt]

        item._apply_references()

        self.assertIsNotNone(doi_stmt.references)
