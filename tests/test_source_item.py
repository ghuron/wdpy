import json
import sys
from unittest import TestCase, mock

import wdpy
from wdpy import SourceItem


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
