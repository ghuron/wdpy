from unittest import TestCase, mock
import urllib.error
from wdpy.connectors import ExoplanetItem
from wdpy import Snak, Statement

@mock.patch('wdpy.Snak.type_of', return_value='external-id')
class TestExtract(TestCase):
    def setUp(self):
        self.config_patcher = mock.patch.object(ExoplanetItem, '_config', {
            'properties': {'P5653': 'https://exoplanet.eu/catalog/{}/'}
        })
        self.config_patcher.start()

    def tearDown(self):
        self.config_patcher.stop()

        @mock.patch('wdpy.source_item.request')

        def test_no_redirect(self, req_mock, *_):

            req_mock.return_value = mock.MagicMock(url='https://exoplanet.eu/catalog/x/', code=200)

            self.assertIsNone(ExoplanetItem.extract(Statement(Snak('P5653', ('x',)))))

    

        @mock.patch('wdpy.source_item.request')

        def test_redirect(self, req_mock, *_):

            req_mock.return_value = mock.MagicMock(url='https://exoplanet.eu/catalog/x1/', code=200)

            res = ExoplanetItem.extract(Statement(Snak('P5653', ('x',))))

            self.assertEqual(res.patch[0].mainsnak.value[0], 'x1')

    

        @mock.patch('wdpy.source_item.request')

        def test_404(self, req_mock, *_):

            req_mock.return_value = mock.MagicMock(code=404)

            self.assertEqual(ExoplanetItem.extract(Statement(Snak('P5653', ('x',)))).patch, [])

    