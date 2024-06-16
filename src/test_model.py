from unittest import TestCase, mock

from wd import Model


class TestEnrichQualifier(TestCase):
    @mock.patch('wd.Wikidata.type_of', return_value='string')
    @mock.patch('wd.Model.config', return_value={'id': 'P972', 'translate': {'HD ': 'Q111130'}})
    def test_catalogue(self, _, __):
        cat = Model.transform('P528', 'HD 1')
        self.assertEqual([('P972', 'Q111130')], Model.enrich_qualifier(cat, cat['datavalue']['value'])['qualifiers'])

    @mock.patch('wd.Wikidata.type_of', return_value='quantity')
    @mock.patch('wd.Model.config', return_value={'id': 'P6259', 'translate': {'': 'Q1264450'}})
    def test_default_qualifier(self, _, __):
        peri = Model.enrich_qualifier(Model.transform('P11796', 100), '')
        self.assertEqual([('P6259', 'Q1264450')], peri['qualifiers'])
