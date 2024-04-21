from unittest import TestCase, mock

from adql import Element, Model


class TestADQL(TestCase):
    @classmethod
    def setUp(cls):
        item = Element('')
        cls.adql = item

    @mock.patch('wd.Wikidata.request', return_value=None)
    def test_tap_query(self, _):
        self.assertIsNone(Model.tap_query('https://simbad.u-strasbg.fr/simbad/sim-tap',
                                          'select main_id from basic where main_id=\'HD 1\''))
        self.assertDictEqual({}, Model.tap_query('https://simbad.u-strasbg.fr/simbad/sim-tap',
                                                 'select main_id from basic where main_id=\'HD 1\'', {}))


class TestGetParentSnak(TestCase):
    @mock.patch('wd.Wikidata.type_of', return_value='wikibase-item')
    def test_parent_name(self, _):
        Model._parents = {'hd 1': 'Q2'}
        self.assertEqual({'P5997': 'HD 1'}, Model.get_parent_snak('HD 1')['decorators'])


class TestParseUrl(TestCase):
    @mock.patch('wd.Wikidata.search', return_value='Q55882019')
    def test_parse_ads_encoded(self, api_search):
        value = Model.parse_url('https://ui.adsabs.harvard.edu/abs/2018A%26A...609A.117T/abstract')
        self.assertEqual('Q55882019', value)
        api_search.assert_called_with('haswbstatement:"P819=2018A&A...609A.117T"')

    @mock.patch('arxiv.Element.get_by_id', return_value='Q100255765')
    def test_parse_arxiv_old_format(self, get_by_id):
        value = Model.parse_url('http://fr.arxiv.org/abs/gr-qc/0204022')
        self.assertEqual('Q100255765', value)
        get_by_id.assert_called_with('gr-qc/0204022')

    @mock.patch('arxiv.Element.get_by_id', return_value='Q113365244')
    def test_parse_arxiv_without_prefix(self, get_by_id):
        value = Model.parse_url('arxiv.org/abs/1205.5704')
        self.assertEqual('Q113365244', value)
        get_by_id.assert_called_with('1205.5704')

    @mock.patch('arxiv.Element.get_by_id', return_value='Q113365525')
    def test_parse_arxiv_with_double_quote(self, get_by_id):
        value = Model.parse_url('http://arxiv.org/abs/1108.0031""')
        self.assertEqual('Q113365525', value)
        get_by_id.assert_called_with('1108.0031')

    @mock.patch('arxiv.Element.get_by_id', return_value='Q114396162')
    def test_parse_doi_arxiv_with_double_quote(self, get_by_id):
        value = Model.parse_url('https://doi.org/10.48550/arXiv.2011.10424"')
        self.assertEqual('Q114396162', value)
        get_by_id.assert_called_with('2011.10424')

    @mock.patch('arxiv.Element.get_by_id', return_value='Q114347665')
    def test_parse_arxiv_with_version(self, get_by_id):
        value = Model.parse_url('http://arxiv.org/abs/0902.4554\narXiv:0902.4554')
        self.assertEqual('Q114347665', value)
        get_by_id.assert_called_with('0902.4554')

    @mock.patch('arxiv.Element.get_by_id', return_value='Q114140841')
    def test_parse_arxiv_newline(self, get_by_id):
        value = Model.parse_url('https://arxiv.org/abs/2207.00101v1')
        self.assertEqual('Q114140841', value)
        get_by_id.assert_called_with('2207.00101')

    @mock.patch('wd.Wikidata.search', return_value='Q69036440')
    def test_parse_doi_trailing_slash(self, api_search):
        value = Model.parse_url('http://iopscience.iop.org/0004-637X/757/1/6/')
        self.assertEqual('Q69036440', value)
        api_search.assert_called_with('haswbstatement:P356=10.1088/0004-637X/757/1/6')

    @mock.patch('wd.Wikidata.search', return_value='Q38459152')
    def test_parse_doi_url_with_params(self, api_search):
        value = Model.parse_url('http://online.liebertpub.com/doi/abs/10.1089/ast.2011.0708?ai=sw&ui=10uu2&af=H')
        self.assertEqual('Q38459152', value)
        api_search.assert_called_with('haswbstatement:P356=10.1089/ast.2011.0708')

    @mock.patch('wd.Wikidata.search', return_value='Q68980169')
    def test_parse_doi_param(self, api_search):
        value = Model.parse_url('ex.php?option=com_article&access=doi&doi=10.1051/0004-6361/200912789&Itemid=129')
        self.assertEqual('Q68980169', value)
        api_search.assert_called_with('haswbstatement:P356=10.1051/0004-6361/200912789')

    @mock.patch('wd.Wikidata.search', return_value='Q113365192')
    def test_parse_isbn_param(self, api_search):
        value = Model.parse_url('http://www.cup.cam.ac.uk/aus/catalogue/catalogue.asp?isbn=9780521765596')
        self.assertEqual('Q113365192', value)
        api_search.assert_called_with('haswbstatement:P212=978-0-521-76559-6')

    @mock.patch('wd.Wikidata.search', return_value='Q68487809')
    def test_parse_doi_io(self, api_search):
        value = Model.parse_url('http://www.iop.org/EJ/abstract/0004-637X/698/1/451')
        self.assertEqual('Q68487809', value)
        api_search.assert_called_with('haswbstatement:P356=10.1088/0004-637X/698/1/451')

    @mock.patch('wd.Wikidata.search', return_value='Q114417499')
    def test_parse_doi_physica_scripta(self, api_search):
        value = Model.parse_url('http://www.iop.org/EJ/abstract/1402-4896/2008/T130/014010/')
        self.assertEqual('Q114417499', value)
        api_search.assert_called_with('haswbstatement:P356=10.1088/0031-8949/2008/T130/014010')

    @mock.patch('wd.Wikidata.search', return_value='Q68487090')
    def test_parse_doi_1538_4357(self, api_search):
        value = Model.parse_url('http://www.iop.org/EJ/abstract/1538-4357/696/1/L1')
        self.assertEqual('Q68487090', value)
        api_search.assert_called_with('haswbstatement:P356=10.1088/0004-637X/696/1/L1')

    @mock.patch('wd.Wikidata.search', return_value='Q29028722')
    def test_parse_doi_edpsciences(self, api_search):
        value = Model.parse_url('http://www.edpsciences.org/articles/aa/abs/2006/14/aa4611-05/aa4611-05.html')
        self.assertEqual('Q29028722', value)
        api_search.assert_called_with('haswbstatement:P356=10.1051/0004-6361:20054611')

    @mock.patch('wd.Wikidata.search', return_value='Q46025494')
    def test_parse_doi_nature(self, api_search):
        value = Model.parse_url('http://www.nature.com/nature/journal/v463/n7281/abs/nature08775.html')
        self.assertEqual('Q46025494', value)
        api_search.assert_called_with('haswbstatement:P356=10.1038/nature08775')

    @mock.patch('wd.Wikidata.search', return_value='Q68676988')
    def test_parse_doi_aa(self, api_search):
        value = Model.parse_url('https://www.aanda.org/articles/aa/abs/2009/35/aa10097-08/aa10097-08.html')
        self.assertEqual('Q68676988', value)
        api_search.assert_called_with('haswbstatement:P356=10.1051/0004-6361:200810097')

    @mock.patch('wd.Wikidata.search', return_value='Q53953306')
    def test_aa_959(self, api_search):
        value = Model.parse_url('http://www.aanda.org/....url=/articles/aa/abs/2004/18/aa0959/aa0959.html')
        self.assertEqual('Q53953306', value)
        api_search.assert_called_with('haswbstatement:P356=10.1051/0004-6361:20035959')
