from unittest import mock, TestCase
from unittest.mock import MagicMock

from wd import Claim, Model, Element


class TestPreload(TestCase):
    @mock.patch('wd.Wikidata.type_of', return_value='time')
    def setUp(self, _):
        self.q1 = Element('')
        self.q1.obtain_claim(Model.create_snak('P577', '2022-02-02'))

    @mock.patch('wd.Wikidata.type_of', return_value='wikibase-item')
    @mock.patch('wd.Wikidata.load')
    def test_simple_load(self, mock_load: MagicMock, _):
        mock_load.return_value = {'Q1111': self.q1.entity}
        Claim.preload({'Q1111'})
        self.assertEqual(20220202, Claim._pub_dates['Q1111'])
        # Subsequent preload should be skipped
        self.assertEqual(0, len(Claim.extract_references({'references': [Claim._create_ref('Q1111', {})]})))

    @mock.patch('wd.Wikidata.type_of', return_value='wikibase-item')
    @mock.patch('wd.Wikidata.load')
    def test_redirect_load(self, mock_load, _):
        mock_load.return_value = {'Q2222': {'redirects': {'to': 'Q1111'}}}
        Claim.preload({'Q2222'})
        self.assertEqual('Q1111', Claim._redirects['Q2222'])
        # Subsequent preload should be skipped
        self.assertEqual(0, len(Claim.extract_references({'references': [Claim._create_ref('Q2222', {})]})))


@mock.patch('wd.Wikidata.type_of', return_value='wikibase-item')
class TestRemoveDuplicates(TestCase):
    def test_simple_duplicate_no_wdpy(self, _):
        ref = Claim._create_ref('Q1111', {})
        self.assertEqual(1, len(result := Claim._remove_duplicates([ref, ref])))
        self.assertNotIn('wdpy', result[0])

    def test_simple_duplicate_combined_wdpy(self, _):
        ref = Claim._create_ref('Q1111', {})
        self.assertEqual(1, len(result := Claim._remove_duplicates([ref, {**ref, 'wdpy': 1}])))
        self.assertIn('wdpy', result[0])


@mock.patch('wd.Wikidata.type_of', return_value='wikibase-item')
class TestConfirms(TestCase):
    def setUp(self):
        Claim.db_ref = 'Q654724'

    def test_own_P248(self, _):
        p248 = Claim._create_ref(Claim.db_ref, {})
        self.assertEqual(0, len(Claim._confirms([p248])))
        self.assertCountEqual([p248], Claim._confirms([{**p248, 'wdpy': 1}]))

        p12132 = {'snaks': {'P12132': [Model.create_snak('P12132', Claim.db_ref)]}}
        self.assertCountEqual([p12132], Claim._confirms([{**p248, 'wdpy': 1}, {**p12132, 'wdpy': 1}]))
        self.assertCountEqual([p12132], Claim._confirms([{**p12132, 'wdpy': 1}, {**p248, 'wdpy': 1}]))

    def test_foreign_P248(self, _):
        p248 = Claim._create_ref(Claim.db_ref + '1', {})
        self.assertCountEqual([p248], Claim._confirms([p248]))

        p12132 = {'snaks': {'P12132': [Model.create_snak('P12132', Claim.db_ref + '1')]}}
        self.assertCountEqual([p248, p12132], Claim._confirms([{**p248, 'wdpy': 1}, {**p12132, 'wdpy': 1}]))
        self.assertCountEqual([p248, p12132], Claim._confirms([{**p12132, 'wdpy': 1}, {**p248, 'wdpy': 1}]))

    def test_non_confirmed_P12132(self, _):
        own = Model.create_snak('P12132', Claim.db_ref)
        self.assertEqual(0, len(Claim._confirms([{'snaks': {'P12132': [own]}}])))

        foreign = Model.create_snak('P12132', Claim.db_ref + '1')
        self.assertEqual({'snaks': {'P12132': [foreign]}}, Claim._confirms([{'snaks': {'P12132': [own, foreign]}}])[0])


class TestProcessDecorators(TestCase):
    @mock.patch('wd.Wikidata.type_of', return_value='wikibase-item')
    def test_decorator(self, _):
        Claim.db_ref = 'Q1'
        (claim := Claim({})).process_decorators({'decorators': {'P12132': 'Q2'}, 'source': []})
        self.assertEqual('Q1', claim.claim['references'][0]['snaks']['P248'][0]['datavalue']['value']['id'])
        self.assertEqual('Q2', claim.claim['references'][0]['snaks']['P12132'][0]['datavalue']['value']['id'])


@mock.patch('wd.Wikidata.type_of')
class TestFindMorePreciseClaim(TestCase):
    def test_year_and_month(self, mock_type: MagicMock):
        mock_type.return_value = 'time'
        year = Claim.construct(Model.create_snak('P575', '2000'))
        month = Claim.construct(Model.create_snak('P575', '2000-04'))
        mock_type.return_value = 'wikibase-item'

        self.assertEqual(month.claim, year.find_more_precise_claim([year.claim, month.claim]))
        self.assertEqual('deprecated', year.claim['rank'])
        self.assertEqual('Q42727519', year.claim['qualifiers']['P2241'][0]['datavalue']['value']['id'])

    def test_month_different_year(self, mock_type: MagicMock):
        mock_type.return_value = 'time'
        year = Claim.construct(Model.create_snak('P575', '2000'))
        month = Claim.construct(Model.create_snak('P575', '1999-04'))
        mock_type.return_value = 'wikibase-item'

        self.assertEqual(None, year.find_more_precise_claim([month.claim, year.claim]))
        self.assertNotIn('rank', year.claim)

    def test_same_value_diff_precisions(self, mock_type: MagicMock):
        mock_type.return_value = 'time'
        month = Claim.construct(Model.create_snak('P575', '1976-12'))
        year = Claim.construct(Model.create_snak('P575', '1976'))
        year.claim['mainsnak']['datavalue']['value']['time'] = month.claim['mainsnak']['datavalue']['value']['time']
        mock_type.return_value = 'wikibase-item'

        self.assertEqual(None, month.find_more_precise_claim([month.claim, year.claim]))

    def test_2_amounts_without_units(self, mock_type: MagicMock):
        mock_type.return_value = 'quantity'
        rough = Claim.construct(Model.create_snak('P1096', '0.56'))
        precise = Claim.construct(Model.create_snak('P1096', '0.555'))
        mock_type.return_value = 'wikibase-item'

        self.assertEqual(precise.claim, rough.find_more_precise_claim([rough.claim, precise.claim]))
        self.assertEqual('deprecated', rough.claim['rank'])

    def test_different_units(self, mock_type: MagicMock):
        mock_type.return_value = 'quantity'
        rough = Claim.construct(Model.create_snak('P2051', '0.56'))
        rough.claim['mainsnak']['datavalue']['value']['unit'] = 'http://www.wikidata.org/entity/Q681996'
        precise = Claim.construct(Model.create_snak('P2051', '0.555'))
        precise.claim['mainsnak']['datavalue']['value']['unit'] = 'http://www.wikidata.org/entity/Q651336'
        mock_type.return_value = 'wikibase-item'

        self.assertEqual(None, rough.find_more_precise_claim([rough.claim, precise.claim]))

    def test_upper_lower_bound(self, mock_type: MagicMock):
        mock_type.return_value = 'quantity'
        precise = Claim.construct(Model.create_snak('P2583', '113.4314', '0.5211', '0.5211'))
        rough = Claim.construct(Model.create_snak('P2583', '113.43', '0.52', '0.52'))
        mock_type.return_value = 'wikibase-item'

        self.assertEqual(precise.claim, rough.find_more_precise_claim([rough.claim, precise.claim]))


class TestCreateRef(TestCase):
    @mock.patch('wd.Wikidata.type_of', return_value='wikibase-item')
    def test_db_ref(self, _):
        Claim.db_ref = 'Q1385430'
        self.assertNotIn('P12132', Claim._create_ref(Claim.db_ref, {'P12132': Claim.db_ref})['snaks'])
