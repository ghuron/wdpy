import json
from unittest import TestCase, mock
from wdpy import Snak, Statement, References
from wdpy.references import _PUB_DATES, _REDIRECTS


# ── helpers ──────────────────────────────────────────────────────────────────

def p50(qid: str, ordinal: str, display_name: str = '') -> Statement:
    quals = [Snak('P1545', (ordinal,))]
    if display_name:
        quals.append(Snak('P1932', (display_name,)))
    return Statement(Snak('P50', (qid,)), qualifiers=quals)


def p2093(name: str, ordinal: str) -> Statement:
    return Statement(Snak('P2093', (name,)),
                     qualifiers=[Snak('P1545', (ordinal,))])

@mock.patch('wdpy.Snak.type_of', return_value='string')
class Parse(TestCase):
    def test_basic(self, *_):
        s = Statement.parse({'mainsnak': {'property': 'P31'}, 'qualifiers': 
                            {'P433': [{'property': 'P433', 'snaktype': 'value',
                             'datavalue': {'type': 'string', 'value': '1'}}]}})
        self.assertEqual(s.mainsnak.property, 'P31')
        self.assertEqual(s.qualifiers[0].property, 'P433')

@mock.patch('wdpy.Snak.type_of', return_value='string')
class Json(TestCase):
    def test_basic(self, *_):
        s = Statement(Snak('P31', ('Q5',)))
        s.set_qualifier('P433', '1')
        self.assertEqual(json.loads(s.json())['qualifiers']['P433'][0]
                         ['datavalue']['value'], '1')

    def test_full(self, *_):
        s = Statement(Snak('P31', ('Q5',)), id='MyID', rank='preferred',
                      qualifiers=[Snak('P433', ('1',))],
                      references=References([{'snaks': {'P248':
                                 [Snak('P248', ('Q123',))]}}]))
        res = json.loads(s.json())
        self.assertEqual(res['id'], 'MyID')
        self.assertEqual(res['rank'], 'preferred')
        self.assertEqual(len(res['references']), 1)

    def test_minimal(self, *_):
        res = json.loads(Statement(Snak('P31', ('Q5',))).json())
        self.assertNotIn('qualifiers', res)
        self.assertNotIn('references', res)

@mock.patch('wdpy.Snak.type_of', return_value='string')
class SetRank(TestCase):
    def test_preferred(self, *_):
        s = Statement(Snak('P31', ('Q5',)))
        s.set_rank('preferred', 'reason')
        self.assertEqual(s.rank, 'preferred')
        self.assertEqual(s.qualifiers[0].property, 'P7452')

@mock.patch('wdpy.Snak.type_of', return_value='string')
class SetQualifier(TestCase):
    def test_basic(self, *_):
        s = Statement(Snak('P31', ('Q5',)))
        s.set_qualifier('P433', '1')
        self.assertEqual(s.qualifiers[0].value, ('1',))


class Upsert(TestCase):

    # --- _qualifiers_present_in ---

    def test_no_qualifiers_matches_any_candidate(self):
        incoming = Statement(Snak('P31', ('Q5',)))
        candidate = Statement(Snak('P31', ('Q5',)), qualifiers=[Snak('P580', ('2020',))])
        self.assertTrue(incoming._qualifiers_present_in(candidate))

    def test_self_has_qualifiers_candidate_has_none(self):
        incoming = Statement(Snak('P31', ('Q5',)), qualifiers=[Snak('P580', ('2020',))])
        candidate = Statement(Snak('P31', ('Q5',)))
        self.assertFalse(incoming._qualifiers_present_in(candidate))

    def test_qualifier_property_absent_in_candidate(self):
        incoming = Statement(Snak('P31', ('Q5',)), qualifiers=[Snak('P580', ('2020',))])
        candidate = Statement(Snak('P31', ('Q5',)), qualifiers=[Snak('P582', ('2021',))])
        self.assertFalse(incoming._qualifiers_present_in(candidate))

    def test_qualifier_value_differs(self):
        incoming = Statement(Snak('P31', ('Q5',)), qualifiers=[Snak('P580', ('2020',))])
        candidate = Statement(Snak('P31', ('Q5',)), qualifiers=[Snak('P580', ('2019',))])
        self.assertFalse(incoming._qualifiers_present_in(candidate))

    def test_qualifier_matches(self):
        q = Snak('P580', ('2020',))
        incoming = Statement(Snak('P31', ('Q5',)), qualifiers=[q])
        candidate = Statement(Snak('P31', ('Q5',)), qualifiers=[q])
        self.assertTrue(incoming._qualifiers_present_in(candidate))

    def test_candidate_extra_qualifiers_still_matches(self):
        incoming = Statement(Snak('P31', ('Q5',)), qualifiers=[Snak('P580', ('2020',))])
        candidate = Statement(Snak('P31', ('Q5',)),
                              qualifiers=[Snak('P580', ('2020',)), Snak('P582', ('2021',))])
        self.assertTrue(incoming._qualifiers_present_in(candidate))

    def test_all_qualifiers_must_match(self):
        incoming = Statement(Snak('P31', ('Q5',)),
                             qualifiers=[Snak('P580', ('2020',)), Snak('P582', ('2021',))])
        candidate = Statement(Snak('P31', ('Q5',)), qualifiers=[Snak('P580', ('2020',))])
        self.assertFalse(incoming._qualifiers_present_in(candidate))

    # --- upsert: matching logic ---

    def test_empty_existing_returns_none(self):
        self.assertIsNone(Statement(Snak('P31', ('Q5',))).upsert([]))

    def test_value_mismatch_returns_none(self):
        incoming = Statement(Snak('P31', ('Q5',)))
        self.assertIsNone(incoming.upsert([Statement(Snak('P31', ('Q6',)))]))

    def test_value_match_returns_candidate(self):
        incoming = Statement(Snak('P31', ('Q5',)))
        candidate = Statement(Snak('P31', ('Q5',)))
        self.assertIs(incoming.upsert([candidate]), candidate)

    def test_qualifier_mismatch_returns_none(self):
        incoming = Statement(Snak('P31', ('Q5',)), qualifiers=[Snak('P580', ('2020',))])
        candidate = Statement(Snak('P31', ('Q5',)), qualifiers=[Snak('P580', ('2019',))])
        self.assertIsNone(incoming.upsert([candidate]))

    def test_novalue_match(self):
        incoming = Statement(Snak('P31', None, 'novalue'))
        candidate = Statement(Snak('P31', None, 'novalue'))
        self.assertIs(incoming.upsert([candidate]), candidate)

    def test_somevalue_match(self):
        incoming = Statement(Snak('P31', None, 'somevalue'))
        candidate = Statement(Snak('P31', None, 'somevalue'))
        self.assertIs(incoming.upsert([candidate]), candidate)

    def test_novalue_vs_value_no_match(self):
        incoming = Statement(Snak('P31', None, 'novalue'))
        self.assertIsNone(incoming.upsert([Statement(Snak('P31', ('Q5',)))]))

    def test_value_vs_novalue_no_match(self):
        incoming = Statement(Snak('P31', ('Q5',)))
        self.assertIsNone(incoming.upsert([Statement(Snak('P31', None, 'novalue'))]))

    def test_first_match_wins(self):
        incoming = Statement(Snak('P31', ('Q5',)))
        c1, c2 = Statement(Snak('P31', ('Q5',))), Statement(Snak('P31', ('Q5',)))
        self.assertIs(incoming.upsert([c1, c2]), c1)

    def test_skips_non_matching_before_match(self):
        incoming = Statement(Snak('P31', ('Q5',)))
        miss = Statement(Snak('P31', ('Q6',)))
        hit = Statement(Snak('P31', ('Q5',)))
        self.assertIs(incoming.upsert([miss, hit]), hit)

    # --- upsert: time precision-aware matching ---

    @mock.patch.object(Snak, 'type_of', return_value='time')
    def test_time_same_value_same_precision_matches(self, *_):
        incoming  = Statement(Snak('P577', ('20130101', '11', 'Q1985727')))
        candidate = Statement(Snak('P577', ('20130101', '11', 'Q1985727')))
        self.assertIs(incoming.upsert([candidate]), candidate)

    @mock.patch.object(Snak, 'type_of', return_value='time')
    def test_time_month_patch_does_not_match_day_candidate(self, *_):
        """Different precisions must never merge, regardless of direction."""
        incoming  = Statement(Snak('P577', ('20130100', '10', 'Q1985727')))
        candidate = Statement(Snak('P577', ('20130101', '11', 'Q1985727')))
        self.assertIsNone(incoming.upsert([candidate]))

    @mock.patch.object(Snak, 'type_of', return_value='time')
    def test_time_day_patch_does_not_match_month_candidate(self, *_):
        """More-precise patch must not merge into a less-precise candidate."""
        incoming  = Statement(Snak('P577', ('20130101', '11', 'Q1985727')))
        candidate = Statement(Snak('P577', ('20130100', '10', 'Q1985727')))
        self.assertIsNone(incoming.upsert([candidate]))

    @mock.patch.object(Snak, 'type_of', return_value='time')
    def test_time_year_patch_does_not_match_day_candidate(self, *_):
        """Different precisions must never merge, regardless of direction."""
        incoming  = Statement(Snak('P577', ('20130000', '9', 'Q1985727')))
        candidate = Statement(Snak('P577', ('20130101', '11', 'Q1985727')))
        self.assertIsNone(incoming.upsert([candidate]))

    @mock.patch.object(Snak, 'type_of', return_value='time')
    def test_time_day_patch_does_not_match_year_candidate(self, *_):
        """More-precise patch must not merge into a less-precise candidate."""
        incoming  = Statement(Snak('P577', ('20130101', '11', 'Q1985727')))
        candidate = Statement(Snak('P577', ('20130000', '9',  'Q1985727')))
        self.assertIsNone(incoming.upsert([candidate]))

    @mock.patch.object(Snak, 'type_of', return_value='time')
    def test_time_month_patch_does_not_match_year_candidate(self, *_):
        """More-precise patch (month) must not merge into year-precision candidate."""
        incoming  = Statement(Snak('P577', ('20130100', '10', 'Q1985727')))
        candidate = Statement(Snak('P577', ('20130000', '9',  'Q1985727')))
        self.assertIsNone(incoming.upsert([candidate]))

    @mock.patch.object(Snak, 'type_of', return_value='time')
    def test_time_year_patch_does_not_match_month_candidate(self, *_):
        """Different precisions must never merge, regardless of direction."""
        incoming  = Statement(Snak('P577', ('20130000', '9',  'Q1985727')))
        candidate = Statement(Snak('P577', ('20130100', '10', 'Q1985727')))
        self.assertIsNone(incoming.upsert([candidate]))

    @mock.patch.object(Snak, 'type_of', return_value='time')
    def test_time_different_month_no_match(self, *_):
        incoming  = Statement(Snak('P577', ('20130200', '10', 'Q1985727')))
        candidate = Statement(Snak('P577', ('20130101', '11', 'Q1985727')))
        self.assertIsNone(incoming.upsert([candidate]))

    @mock.patch.object(Snak, 'type_of', return_value='time')
    def test_time_different_year_no_match(self, *_):
        incoming  = Statement(Snak('P577', ('20140000', '9', 'Q1985727')))
        candidate = Statement(Snak('P577', ('20130101', '11', 'Q1985727')))
        self.assertIsNone(incoming.upsert([candidate]))

    @mock.patch.object(Snak, 'type_of', return_value='time')
    def test_time_different_calendar_no_match(self, *_):
        incoming  = Statement(Snak('P577', ('20130101', '11', 'Q1985727')))
        candidate = Statement(Snak('P577', ('20130101', '11', 'Q1985786')))
        self.assertIsNone(incoming.upsert([candidate]))

    # --- upsert: reference merging ---

    def test_no_incoming_references_leaves_candidate_unchanged(self):
        incoming = Statement(Snak('P31', ('Q5',)))
        candidate = Statement(Snak('P31', ('Q5',)))
        incoming.upsert([candidate])
        self.assertIsNone(candidate.references)

    @mock.patch('wdpy.references._preload')
    def test_references_merged_into_candidate_with_none_refs(self, *_):
        ref = Snak('P248', ('Q123',))
        incoming = Statement(Snak('P31', ('Q5',)),
                             references=References([{'snaks': {'P248': [ref]}}]))
        candidate = Statement(Snak('P31', ('Q5',)))
        incoming.upsert([candidate])
        self.assertIsNotNone(candidate.references)
        self.assertEqual(len(candidate.references._items), 1)

    @mock.patch('wdpy.references._preload')
    def test_different_references_both_kept(self, *_):
        incoming = Statement(Snak('P31', ('Q5',)),
                             references=References([{'snaks': {'P248': [Snak('P248', ('Q2',))]}}]))
        candidate = Statement(Snak('P31', ('Q5',)),
                              references=References([{'snaks': {'P248': [Snak('P248', ('Q1',))]}}]))
        incoming.upsert([candidate])
        self.assertEqual(len(candidate.references._items), 2)

    @mock.patch('wdpy.references._preload')
    @mock.patch.object(Snak, 'try_to_merge_references', return_value=True)
    def test_identical_references_merged_not_duplicated(self, *_):
        ref = {'snaks': {'P248': [Snak('P248', ('Q1',))]}}
        incoming = Statement(Snak('P31', ('Q5',)), references=References([ref]))
        candidate = Statement(Snak('P31', ('Q5',)), references=References([ref]))
        incoming.upsert([candidate])
        self.assertEqual(len(candidate.references._items), 1)


class UpsertRank(TestCase):

    def test_propagates_deprecated_rank(self):
        incoming = Statement(Snak('P31', ('Q5',)), rank='deprecated')
        candidate = Statement(Snak('P31', ('Q5',)), rank='normal')
        incoming.upsert([candidate])
        self.assertEqual(candidate.rank, 'deprecated')

    def test_propagates_normal_rank(self):
        incoming = Statement(Snak('P31', ('Q5',)), rank='normal')
        candidate = Statement(Snak('P31', ('Q5',)), rank='deprecated')
        incoming.upsert([candidate])
        self.assertEqual(candidate.rank, 'normal')

    def test_none_rank_preserves_existing(self):
        incoming = Statement(Snak('P31', ('Q5',)), rank=None)
        candidate = Statement(Snak('P31', ('Q5',)), rank='deprecated')
        incoming.upsert([candidate])
        self.assertEqual(candidate.rank, 'deprecated')


class DeduplicateAuthors(TestCase):

    ORD = 'P1545'

    def dedup(self, *stmts):
        return Statement.deduplicate_authors(list(stmts), self.ORD)

    # ── no-op cases ──────────────────────────────────────────────────────────

    def test_empty_list(self):
        self.assertEqual(self.dedup(), [])

    def test_single_author_no_duplicate(self):
        self.assertEqual(self.dedup(p2093('Alice', '1')), [])

    def test_no_ordinal_qualifier_skipped(self):
        s = Statement(Snak('P2093', ('Alice',)))     # no P1545
        self.assertEqual(self.dedup(s), [])

    def test_different_ordinals_no_deletion(self):
        self.assertEqual(self.dedup(p2093('Alice', '1'), p2093('Bob', '2')), [])

    # ── P2093 vs P2093 ───────────────────────────────────────────────────────

    def test_p2093_first_longer_second_deleted(self):
        first = p2093('Alice Smith', '1')
        second = p2093('A. Smith', '1')
        result = self.dedup(first, second)
        self.assertEqual(result, [second])

    def test_p2093_second_longer_first_deleted(self):
        first = p2093('A. Smith', '1')
        second = p2093('Alice Smith', '1')
        result = self.dedup(first, second)
        self.assertEqual(result, [first])

    def test_p2093_equal_length_first_kept(self):
        first = p2093('Alice', '1')
        second = p2093('Smith', '1')
        result = self.dedup(first, second)
        self.assertEqual(result, [second])

    # ── P50 vs P2093 ─────────────────────────────────────────────────────────

    def test_p2093_then_p50_p2093_deleted(self):
        string_author = p2093('Alice Smith', '1')
        linked_author = p50('Q42', '1')
        result = self.dedup(string_author, linked_author)
        self.assertEqual(result, [string_author])

    def test_p50_then_p2093_p2093_deleted(self):
        linked_author = p50('Q42', '1')
        string_author = p2093('Alice Smith', '1')
        result = self.dedup(linked_author, string_author)
        self.assertEqual(result, [string_author])

    # ── P50 vs P50, same QID ─────────────────────────────────────────────────

    def test_p50_same_qid_second_longer_name_first_deleted(self):
        first = p50('Q42', '1', 'A. Smith')
        second = p50('Q42', '1', 'Alice Smith')
        result = self.dedup(first, second)
        self.assertEqual(result, [first])

    def test_p50_same_qid_first_longer_name_second_deleted(self):
        first = p50('Q42', '1', 'Alice Smith')
        second = p50('Q42', '1', 'A. Smith')
        result = self.dedup(first, second)
        self.assertEqual(result, [second])

    def test_p50_same_qid_no_p1932_second_deleted(self):
        first = p50('Q42', '1')      # no display name
        second = p50('Q42', '1')
        result = self.dedup(first, second)
        self.assertEqual(result, [second])

    # ── P50 vs P50, different QIDs ───────────────────────────────────────────

    def test_p50_different_qids_both_kept(self):
        self.assertEqual(self.dedup(p50('Q1', '1'), p50('Q2', '1')), [])

    def test_p50_different_qids_third_same_as_first_kept_with_first(self):
        # Q1 appears twice at ordinal 1, Q2 is different → Q2 kept, second Q1 deleted
        a = p50('Q1', '1', 'Alice')
        b = p50('Q2', '1', 'Bob')
        c = p50('Q1', '1', 'Alice Smith')   # same QID as a, longer name
        result = self.dedup(a, b, c)
        # b gets a 'continue' (different QID from incumbent a), so best['1'] stays a
        # then c: same QID as a, longer name → c wins, a deleted
        self.assertIn(a, result)
        self.assertNotIn(b, result)
        self.assertNotIn(c, result)

    # ── multiple ordinals ─────────────────────────────────────────────────────

    def test_mixed_ordinals_only_duplicates_deleted(self):
        a1 = p2093('Alice Smith', '1')
        a1_dup = p2093('A. Smith', '1')
        b2 = p50('Q99', '2')
        result = self.dedup(a1, a1_dup, b2)
        self.assertEqual(result, [a1_dup])


# ── helpers for date-based tests ──────────────────────────────────────────────

def _dated_refs(date: int) -> References:
    """References with a single P248 snak whose publication date is `date`."""
    qid = f'Q{date}'
    _PUB_DATES[qid] = date
    return References([{'snaks': {'P248': [Snak('P248', (qid,))]}}])


class SelectOutdated(TestCase):
    GROUP = 'P1545'

    def setUp(self):
        _PUB_DATES.clear()
        _REDIRECTS.clear()

    def _s(self, val='Q1', date=None):
        refs = _dated_refs(date) if date is not None else None
        return Statement(Snak('P31', (val,)), references=refs)

    def _sg(self, val='Q1', group='1', date=None):
        s = self._s(val, date)
        s.qualifiers = [Snak(self.GROUP, (group,))]
        return s

    def _nv(self, group=None):
        quals = [Snak(self.GROUP, (group,))] if group else None
        return Statement(Snak('P31', None, 'novalue'), qualifiers=quals)

    # ── no group_by ───────────────────────────────────────────────────────────

    def test_empty(self):
        self.assertEqual(Statement.select_outdated([]), [])

    def test_single(self):
        self.assertEqual(Statement.select_outdated([self._s()]), [])

    def test_older_deleted(self):
        old = self._s('Q1', 20200101)
        new = self._s('Q2', 20220101)
        self.assertEqual(Statement.select_outdated([new, old]), [old])

    def test_older_first_still_deleted(self):
        old = self._s('Q1', 20200101)
        new = self._s('Q2', 20220101)
        self.assertEqual(Statement.select_outdated([old, new]), [old])

    def test_no_refs_deleted_when_other_is_dated(self):
        no_date = self._s('Q1')
        dated = self._s('Q2', 20220101)
        self.assertEqual(Statement.select_outdated([no_date, dated]), [no_date])

    def test_equal_dates_first_kept_second_deleted(self):
        a = self._s('Q1', 20220101)
        b = self._s('Q2', 20220101)
        self.assertEqual(Statement.select_outdated([a, b]), [b])

    def test_novalue_beats_value(self):
        val = self._s('Q1', 20220101)
        nv = self._nv()
        result = Statement.select_outdated([val, nv])
        self.assertIn(val, result)
        self.assertNotIn(nv, result)

    def test_two_novalue_second_deleted(self):
        nv1, nv2 = self._nv(), self._nv()
        self.assertEqual(Statement.select_outdated([nv1, nv2]), [nv2])

    # ── with group_by ─────────────────────────────────────────────────────────

    def test_missing_qualifier_always_deleted(self):
        s = self._s()   # no P1545
        self.assertEqual(Statement.select_outdated([s], group_by=self.GROUP), [s])

    def test_groups_managed_independently(self):
        a_old = self._sg('Q1', '1', 20200101)
        a_new = self._sg('Q2', '1', 20220101)
        b_old = self._sg('Q3', '2', 20190101)
        b_new = self._sg('Q4', '2', 20210101)
        result = Statement.select_outdated([a_old, a_new, b_old, b_new],
                                           group_by=self.GROUP)
        self.assertIn(a_old, result)
        self.assertIn(b_old, result)
        self.assertNotIn(a_new, result)
        self.assertNotIn(b_new, result)

    def test_unqualified_deleted_alongside_group_outdated(self):
        no_qual = self._s()
        old = self._sg('Q1', '1', 20200101)
        new = self._sg('Q2', '1', 20220101)
        result = Statement.select_outdated([no_qual, old, new], group_by=self.GROUP)
        self.assertIn(no_qual, result)
        self.assertIn(old, result)
        self.assertNotIn(new, result)

    def test_novalue_in_group_beats_value(self):
        val = self._sg('Q1', '1', 20220101)
        nv = self._nv(group='1')
        result = Statement.select_outdated([val, nv], group_by=self.GROUP)
        self.assertIn(val, result)
        self.assertNotIn(nv, result)


class RankByRecency(TestCase):

    def setUp(self):
        _PUB_DATES.clear()
        _REDIRECTS.clear()

    def _s(self, date=None, *, rank=None, p2241=False):
        refs = _dated_refs(date) if date is not None else None
        quals = [Snak('P2241', ('Q1',))] if p2241 else None
        return Statement(Snak('P31', ('Q1',)), rank=rank, qualifiers=quals, references=refs)

    # ── no-op cases ───────────────────────────────────────────────────────────

    def test_empty(self):
        Statement.rank_by_recency([])   # must not raise

    def test_preferred_halts_all_changes(self):
        preferred = self._s(20220101, rank='preferred')
        old = self._s(20200101)
        Statement.rank_by_recency([old, preferred])
        self.assertIsNone(old.rank)

    def test_p2241_statement_not_modified(self):
        s = self._s(p2241=True)
        Statement.rank_by_recency([s])
        self.assertIsNone(s.rank)

    # ── rank assignment ───────────────────────────────────────────────────────

    def test_single_no_date_becomes_normal(self):
        s = self._s()
        Statement.rank_by_recency([s])
        self.assertEqual(s.rank, 'normal')

    def test_newer_normal_older_deprecated(self):
        new = self._s(20220101)
        old = self._s(20200101)
        Statement.rank_by_recency([new, old])
        self.assertEqual(new.rank, 'normal')
        self.assertEqual(old.rank, 'deprecated')

    def test_order_independent(self):
        new = self._s(20220101)
        old = self._s(20200101)
        Statement.rank_by_recency([old, new])
        self.assertEqual(new.rank, 'normal')
        self.assertEqual(old.rank, 'deprecated')

    def test_no_refs_deprecated_when_other_is_dated(self):
        dated = self._s(20220101)
        undated = self._s()
        Statement.rank_by_recency([dated, undated])
        self.assertEqual(dated.rank, 'normal')
        self.assertEqual(undated.rank, 'deprecated')

    def test_equal_dates_first_normal_second_deprecated(self):
        a = self._s(20220101)
        b = self._s(20220101)
        Statement.rank_by_recency([a, b])
        self.assertEqual(a.rank, 'normal')
        self.assertEqual(b.rank, 'deprecated')

    def test_p2241_excluded_others_ranked(self):
        with_reason = self._s(20230101, p2241=True)
        new = self._s(20220101)
        old = self._s(20200101)
        Statement.rank_by_recency([with_reason, new, old])
        self.assertIsNone(with_reason.rank)
        self.assertEqual(new.rank, 'normal')
        self.assertEqual(old.rank, 'deprecated')


def _p577(date: str, precision: int) -> Statement:
    return Statement(Snak('P577', (date, str(precision), 'Q1985727')))


class RankByPrecision(TestCase):

    def test_higher_precision_gets_preferred_over_lower(self):
        """Precision-9 must not be promoted when a precision-10 statement
        for the same year exists.

        Bug: both ('20130000','9',...) and ('20130000','10',...) truncate to
        the same string at precision-10, so _is_most_precise() returned True
        for the precision-9 statement and it was promoted first.
        """
        p9  = _p577('20130000', 9)
        p10 = _p577('20130000', 10)
        Statement.rank_by_precision([p9, p10])
        self.assertEqual(p10.rank, 'preferred')
        self.assertNotEqual(p9.rank, 'preferred')
