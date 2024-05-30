from unittest import TestCase, mock

from wd import Article, Wikidata


class TestPostProcess(TestCase):
    @mock.patch('wd.Wikidata.load', return_value=None)
    @mock.patch('wd.Wikidata.type_of', return_value='wikibase-item')
    def test_rearrange(self, mock_type, __):
        item = Article('test_id', 'Q1')
        (author1 := Wikidata.create_snak('P50', 'Q23'))['qualifiers'] = {'P1545': '1'}
        (author2 := Wikidata.create_snak('P50', 'Q42'))['qualifiers'] = {'P1545': '2'}
        mock_type.return_value = 'string'
        (claim2 := item.obtain_claim(author2))['mainsnak']['hash'] = ''
        (claim1 := item.obtain_claim(author1))['mainsnak']['hash'] = ''
        item.post_process()
        self.assertIn('remove', item._queue[0])
        self.assertIn('remove', item._queue[1])
        self.assertDictEqual(claim1, item._queue[2])
        self.assertDictEqual(claim2, item._queue[3])
