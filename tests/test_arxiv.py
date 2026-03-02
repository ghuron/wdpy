from unittest import TestCase, mock

from wdpy import Snak, Statement

with mock.patch('wdpy.source_item.request', return_value=None):
    from wdpy.connectors.arxiv import ArxivItem

_ATOM = 'http://www.w3.org/2005/Atom'
_ARXIV = 'http://arxiv.org/schemas/atom'
_IDENT = Statement(Snak('P818', ('1309.0951',)))


def _feed(*entries: str) -> str:
    return f'<feed xmlns="{_ATOM}" xmlns:arxiv="{_ARXIV}">{"".join(entries)}</feed>'


def _entry(arxiv_id: str = '1309.0951v2', title: str = 'A Title',
           authors: tuple = ('Smith, John',), doi: str = '10.1234/T') -> str:
    parts = [f'<id>http://arxiv.org/abs/{arxiv_id}</id>', f'<title>{title}</title>']
    for a in authors:
        parts.append(f'<author><name>{a}</name></author>')
    if doi:
        parts.append(f'<arxiv:doi>{doi}</arxiv:doi>')
    return '<entry>' + ''.join(parts) + '</entry>'


def _vals(result: ArxivItem, prop: str) -> list:
    return [s.mainsnak.value for s in result.patch if s.mainsnak.property == prop]


def _quals(result: ArxivItem, prop: str, qprop: str) -> list:
    return [q.value for s in result.patch if s.mainsnak.property == prop
            for q in (s.qualifiers or []) if q.property == qprop]


@mock.patch('wdpy.Snak.type_of', return_value=None)
class TestParse(TestCase):
    def _parse(self, xml: str) -> ArxivItem:
        item = ArxivItem(patch=[_IDENT])
        item.parse(xml)
        return item

    def test_no_entry(self, *_):
        self.assertEqual(self._parse(_feed()).patch, [])

    def test_instance_of(self, *_):
        self.assertIn(('Q13442814',), _vals(self._parse(_feed(_entry())), 'P31'))

    def test_pdf_url(self, *_):
        self.assertEqual(_vals(self._parse(_feed(_entry())), 'P953'),
                         [('https://arxiv.org/pdf/1309.0951',)])

    def test_version_stripped(self, *_):
        r = self._parse(_feed(_entry(arxiv_id='1309.0951v3')))
        self.assertEqual(_vals(r, 'P953'), [('https://arxiv.org/pdf/1309.0951',)])

    def test_title(self, *_):
        self.assertEqual(_vals(self._parse(_feed(_entry(title='Dark Energy Survey'))), 'P1476'),
                         [('Dark Energy Survey', 'en')])

    def test_title_whitespace_normalized(self, *_):
        self.assertEqual(_vals(self._parse(_feed(_entry(title='  Foo   Bar  '))), 'P1476'),
                         [('Foo Bar', 'en')])

    def test_authors(self, *_):
        r = self._parse(_feed(_entry(authors=('Smith, John', 'Doe, Jane'))))
        self.assertEqual(_vals(r, 'P2093'), [('Smith, John',), ('Doe, Jane',)])
        self.assertEqual(_quals(r, 'P2093', 'P1545'), [('1',), ('2',)])

    def test_short_author_skipped(self, *_):
        r = self._parse(_feed(_entry(authors=('AB', 'Smith, John'))))
        self.assertEqual(_vals(r, 'P2093'), [('Smith, John',)])
        self.assertEqual(_quals(r, 'P2093', 'P1545'), [('1',)])

    def test_doi(self, *_):
        self.assertEqual(_vals(self._parse(_feed(_entry(doi='10.1234/test'))), 'P356'),
                         [('10.1234/TEST',)])

    def test_multiple_doi_ignored(self, *_):
        entry = ('<entry><id>http://arxiv.org/abs/1309.0951v1</id><title>T</title>'
                 '<arxiv:doi>10.1/A</arxiv:doi><arxiv:doi>10.1/B</arxiv:doi></entry>')
        self.assertEqual(_vals(self._parse(_feed(entry)), 'P356'), [])

    def test_no_doi(self, *_):
        self.assertEqual(_vals(self._parse(_feed(_entry(doi=''))), 'P356'), [])
