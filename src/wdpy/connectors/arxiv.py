from xml.etree import ElementTree
from wdpy import SourceItem

class ArxivItem(SourceItem):
    def parse(self, text: str) -> bool:
        ns = self._config['namespaces']
        if (entry := ElementTree.fromstring(text).find('w3:entry', ns)) is None:
            self.patch = []
            return True
        id_text = getattr(entry.find('w3:id', ns), 'text', '') or ''
        arxiv_id = id_text.split('/')[-1].split('v')[0]
        self.add_claim('P31', 'Q13442814')
        if arxiv_id: self.add_claim('P953', 'https://arxiv.org/pdf/' + arxiv_id)
        t = entry.find('w3:title', ns)
        if t is not None and (title := ' '.join((t.text or '').split())):
            self.add_claim('P1476', title, 'en')
        i = 0
        for a in entry.findall('w3:author/w3:name', ns):
            if len(name := (a.text or '').strip()) > 3:
                self.add_claim('P2093', name).set_qualifier('P1545', str(i := i + 1))
        if len(doi := entry.findall('arxiv:doi', ns)) == 1 and doi[0].text:
            self.add_claim('P356', doi[0].text.upper())
        return True

# r = ArxivItem.extract(Statement(Snak('P818', ('1309.0951',))))
# r = ArxivItem.extract(Statement(Snak('P356', ('10.4171/161',))))
# pass
