from xml.etree import ElementTree
from wdpy import SourceItem, Statement

class ArxivItem(SourceItem):
    def parse(self, text: str, ident: Statement) -> None:
        ns = self._config['namespaces']
        if (entry := ElementTree.fromstring(text).find('w3:entry', ns)) is None:
            if ident.mainsnak.property == 'P818':
                self.prior_ident = ident
            return
        id_text = getattr(entry.find('w3:id', ns), 'text', '') or ''
        if ident.mainsnak.property == 'P818' and '/abs/' not in id_text:
            self.prior_ident = ident
            return
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
