import csv
import logging
import re
from contextlib import closing
from decimal import InvalidOperation
from urllib.parse import unquote

from astropy import coordinates

import wd


class Model(wd.Model):
    """Retrieve data from TAP 'endpoint' using 'queries' specified in json-file"""
    _dataset, _ADQL_WRAPPER = {}, 'SELECT * FROM ({}) a WHERE {}'

    @classmethod
    def load(cls, condition=None) -> dict:
        result = {}
        for lines in cls.config('queries'):
            query = ''.join(lines)
            if condition:
                query = cls._ADQL_WRAPPER.format(query, condition)
            Model.tap_query(cls.config('endpoint'), query, result)
        return result

    @classmethod
    def prepare_data(cls, external_id):
        model = super().prepare_data(external_id)
        if external_id in cls._dataset:
            rows = cls._dataset[external_id]
        elif external_id in (result := cls.load('main_id = \'{}\''.format(external_id))):
            rows = result[external_id]
        else:
            return None

        for row in rows:
            for col in row:
                if row[col] and re.search('\\d+$', col):
                    model.construct_snak(row, col)
                    if col.upper() in ['P6257', 'P6258']:  # add J2000 epoch
                        model.input_snaks.append(cls.create_snak('P6259', 'Q1264450'))
        return model

    def construct_snak(self, row, col, new_col=None):
        new_col = (new_col if new_col else col).upper()
        if wd.Wikidata.type_of(new_col) != 'quantity':
            result = Model.get_parent_snak(row[col]) if col == 'p397' else Model.create_snak(new_col, row[col])
        elif col + 'h' not in row or row[col + 'h'] == '':
            result = Model.create_snak(new_col, Model.format_figure(row, col))
        else:
            try:
                high = Model.format_figure(row, col + 'h')
                low = Model.format_figure(row, col + 'l')
                figure = Model.format_figure(row, col)
                result = Model.create_snak(new_col, figure, low, high)
            except InvalidOperation:
                return

        if result is not None:
            if 'mespos' in row:
                result['mespos'] = row['mespos']
            if col + 'u' in row and (unit := self.lut(row[col + 'u'])):
                result['datavalue']['value']['unit'] = 'http://www.wikidata.org/entity/' + unit
            reference = row[col + 'r'] if col + 'r' in row and row[col + 'r'] else None
            reference = row['reference'] if 'reference' in row and row['reference'] else reference
            if reference and (ref_id := Model.parse_url(re.sub('.*(http\\S+).*', '\\g<1>', reference))):
                result['source'] = [ref_id] if 'source' not in result else result['source'] + [ref_id]

        if result := self.enrich_qualifier(result, row['qualifier'] if 'qualifier' in row else row[col]):
            self.input_snaks.append(result)

    @staticmethod
    def tap_query(url, sql, result=None):
        if response := wd.Wikidata.request(url + '/sync', data={'request': 'doQuery', 'lang': 'adql', 'format': 'csv',
                                                                'maxrec': -1, 'query': sql}, stream=True):
            with closing(response) as r:
                reader = csv.reader(r.iter_lines(decode_unicode='utf-8'), delimiter=',', quotechar='"')
                header = next(reader)
                result = {} if result is None else result
                for line in reader:
                    if len(line) > 0:
                        row = {}
                        for i in range(1, len(line)):
                            row[header[i]] = ' '.join(line[i].split()) if isinstance(line[i], str) else line[i]
                        object_id = ' '.join(line[0].split())
                        if object_id in result:
                            result[object_id].append(row)
                        else:
                            result[object_id] = [row]
        return result

    _parents, __PATTERN = None, 'https://www.wikidata.org/wiki/{}#P528\tcatalogue cache miss "{}"'

    @staticmethod
    def get_parent_snak(name: str):
        if Model._parents is None:
            Model._parents = wd.Wikidata.query('SELECT DISTINCT ?c ?i { ?i ^ps:P397 []; wdt:P528 ?c }',
                                               lambda row, _: (row[0].lower(), row[1]))

        name = name[:-1] if re.search('OGLE.+L$', name) else name  # In SIMBAD OGLE names are w/o trailing 'L'
        if name.lower() not in Model._parents:
            import simbad_dap
            if (simbad_id := simbad_dap.Model.get_id_by_name(name)) is None:
                return
            if simbad_id.lower() not in Model._parents:
                if (instance := simbad_dap.Element.get_by_id(simbad_id)) is None:
                    return
                Model._parents[simbad_id.lower()] = instance.qid
            Model._parents[name.lower()] = Model._parents[simbad_id.lower()]
            logging.info(Model.__PATTERN.format(Model._parents[name.lower()], name))
        if snak := Model.create_snak('P397', Model._parents[name.lower()]):
            return {**snak, 'decorators': {'P5997': name}}

    @staticmethod
    def format_figure(row, col):  # SIMBAD-specific way to specify figure precision
        return Model.format_float(row[col], int(row[col + 'p']) if col + 'p' in row and row[col + 'p'] != '' else -1)

    @staticmethod
    def parse_url(url: str) -> str:
        """Try to find qid of the reference based on the url provided"""
        import ads
        import arxiv

        if url and url.strip() and (url := url.split()[0]):  # get text before first whitespace and strip
            for pattern, repl in Model.config('transform').items():
                if (query := unquote(re.sub(pattern, repl, url, flags=re.S))).startswith('P'):
                    if query.startswith('P818='):
                        if item := arxiv.Element.get_by_id(query.replace('P818=', '')):
                            return item.qid
                    elif query.startswith('P819='):
                        if item := ads.Element.get_by_id(query.replace('P819=', '')):
                            return item.qid
                    else:  # fallback
                        try:
                            return wd.Wikidata.search('haswbstatement:' + query)
                        except ValueError as e:
                            logging.warning('Found {} instances of {}'.format(e.args[0], query))


class Element(wd.Element):
    __const, _model, __cache, __existing = None, Model, {}, {}

    def obtain_claim(self, snak):
        snak['decorators'] = snak['decorators'] if 'decorators' in snak else {}
        snak['decorators']['P12132'] = self._claim.db_ref
        if claim := super().obtain_claim(snak):
            if 'mespos' in snak and ('mespos' not in claim or int(claim['mespos']) > int(snak['mespos'])):
                claim['mespos'] = snak['mespos']
            if snak['property'] == 'P1215':
                if 'qualifiers' in snak and 'P1227' in snak['qualifiers'] and snak['qualifiers']['P1227'] == 'Q4892529':
                    claim['rank'] = 'preferred'  # V-magnitude is always preferred
        return claim

    def remove_all_but_one(self, property_id):
        if property_id not in ['P528']:
            super().remove_all_but_one(property_id)

    def post_process(self):
        super().post_process()
        try:
            ra = self.entity['claims']['P6257'][0]['mainsnak']['datavalue']['value']['amount']
            dec = self.entity['claims']['P6258'][0]['mainsnak']['datavalue']['value']['amount']
            tla = coordinates.SkyCoord(ra, dec, frame='icrs', unit='deg').get_constellation(short_name=True)
            if Element.__const is None:
                Element.__const = wd.Wikidata.query(
                    'SELECT DISTINCT ?n ?i {?i wdt:P31/wdt:P279* wd:Q8928; wdt:P1813 ?n}')
            target = None
            for claim in list(self.entity['claims']['P59'] if 'P59' in self.entity['claims'] else []):
                if target or (claim['mainsnak']['datavalue']['value']['id'] != Element.__const[tla]):
                    self.delete_claim(claim)
                else:
                    target = claim
            target = target if target else self.obtain_claim(Model.create_snak('P59', Element.__const[tla]))
            target['references'] = [{'snaks': {'P887': [Model.create_snak('P887', 'Q123764736')]}}]
        except KeyError:
            return


Model.initialize(__file__)
