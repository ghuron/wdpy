#!/usr/bin/python3
import json
import logging
import sys
from os.path import basename

import requests

from wikidata import WikiData


class YadVashem(WikiData):
    config = WikiData.load_config(__file__)
    db_property, db_ref = 'P1979', 'Q77598447'
    pending = []

    def __init__(self, group_id, named_as):
        super().__init__(group_id)
        self.named_as = named_as
        self.award_cleared_qualifiers = []

    def get_summary(self):
        return 'basic facts about: ' + self.named_as + ' from Yad Vashem database entry ' + self.external_id

    __endpoint = requests.Session()
    __endpoint.verify = False
    __endpoint.headers.update({'Content-Type': 'application/json'})

    @staticmethod
    def post(method: str, num=0):
        try:
            if response := YadVashem.request('https://righteous.yadvashem.org/RighteousWS.asmx/' + method,
                                             YadVashem.__endpoint, data=YadVashem.config['api'][method].format(num)):
                return response.json()
        except json.decoder.JSONDecodeError:
            logging.error('Cannot decode {} response for {}'.format(method, num))

    @staticmethod
    def get_next_chunk(offset):
        offset = 0 if offset is None else offset
        page = YadVashem.post('GetRighteousList', offset)
        result = []
        for case_item in page['d']:
            result.append(str(case_item['BookId']))
        return result, offset + len(result)

    @staticmethod
    def process_sparql_row(new, result):
        if new[0] not in result:
            return new[0], {new[1]: new[2]}  # create a new case
        elif new[1] not in result[new[0]]:
            return new[0], {**result[new[0]], new[1]: new[2]}  # add person to existing case
        elif isinstance(result[new[0]][new[1]], int):
            return new[0], {**result[new[0]], new[1]: 1 + result[new[0]][new[1]]}  # increment duplication count
        else:
            return new[0], {**result[new[0]], new[1]: 2}  # convert to duplication count

    def obtain_claim(self, snak):
        if snak is not None:
            if snak['property'] in ['P585', 'P27']:  # date and nationality to qualifiers for award
                award = super().obtain_claim(self.create_snak('P166', 'Q112197'))
                award['qualifiers'] = {} if 'qualifiers' not in award else award['qualifiers']
                if snak['property'] not in award['qualifiers'] or snak['property'] not in self.award_cleared_qualifiers:
                    self.award_cleared_qualifiers.append(snak['property'])  # clear each qualifier only once
                    award['qualifiers'][snak['property']] = []
                award['qualifiers'][snak['property']].append(snak)
            elif claim := super().obtain_claim(snak):
                if snak['property'] in ['P569', 'P570']:  # birth/death date statement only
                    if 'rank' not in claim:  # unspecified rank means it was just created
                        if snak['datavalue']['value']['precision'] == 11:  # date precision on the day level
                            if snak['datavalue']['value']['time'].endswith('-01-01T00:00:00Z'):  # January 1
                                claim['rank'] = 'deprecated'  # db artefact, only year known for sure
                                claim['qualifiers'] = {'P2241': [self.create_snak('P2241', 'Q41755623')]}
                return claim

    def trace(self, message: str, level=20):
        if self.entity is not None and 'id' in self.entity:
            message = 'https://www.wikidata.org/wiki/' + self.entity['id'] + '\t' + message
        YadVashem.info(self.external_id, self.named_as, message)

    @staticmethod
    def info(book_id, named_as, message):
        logging.info('https://righteous.yadvashem.org/?itemId=' + book_id + '\t"' + named_as + '"\t' + message)

    def prepare_data(self, source=None):
        self.input_snaks = [WikiData.create_snak(self.db_property, self.external_id)]
        self.input_snaks[0]['qualifiers'] = {'P1810': self.named_as}
        self.input_snaks.append(self.create_snak('P31', 'Q5'))
        for element in source:
            if element['Title'] in YadVashem.config['properties']:
                self.input_snaks.append(
                    self.create_snak(YadVashem.config['properties'][element['Title']], element['Value']))

    def post_process(self):
        if 'labels' not in self.entity:
            self.entity['labels'] = {}
        if 'en' not in self.entity['labels']:
            words = self.named_as.split('(')[0].split()
            if words[0].lower() in ['dalla', 'de', 'del', 'della', 'di', 'du', 'Im', 'le', 'te', 'van', 'von']:
                label = ' '.join([words[-1]] + words[:-1])  # van Allen John -> John van Allen
            else:
                label = ' '.join([words[-1]] + words[1:-1] + [words[0]])  # Pol van de John -> John van de Pol
            self.entity['labels']['en'] = {'value': label, 'language': 'en'}

    def save(self):
        if 'id' in self.entity:
            super().save()
        else:
            self.pending.append(self)

    def create(self):
        super().save()

    @staticmethod
    def create_pending(remaining):
        items_absent_in_yv = False
        if remaining is not None:
            for name in list(remaining):
                if isinstance(remaining[name], int):
                    YadVashem.info(item_id, name, ' is ambiguous: ' + str(remaining[name]))
                else:
                    YadVashem.info(item_id, name, 'https://wikidata.org/wiki/' + remaining[name] + ' is missing')
                    items_absent_in_yv = True

        for new_item in YadVashem.pending:
            if items_absent_in_yv:
                new_item.trace('was not created (see above)')
            else:
                new_item.create()


if sys.argv[0].endswith(basename(__file__)):  # if not imported
    YadVashem.logon(sys.argv[1], sys.argv[2])
    YadVashem.post('BuildQuery')
    wd_items = YadVashem.get_all_items(
        'SELECT ?r ?n ?i { ?i wdt:P31 wd:Q5; p:P1979 ?s . ?s ps:P1979 ?r OPTIONAL {?s pq:P1810 ?n}}',
        YadVashem.process_sparql_row)

    for item_id in wd_items:
        # item_id = '6658068'  # uncomment to debug specific group of people
        qids = wd_items[item_id].values() if wd_items[item_id] is not None else []
        if (group := YadVashem.load_items(list(filter(lambda x: isinstance(x, str), qids)))) is None:
            continue
        YadVashem.pending = []
        case = YadVashem.post('GetPersonDetailsBySession', item_id)
        for row in case['d']['Individuals']:
            if row['Title'] is not None:
                row['Title'] = ' '.join(row['Title'].split())
                if row['Title'] in wd_items[item_id] and isinstance(wd_items[item_id][row['Title']], int):
                    wd_items[item_id][row['Title']] -= 1
                    continue  # multiple values for the same key - skipping updates

                item = YadVashem(item_id, row['Title'])
                if row['Title'] in wd_items[item_id] and wd_items[item_id][row['Title']] in group:
                    item.entity = group[wd_items[item_id][row['Title']]]
                    del wd_items[item_id][row['Title']]  # consider it processed

                item.prepare_data(row['Details'])
                item.update()

        YadVashem.create_pending(wd_items[item_id])
