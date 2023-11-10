#!/usr/bin/python3
import json
import logging

import requests

from wd import Wikidata, Element


class YadVashem(Element):
    db_property, db_ref = 'P1979', 'Q77598447'

    def __init__(self, case_id, named_as, qid):
        super().__init__(case_id, qid)
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
            if response := Wikidata.request('https://righteous.yadvashem.org/RighteousWS.asmx/' + method,
                                            YadVashem.__endpoint, data=YadVashem.config['api'][method].format(num)):
                return response.json()
        except json.decoder.JSONDecodeError:
            logging.error('Cannot decode {} response for {}'.format(method, num))

    _entities = None
    _rows = None

    @staticmethod
    def get_items(case_id, people: dict):
        YadVashem._rows = {}
        if case := YadVashem.post('GetPersonDetailsBySession', case_id):
            result, remaining = {}, []
            for row in case['d']['Individuals']:
                if row['Title'] is not None:
                    row['Title'] = ' '.join(row['Title'].split())  # Fix multiple spaces
                    YadVashem._rows[row['Title']] = row['Details']  # Save for future parsing
                    if row['Title'] in people:  # Find match is wikidata
                        if isinstance(people[row['Title']], int):
                            people[row['Title']] -= 1  # Counting how many are in Yad Vashem database
                        else:
                            result[row['Title']] = people[row['Title']]  # Will process as normal match
                            del people[row['Title']]  # Remove from the source
                    else:
                        remaining.append(row['Title'])  # No match, save for future analysis

            if len(people) == 0:  # Copy remaining names to be created
                for named_as in remaining:
                    result[named_as] = None
            elif len(people) == 1 and len(remaining) == 1:  # only one record unmatched - assuming it is the same person
                if isinstance(list(people.values())[0], str):
                    YadVashem.info(case_id, list(people.keys())[0], ' probably changed to "{}"'.format(remaining[0]))
                    result[remaining[0]] = list(people.values())[0]
            else:  # Possibly complex case for manual processing, just log
                for named_as in people:
                    if isinstance(people[named_as], int):
                        YadVashem.info(case_id, named_as, ' is ambiguous: ' + str(people[named_as]))
                    else:
                        YadVashem.info(case_id, named_as, 'https://wikidata.org/wiki/' + remaining[name] + ' missing')

            if loaded := Wikidata.load(list(filter(lambda x: isinstance(x, str), result.values()))):
                YadVashem._entities = loaded
                return result
            else:
                YadVashem.info(case_id, '', 'could not load items, skipping')
                return {}

    @property
    def entity(self) -> dict:
        if not self._entity:
            if self.qid and self.qid in YadVashem._entities:
                self._entity = YadVashem._entities[item.qid]
            else:
                self._entity = {'label': {}, 'claims': {}}
        return self._entity

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

    def prepare_data(self):
        input_snaks = [Element.create_snak(self.db_property, self.external_id)]
        input_snaks[0]['qualifiers'] = {'P1810': self.named_as}
        input_snaks.append(self.create_snak('P31', 'Q5'))
        for element in YadVashem._rows[self.named_as]:
            if element['Title'] in YadVashem.config['properties']:
                input_snaks.append(self.create_snak(YadVashem.config['properties'][element['Title']], element['Value']))
        return input_snaks

    def post_process(self):
        if 'en' not in self.entity['labels']:
            words = self.named_as.split('(')[0].split()
            if words[0].lower() in ['dalla', 'de', 'del', 'della', 'di', 'du', 'Im', 'le', 'te', 'van', 'von']:
                label = ' '.join([words[-1]] + words[:-1])  # van Allen John -> John van Allen
            else:
                label = ' '.join([words[-1]] + words[1:-1] + [words[0]])  # Pol van de John -> John van de Pol
            self.entity['labels']['en'] = {'value': label, 'language': 'en'}


if YadVashem.initialize(__file__):  # if not imported
    YadVashem.post('BuildQuery')
    groups = YadVashem.get_all_items(
        'SELECT ?r ?n ?i { ?i wdt:P31 wd:Q5; p:P1979 ?s . ?s ps:P1979 ?r OPTIONAL {?s pq:P1810 ?n}}',
        YadVashem.process_sparql_row)

    for group_id in groups:
        try:
            # group_id = '4022505'  # uncomment to debug specific group of people
            if wd_items := YadVashem.get_items(group_id, groups[group_id]):
                for name in wd_items:
                    item = YadVashem(group_id, name, wd_items[name] if wd_items[name] else [])
                    item.update(item.prepare_data())
        except Exception as e:
            logging.critical(group_id + ' ' + e.__str__())
