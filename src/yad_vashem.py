#!/usr/bin/python3
import json
import logging
import traceback

import requests

import wd


class Model(wd.Model):
    property = 'P1979'

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

    __endpoint = requests.Session()
    __endpoint.verify = False
    __endpoint.headers.update({'Content-Type': 'application/json'})

    @staticmethod
    def post(method: str, num=0):
        try:
            if response := wd.Wikidata.request('https://righteous.yadvashem.org/RighteousWS.asmx/' + method,
                                               Model.__endpoint, data=Model.config('api', method).format(num)):
                return response.json()
        except json.decoder.JSONDecodeError:
            logging.error('Cannot decode {} response for {}'.format(method, num))

    @staticmethod
    def get_next_chunk(offset):
        page, result = Model.post('GetRighteousList', (offset := 0 if offset is None else offset)), []
        for case_item in page['d']:
            result.append(str(case_item['BookId']))
        return result, offset + len(result)

    _rows, items, group_id = {}, {}, None

    @staticmethod
    def extract(case_id, people: dict):
        Element._rows, Element._items, Element._group_id = {}, {}, case_id
        if case := Model.post('GetPersonDetailsBySession', case_id):
            result, remaining = {}, []
            for row in case['d']['Individuals']:
                if row['Title'] is not None:
                    row['Title'] = ' '.join(row['Title'].split())  # Fix multiple spaces
                    Element._rows[row['Title']] = row['Details']  # Save for future parsing
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
                    Element.info(case_id, list(people.keys())[0], ' probably changed to "{}"'.format(remaining[0]))
                    result[remaining[0]] = list(people.values())[0]
            else:  # Possibly complex case for manual processing, just log
                for named_as in people:
                    if isinstance(people[named_as], int):
                        Element.info(case_id, named_as, ' is ambiguous: ' + str(people[named_as]))
                    else:
                        Element.info(case_id, named_as, 'https://wikidata.org/wiki/' + people[named_as] + ' missing')

            if loaded := wd.Wikidata.load(set(filter(lambda x: isinstance(x, str), result.values()))):
                Element._items = loaded
                return result

        Element.info(case_id, '', 'could not load items, skipping')
        return {}

    @classmethod
    def prepare_data(cls, external_id: str) -> []:
        input_snaks = [Model.create_snak(Model.property, Model.group_id)]
        input_snaks[0]['qualifiers'] = {'P1810': external_id}
        input_snaks.append(cls.create_snak('P31', 'Q5'))
        input_snaks.append(cls.create_snak('P166', 'Q112197'))
        for element in Model._rows[external_id]:
            if property_id := Model.config('properties', element['Title']):
                input_snaks.append(cls.create_snak(property_id, element['Value']))
        return input_snaks


class Element(wd.Element):
    wd.Claim.db_ref = 'Q77598447'

    def __init__(self, named_as, qid):
        super().__init__(named_as, qid)
        self.award_cleared_qualifiers = []

    @property
    def entity(self) -> dict:
        if not self._entity:
            self._entity = Model.items[self.qid] if self.qid in Model.items else {'labels': {}, 'claims': {}}
        return self._entity

    def obtain_claim(self, snak):
        if snak is not None:
            if snak['property'] in ['P585', 'P27']:  # date and nationality to qualifiers for award
                award = super().obtain_claim(Model.create_snak('P166', 'Q112197'))
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
                                claim['qualifiers'] = {'P2241': [Model.create_snak('P2241', 'Q41755623')]}
                return claim

    def post_process(self):
        if 'en' not in self.entity['labels']:
            words = self.external_id.split('(')[0].split()
            if words[0].lower() in ['dalla', 'de', 'del', 'della', 'di', 'du', 'Im', 'le', 'te', 'van', 'von']:
                label = ' '.join([words[-1]] + words[:-1])  # van Allen John -> John van Allen
            else:
                label = ' '.join([words[-1]] + words[1:-1] + [words[0]])  # Pol van de John -> John van de Pol
            self.entity['labels']['en'] = {'value': label, 'language': 'en'}

    def get_summary(self):
        return 'basic facts about: ' + self.external_id + ' from Yad Vashem database entry ' + Model.group_id

    def trace(self, message: str, level=20):
        if self.entity is not None and 'id' in self.entity:
            message = 'https://www.wikidata.org/wiki/' + self.entity['id'] + '\t' + message
        Element.info(Model.group_id, self.external_id, message)

    @staticmethod
    def info(book_id, named_as, message):
        logging.info('https://righteous.yadvashem.org/?itemId=' + book_id + '\t"' + named_as + '"\t' + message)


if Model.initialize(__file__):  # if not imported
    Model.post('BuildQuery')
    QUERY = 'SELECT ?r ?n ?i { ?i wdt:P31 wd:Q5; p:P1979 ?s . ?s ps:P1979 ?r OPTIONAL {?s pq:P1810 ?n}}'
    groups = Model.get_all_items(QUERY, Model.process_sparql_row)

    for _id in groups:
        try:
            # _id = '4022505'  # uncomment to debug specific group of people
            if wd_items := Model.extract(_id, groups[_id]):
                for name in wd_items:
                    Element(name, wd_items[name]).update(Model.prepare_data(name))
        except Exception as e:
            logging.critical('while processing {}. {}'.format(_id, traceback.format_exc()))
