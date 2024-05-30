#!/usr/bin/python3
import json
import logging

import wd


class Element(wd.Element):
    __cache, _items = {}, {}

    def __init__(self, named_as, qid=None):
        super().__init__(named_as, qid)
        self.award_cleared_qualifiers = []

    @staticmethod
    def load(items: dict):
        Element.get_cache(reset=items)
        Element._items = wd.Wikidata.load(set(filter(lambda x: isinstance(x, str), items.values())))
        return Element._items

    @property
    def entity(self) -> dict:
        if not self._entity:
            self._entity = Element._items[self.qid] if self.qid in Element._items else {'labels': {}, 'claims': {}}
            self.__original = json.dumps(self._entity)
        return self._entity

    def obtain_claim(self, snak):
        if snak is not None:
            if snak['property'] in ['P585', 'P27']:  # date and nationality to qualifiers for award
                award = super().obtain_claim(Model.transform('P166', 'Q112197'))
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
                                claim['qualifiers'] = {'P2241': [wd.Wikidata.create_snak('P2241', 'Q41755623')]}
                return claim

    def post_process(self):
        if 'en' not in self.entity['labels']:
            words = self.external_id.split('(')[0].split()
            if words[0].lower() in ['dalla', 'de', 'del', 'della', 'di', 'du', 'Im', 'le', 'te', 'van', 'von']:
                label = ' '.join([words[-1]] + words[:-1])  # van Allen John -> John van Allen
            else:
                label = ' '.join([words[-1]] + words[1:-1] + [words[0]])  # Pol van de John -> John van de Pol
            self.entity['labels']['en'] = {'value': label, 'language': 'en'}
        super().post_process()

    def get_summary(self):
        return 'basic facts about: ' + self.external_id + ' from Yad Vashem database entry ' + Model.group_id

    def trace(self, message: str, level=20):
        if self.entity is not None and 'id' in self.entity:
            message = 'https://www.wikidata.org/wiki/' + self.entity['id'] + '\t' + message
        Element.info(Model.group_id, self.external_id, message)

    @staticmethod
    def info(book_id, named_as, message):
        logging.info('https://righteous.yadvashem.org/?itemId=' + book_id + '\t"' + named_as + '"\t' + message)


class Model(wd.Model):
    property, db_ref, item, __offset = 'P1979', 'Q77598447', Element, 0

    @classmethod
    def next(cls):
        pattern = 'https://yv360.yadvashem.org/api/Search/GetDataResultsQuery?pageNumber={}&pageSize=10&cardType=card'
        payload = {"filters": {"filters": [{"fieldName": "data_bank", "values": ["righteous"]}]}, "currentTab": {}}
        result, Model.__offset = [], Model.__offset + 1
        if response := wd.Wikidata.request(pattern.format(Model.__offset), json=payload):
            for card in response.json()['cards']:
                result.append(card['id'])
        return result

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

    _rows, group_id = {}, None

    @staticmethod
    def extract(case_id, people: dict):
        Model._rows, Model.group_id = {}, case_id
        if case := wd.Wikidata.request(
                'https://yv360.yadvashem.org/api/Righteous/GetFullDetails?lang=en&id=' + case_id).json():
            result, remaining = {}, []
            for row in case['righteousList']:
                if row['title'] is not None:
                    row['title'] = ' '.join(row['title'].split())  # Fix multiple spaces
                    Model._rows[row['title']] = row['details']  # Save for future parsing
                    if row['title'] in people:  # Find match is wikidata
                        if isinstance(people[row['title']], int):
                            people[row['title']] -= 1  # Counting how many are in Yad Vashem database
                        else:
                            result[row['title']] = people[row['title']]  # Will process as normal match
                            del people[row['title']]  # Remove from the source
                    else:
                        remaining.append(row['title'])  # No match, save for future analysis

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

            return result

        Element.info(case_id, '', 'could not load items, skipping')
        return {}

    @classmethod
    def prepare_data(cls, external_id: str):
        (snak := Model.transform(Model.property, Model.group_id))['qualifiers'] = {'P1810': external_id}
        result = Model(external_id, [snak])
        result.input_snaks.append(cls.transform('P31', 'Q5'))
        result.input_snaks.append(cls.transform('P166', 'Q112197'))
        for element in Model._rows[external_id]:
            if property_id := Model.config('properties', element['title']):
                for val in element['value']:
                    result.input_snaks.append(cls.transform(property_id, val['value']))
        return result


if Model.initialize(__file__):  # if not imported
    QUERY = 'SELECT ?r ?n ?i { ?i wdt:P31 wd:Q5; p:P1979 ?s . ?s ps:P1979 ?r OPTIONAL {?s pq:P1810 ?n}}'
    groups = wd.Wikidata.query(QUERY, Model.process_sparql_row)

    for _id in groups:
        # _id = '4022505'  # uncomment to debug specific group of people
        if (wd_items := Model.extract(_id, groups[_id])) and Element.load(wd_items):
            for name in wd_items:
                Model.get_by_id(name, forced=True).save()
