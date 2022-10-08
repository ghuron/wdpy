#!/usr/bin/python3
import sys
from os.path import basename

import requests

from wikidata import WikiData


class YadVashem(WikiData):
    db_property = 'P1979'
    db_ref = 'Q77598447'
    endpoint = requests.Session()
    endpoint.headers.update({'Content-Type': 'application/json'})
    pending = []

    def __init__(self, group_id, named_as):
        super().__init__(group_id)
        self.named_as = named_as
        self.award_cleared_qualifiers = []

    def get_summary(self):
        return 'basic facts about: ' + self.named_as + ' from Yad Vashem database entry ' + self.external_id

    @staticmethod
    def post(method, params):
        return YadVashem.endpoint.post('https://righteous.yadvashem.org/RighteousWS.asmx/' + method,
                                       data='{lang:"eng",' + params + '}').json()

    @staticmethod
    def get_next_chunk(o):
        o = 0 if o is None else o
        page = YadVashem.post('GetRighteousList',
                              'uniqueId:"987",searchType:"righteous_only",rowNum:' + str(o) + ',sort:{dir:"",field:""}')
        result = []
        for case_item in page['d']:
            result.append(str(case_item['BookId']))
        return result, o + len(result)

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

    def trace(self, message):
        if self.entity is not None and 'id' in self.entity:
            message = 'https://www.wikidata.org/wiki/' + self.entity['id'] + '\t' + message
        YadVashem.info(self.external_id, self.named_as, message)

    @staticmethod
    def info(book_id, named_as, message):
        print('https://righteous.yadvashem.org/?itemId=' + book_id + '\t"' + named_as + '"\t' + message)

    def parse_input(self, source=None):
        mapping = {
            'ACCOUNTANT': 326653, 'ACTIVIST': 15253558, 'ACTOR': 33999, 'ACTRESS': 33999, 'ADMINISTRATOR': 16532929,
            'ADVISOR': 2994387, 'AGENT': 109555060, 'AGRICULTEUR': 131512, 'AGRICULTURAL ENGINEER': 10272925,
            'Agriculture': 11451, 'AGRICULTURIST': 1781198, 'AGRONOMIST': 1781198, 'AIR FORCE OFFICER': 19934710,
            'ALBANIA': 222, 'ANTHROPOLOGIST': 4773904, 'ARCHAEOLOGIST': 3621491, 'ARCHITECT': 42973,
            'ARCHITECTURAL ENGINEER': 3151019, 'ARMENIA': 399, 'ARMY OFFICER': 38239859, 'ART HISTORIAN': 1792450,
            'ARTIST': 483501, 'ASPHYXIATION': 193840, 'ASSISTANT': 23835475, 'ASSISTANT DIRECTOR': 1757008,
            'ASSISTANT PROFESSOR': 5669847, 'Atheist': 7066, 'ATHLETE': 2066131, 'Auctioneer': 2743689, 'AUSTRIA': 40,
            'AUTHOR': 482980, 'BAILIFF': 10970991, 'BAKER': 160131, 'BALLET TEACHER': 55092822,
            'BANK MANAGER': 32947888, 'BANKER': 806798, 'BAPTIST PROTESTANT': 93191, 'BARBER': 107198,
            'BEADLE': 2692509, 'BEEKEEPER': 852389, 'BELARUS': 184, 'BELGIUM': 31, 'BIOLOGIST': 864503,
            'BIOLOGY TEACHER': 104828277, 'BLACKSMITH': 1639825, 'BOMBARDMENT': 2380335, 'BOOKBINDER': 1413170,
            'Bookkeeper': 56224063, 'BOSNIA AND HERZEGOVINA': 225, 'BRAZIL': 155, 'BRICKLAYER': 327321,
            'BUILDING CONTRACTOR': 63755054, 'BUILDING ENGINEER': 21778977, 'BUILDING MANAGER': 1090219,
            'BULGARIA': 219, 'BURNING ALIVE': 468455, 'BUS DRIVER': 829020, 'BUSINESSMAN': 43845, 'BUTCHER': 329737,
            'BUYER': 1308239, 'CALVINIST': 101849, 'CANCER': 12078, 'CAPTAIN': 19100, 'car accident': 9687,
            'CAREGIVER': 553079, 'CARETAKER': 96943145, 'CARPENTER': 154549, 'CASHIER': 1735282, 'CATHOLIC': 9592,
            'CELLIST': 13219637, 'CHARITY': 1077064, 'CHARWOMAN': 5086996, 'CHEF': 3499072,
            'CHEMICAL ENGINEER': 7888586, 'CHEMIST': 593644, 'CHEMISTRY TEACHER': 104693589, 'CHILD': 7569,
            'CHILE': 298, 'CHIMNEY SWEEP': 506126, 'CHINA': 148, 'CHRISTIAN': 34651, 'CIGAR MAKER': 70145229,
            'CIVIL ENGINEER': 13582652, 'CIVIL SERVANT': 212238, 'CLEANER': 1760141, 'CLERGYMAN': 3315492,
            'CLERK': 738142, 'COACHBUILDER': 1734300, 'COLONEL': 104680, 'COMMANDER': 1780020,
            'COMMERCIAL AGENT': 705908, 'COMMERCIAL DIRECTOR': 3029403, 'COMPANY DIRECTOR': 107100767,
            'COMPOSER': 36834, 'COMPTROLLER': 673633, 'CONCIERGE': 2664461, 'CONDUCTOR': 158852,
            'CONFECTIONER': 2992505, 'CONFESSING CHURCH': 161362, 'CONSTRUCTION WORKER': 811122, 'CONSUL': 207978,
            'CONTRACTOR': 15982656, 'CONTROLLER': 1129230, 'Convert to Judaism': 9268, 'COOK': 156839, 'COOPER': 38883,
            'CORRESPONDENT': 1155838, 'COUNCIL MEMBER': 708492, 'COURIER': 848466, 'CRAFTSMAN': 57260825,
            'CROATIA': 224, 'CUBA': 241, 'CZECH REPUBLIC': 213, 'DAIRY FARMER': 23666894, 'DAIRY WORKER': 12326565,
            'DANCER': 5716684, 'DEALER': 18242306, 'DECORATOR': 22811707, 'DENMARK': 35, 'DENTAL TECHNICIAN': 144075,
            'DENTIST': 27349, 'DEPUTY MAYOR': 581817, 'DERMATOLOGIST': 2447386, 'DETECTIVE': 842782,
            'DIAMOND CUTTER': 1208613, 'DIAMOND DEALER': 55652056, 'DIPLOMAT': 193391, 'DIRECTOR': 3455803,
            'DIRECTRICE': 16033672, 'DISABLED VETERAN': 5281208, 'DISEASE': 12136, 'DIVER': 1866686,
            'DOCK WORKER': 21401846, 'DOCTOR': 4618975, 'DOCTOR OF ECONOMICS': 17281072, 'DOCTOR OF MEDICINE': 913404,
            'DOMESTIC WORKER': 54128, 'DRESSMAKER': 2034021, 'DRIVER': 352388, 'DROGIST': 16101735, 'DROWNING': 506616,
            'DRUMMER': 386854, 'ECONOMICS TEACHER': 80027163, 'ECONOMIST': 188094, 'ECUADOR': 736, 'EDITOR': 1607826,
            'EDUCATOR': 974144, 'EGYPT': 79, 'EL SALVADOR': 792, 'ELECTRICAL ENGINEER': 1326886, 'ELECTRICIAN': 165029,
            'Electrotechnician': 1327627, 'EMBROIDERER': 1509440, 'EMPLOYEE': 703534, 'EMPLOYER': 3053337,
            'ENGINEER': 81096, 'ENGLISH TEACHER': 101444631, 'ENGRAVER': 329439, 'ESTONIA': 191,
            'EVANGELICAL PROTESTANT': 194253, 'EXCISE OFFICER': 84094525, 'EXECUTION': 3966286,
            'Execution - shot': 15747939, 'EXECUTIVE': 978044, 'EXPLOSION': 179057, 'EYE SURGEON': 15059883,
            'FACTORY EMPLOYEE': 55070649, 'FACTORY WORKER': 87285943, 'FARMER': 131512, 'FASHION DESIGNER': 3501317,
            'Female': 6581072, 'FINANCIAL ADVISER': 683476, 'FINLAND': 33, 'FIREMAN': 1147709, 'FISHMONGER': 550594,
            'FITTER': 2828885, 'FLORIST': 16735601, 'FOREMAN': 79288, 'FOREST OWNER': 978061,
            'FOREST WORKER': 12335817, 'FORESTER': 1895303, 'FRANCE': 142, 'FRENCH TEACHER': 101444601,
            'FURNITURE MAKER': 19382184, 'FURNITURE MERCHANT': 108134083, 'FURRIER': 2295938, 'GARAGE OWNER': 96686209,
            'GARDENER': 758780, 'GAS CHAMBERS': 25391961, 'GENDARME': 19801630, 'GENERAL': 2608441,
            'GENERAL SECRETARY': 6501749, 'Geodesist': 294126, 'GEOLOGIST': 520549, 'GEORGIA': 230,
            'GERMAN TEACHER': 101444594, 'GERMANY': 183, 'GOLDSMITH': 211423, 'GOVERNESS': 1540278, 'GRADUATE': 332763,
            'GRAFIK': 5592483, 'GRAPHIC ARTIST': 1925963, 'GRAVEDIGGER': 537575, 'Great Britain': 145, 'GREECE': 41,
            'GREEK CATHOLIC': 1546359, 'GREEK ORTHODOX': 7970362, 'GREENGROCER': 104383916, 'GROCER': 5669609,
            'GROOM': 1455520, 'GUIDE': 14290559, 'GYMNASTICS TEACHER': 60583668, 'HABERDASHER': 17305512,
            'HABERDASHERY': 1491689, 'HAIRDRESSER': 55187, 'HANDICRAFT': 877729, 'HANGING': 175111, 'HATTER': 1639239,
            'HEAD GARDENER': 5689301, 'HEAD OF DEPARTMENT': 4182948, 'HEART ATTACK': 181754,
            'HIGH SCHOOL STUDENT': 15360275, 'HIGH SCHOOL TEACHER': 5758653, 'HISTORIAN': 201788,
            'HISTORY TEACHER': 58209937, 'HORSE TRAINER': 466640, 'HORTICULTURIST': 3140857,
            'HOSPITAL WORKER': 107363294, 'HOTEL': 27686, 'HOTEL MANAGER': 1631120, 'HOTEL OWNER': 105756071,
            'HOUSEKEEPER': 2596569, 'HOUSEWIFE': 38126150, 'HOUSEWORK': 3406668, 'HUNGARY': 28, 'HUNTER': 1714828,
            'ILLUSTRATOR': 644687, 'INDONESIA': 252, 'INDUSTRIALIST': 6606110, 'INNKEEPER': 16513769,
            'INSPECTOR': 27214348, 'INSTRUCTOR': 16358610, 'INSTRUMENT MAKER': 2341443, 'INSURANCE': 43183,
            'INTELLIGENCE OFFICER': 5121444, 'INTERNIST': 15924224, 'IRELAND': 27, 'ITALY': 38, 'JANITOR': 84312746,
            'JAPAN': 17, 'Jehova witness': 35269, 'JEWELER': 336221, 'JEWISH': 9268, 'JOURNALIST': 1930187,
            'JUDGE': 16533, 'JURIST': 185351, 'Killed in combat': 210392, 'LABORATORY TECHNICIAN': 1483433,
            'LAND OWNER': 1483709, 'LANDLORD': 618532, 'LATHE OPERATOR': 28933489, 'LATIN TEACHER': 101444703,
            'LATVIA': 211, 'LAW CLERK': 883231, 'LAW STUDENT': 105492608, 'LAWYER': 40348, 'LECTURER': 1569495,
            'LIBRARIAN': 182436, 'LITERATURE TEACHER': 99398506, 'LITHUANIA': 37, 'LIVESTOCK DEALER': 1669408,
            'LOCK KEEPER': 2420502, 'LOCKSMITH': 3479990, 'LUTHERAN PROTESTANT': 75809, 'LUXEMBOURG': 32,
            'MACEDONIA': 221, 'MACHINIST': 196721, 'MAGISTRAT': 16009129, 'MAID': 833860, 'Male': 6581097,
            'Manager': 2462658, 'MANAGING DIRECTOR': 19940089, 'MANUAL WORKER': 12713481, 'MANUFACTURER': 13235160,
            'MASS KILLING': 750215, 'MATH TEACHER': 42418493, 'MATHEMATICIAN': 170790, 'MATRON': 1396008,
            'MAYOR': 30185, 'MECHANIC': 327029, 'MECHANICAL ENGINEER': 1906857, 'MEDICAL STUDENT': 21263917,
            'MERCHANT': 215536, 'MESSENGER': 54402883, 'METALLURGICAL ENGINEER': 27947380, 'METALWORKER': 15980591,
            'MIDWIFE': 185196, 'MILITARY COMMANDER': 11545923, 'MILITARY MAN': 6857695, 'MILKMAN': 278138,
            'MILLER': 694116, 'MINER': 820037, 'MINING ENGINEER': 18524075, 'MINISTER': 1423891, 'MODISTE': 18199649,
            'MOLDOVA': 217, 'MONK': 733786, 'MONTENEGRO': 236, 'MUNICIPAL CLERK': 883211,
            'MUNICIPAL EMPLOYEE': 98058553, 'MUSIC TEACHER': 2675537, 'MUSICIAN': 639669, 'MUSICOLOGIST': 14915627,
            'MUSLIM': 432, 'NANNY': 936969, 'Naval officer': 10669499, 'NEEDLEWORKER': 28966333, 'NEUROLOGIST': 783906,
            'NEWSPAPER EDITOR': 17351648, 'NIGHT WATCHMAN': 7973168, 'NORWAY': 20, 'NOTARY': 189010,
            'NOTARYS ASSISTANT': 319341, 'NUN': 191808, 'NURSE': 186360, 'NURSERY SCHOOL TEACHER': 17420002,
            'OFFICE CLERK': 61944332, 'OFFICE MANAGER': 1021671, 'OFFICE WORKER': 64275409, 'OFFICER': 61022630,
            'OFFICIAL': 599151, 'OPERA SINGER': 2865819, 'OPERATOR': 1970438, 'OPTICIAN': 1996635,
            'OPTOMETRIST': 3354501, 'ORGANISTE': 3356167, 'ORTHODOX': 35032, 'OTOLARYNGOLOGIST': 2854916,
            'PAINTER': 1028181, 'PAPER FACTORY OWNER': 88283675, 'PAPERMAKER': 3362826, 'PARAMEDIC': 330204,
            'PARTY OFFICIAL': 88190406, 'PASTOR': 152002, 'PEASANT': 838811, 'PEDDLER': 638172, 'PEDIATRICIAN': 1919436,
            'PENSIONER': 1749879, 'PERU': 419, 'PHARMACIST': 105186, 'PHOTOGRAPHER': 33231,
            'PHOTOGRAPHIC STUDIO': 672070, 'PHYSICIAN': 39631, 'PHYSICIST': 169470, 'PHYSICS TEACHER': 101445630,
            'PIANIST': 486748, 'PIANO TEACHER': 17303149, 'PIANO TUNER': 1191329, 'PILOT': 2095349,
            'PLASTERER': 15284879, 'PLUMBER': 252924, 'POET': 49757, 'POLAND': 36, 'POLICE OFFICER': 384593,
            'POLITICAL ACTIVIST': 11499147, 'PORTER': 1509714, 'PORTUGAL': 45, 'POST OFFICE MANAGER': 2106113,
            'POSTAL CLERK': 17093221, 'POSTAL WORKER': 7234072, 'POTTER': 3400050, 'PREACHER': 432386,
            'PRINCIPAL': 7245047, 'PRINTER': 175151, 'PROFESSOR': 121594, 'PROFESSOR OF PHILOSOPHY': 26973007,
            'PROJECTIONIST': 1415369, 'PROSECUTOR': 600751, 'PROSTITUTE': 14915751, 'PROTESTANT': 23540,
            'PSYCHIATRIST': 211346, 'PSYCHOLOGIST': 212980, 'PUBLICIST': 4178004, 'PUBLISHER': 2516866,
            'QUAKER': 170208, 'RADIOLOGIST': 18245236, 'RAILWAY OFFICIAL': 108396200, 'RAILWAYMAN': 40126298,
            'REAL ESTATE OWNER': 47072988, 'REGISTRAR': 58371759, 'RESERVIST': 561904, 'RESTAURANT MANAGER': 96093243,
            'RESTAURANT OWNER': 43563252, 'REVEREND': 841594, 'ROAD WORKER': 19397650, 'ROMAN CATHOLIC': 597526,
            'ROMANIA': 218, 'ROOFER': 552378, 'RUSSIA': 159, 'RUSSIAN ORTHODOX': 60995, 'RUSSIAN TEACHER': 101444477,
            'SADDLER': 1760988, 'SAILOR': 476246, 'SALES REPRESENTATIVE': 685433, 'SAUSAGE MAKER': 108300820,
            'SCHOLAR': 2248623, 'SCHOOL TEACHER': 2251335, 'SCHOOLMASTER': 1459149, 'SCULPTOR': 1281618,
            'SEAMAN': 707995, 'SEAMSTRESS': 67205775, 'SECRETARY': 80687, 'SENATOR': 15686806, 'SERBIA': 403,
            'SERGEANT': 157696, 'SEVENTH-DAY ADVENTIST CHURCH': 104319, 'SEXTON': 4129937, 'SHEPHERD': 81710,
            'SHOP ASSISTANT': 5803091, 'SHOT': 2140674, 'SHOT TO THE HEAD': 15824243, 'SILVERSMITH': 2216340,
            'SINGER': 177220, 'SLAUGHTERER': 99689108, 'SLOVAKIA': 214, 'SLOVENIA': 215, 'SOCIAL WORKER': 7019111,
            'SOLICITOR': 14284, 'SPAIN': 29, 'STABBING': 464643, 'STENOGRAPHER': 18810149, 'STEWARD': 14515331,
            'STOCKBROKER': 4182927, 'STOMATOLOGIST': 16061291, 'STREET SWEEPER': 17309512, 'STUDENT': 48282,
            'STUDENT MEDICINE': 27720315, 'SUPERINTENDENT': 2296367, 'SUPERVISOR': 1240788, 'SURGEON': 774306,
            'SURVEYOR': 63489892, 'SWEDEN': 34, 'SWITZERLAND': 39, 'TAILOR': 242468, 'TANNER': 365461,
            'TAX COLLECTOR': 1139055, 'TAX CONSULTANT': 2346491, 'TAX INSPECTOR': 24041571, 'TAXI DRIVER': 2961580,
            'TEACHER': 37226, 'TEACHING': 352842, 'TECHNICIAN': 5352191, 'TECHNOLOGIST': 12376667,
            'TELEGRAPHIST': 2024200, 'TELEPHONE OPERATOR': 47319622, 'TEXTILE ENGINEER': 84315222,
            'TEXTILE MERCHANT': 66095885, 'TEXTILE WORKER': 66035178, 'THE NETHERLANDS': 55, 'THEOLOGIAN': 1234713,
            'TICKET CONTROLLER': 389565, 'TINSMITH': 22671340, 'TOBACCO GROWER': 55331964, 'TORTURE': 72672186,
            'TRANSLATOR': 333634, 'TRANSPORTATION WORKER': 91143339, 'TRAVELER': 22813352, 'TREASURER': 388338,
            'TRUCK DRIVER': 508846, 'Trumpeter': 12377274, 'TURKEY': 43, 'TURNER': 1716419, 'TUTOR': 901222,
            'TYPESETTER': 4108101, 'TYPHUS': 160649, 'TYPIST': 58487031, 'UKRAINE': 212, 'UNEMPLOYED': 28790169,
            'Union leader': 50768646, 'UNITARIAN': 106687, 'United States of America': 30,
            'UNIVERSITY PROFESSOR': 16481664, 'UNIVERSITY STUDENT': 315247, 'UPHOLSTERER': 23754740,
            'UROLOGIST': 17345122, 'VALET DE CHAMBRE': 1723189, 'VETERINARIAN': 202883, 'VIETNAM': 881,
            'VIOLINIST': 1259917, 'WAITER': 157195, 'Warehouse worker': 92204276, 'WAREHOUSEMAN': 1391362,
            'WATCHMAKER': 157798, 'WATER CARRIER': 965417, 'WEAVER': 437512, 'WELDER': 836328, 'WET NURSE': 472898,
            'WHOLESALE MERCHANT': 29051324, 'WINE MERCHANT': 2556132, 'WORKER': 327055, 'WOUNDS': 16861372,
            'WRITER': 36180, 'YUGOSLAVIA': 36704}
        properties = {'recognition_date': 'P585', 'nationality': 'P27', 'gender': 'P21', 'cause_of_death': 'P509',
                      'religion': 'P140', 'date_of_death': 'P570', 'date_of_birth': 'P569', 'profession': 'P106'}

        self.input_snaks = [WikiData.create_snak(self.db_property, self.external_id)]
        self.input_snaks[0]['qualifiers'] = {'P1810': self.named_as}
        self.input_snaks.append(self.create_snak('P31', 'Q5'))
        for element in source:
            if element['Title'] in properties:
                value = 'Q' + str(mapping[element['Value']]) if element['Value'] in mapping else element['Value']
                self.input_snaks.append(self.create_snak(properties[element['Title']], value))

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
    YadVashem.post('BuildQuery',
                   'uniqueId:"987",strSearch:"",newSearch:true,clearFilter:true,searchType:"righteous_only"')
    wd_items = YadVashem.get_all_items(
        'SELECT ?r ?n ?i { ?i wdt:P31 wd:Q5; p:P1979 ?s . ?s ps:P1979 ?r OPTIONAL {?s pq:P1810 ?n}}',
        lambda new, existing: {new[0]: new[1]} if len(existing) == 0 else
        {**existing, new[0]: new[1]} if new[0] not in existing else
        {**existing, new[0]: existing[new[0]] + 1} if isinstance(existing[new[0]], int) else
        {**existing, new[0]: 2})

    for item_id in wd_items:
        # item_id = '6658068'  # uncomment to debug specific group of people
        qids = wd_items[item_id].values() if wd_items[item_id] is not None else []
        if (group := YadVashem.load_items(list(filter(lambda x: isinstance(x, str), qids)))) is None:
            continue
        YadVashem.pending = []
        case = YadVashem.post('GetPersonDetailsBySession', 'bookId:"' + item_id + '"')
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

                item.parse_input(row['Details'])
                item.update()

        YadVashem.create_pending(wd_items[item_id])
