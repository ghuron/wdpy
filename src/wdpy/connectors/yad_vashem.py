from wdpy import Snak, SourceItem, Statement


class YadVashem(SourceItem):
    pass


r = YadVashem.extract(Statement(Snak('P1979', ('5891560', 'Ehret Anna (1903 - 1967 )'))))
pass
