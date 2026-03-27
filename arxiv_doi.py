import logging
from wdpy import Item, logon

if logon('Ghuron@Ghuron', '1t0ev93e72p8neccgquoo87jllr49mli'):
    logging.basicConfig(level=logging.INFO)
    item = Item('Q68126267').sync().write('test')