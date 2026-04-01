from __future__ import annotations
from dataclasses import dataclass, field
from datetime import date, datetime
import json, re
from typing import Any, ClassVar, Dict, Iterable, List, Literal, Optional, Set, Tuple
from wdpy import exec

@dataclass
class Snak:
    """Wrapper around a Wikibase snak."""
    property: str
    value: Optional[Tuple[str, ...]] = None
    snaktype: Literal['value', 'novalue', 'somevalue'] = 'value'
    hash: Optional[str] = field(default=None, compare=False)
    _PROPERTY_TYPES: ClassVar[Optional[Dict[str, str]]] = None
    TODAY: ClassVar[date] = date.today()

    @staticmethod
    def retrieved() -> Snak:
        """Return a P813 (retrieved) snak with today's date."""
        return Snak('P813', (Snak.TODAY.strftime('%Y%m%d'), '11', 'Q1985727'))

    @staticmethod
    def type_of(property_id: str) -> Optional[str]:
        """Return the datatype of a Wikibase property by ID."""
        if Snak._PROPERTY_TYPES is None:
            Snak._PROPERTY_TYPES = {u.rsplit('/', 1)[-1]: _normalize_type(r['t'])
                                    for u, r in (exec('SELECT ?p ?t { ?p '
                                    'wikibase:propertyType ?t }') or {}).items()}
        return Snak._PROPERTY_TYPES.get(property_id)

    @staticmethod
    def parse(data: Dict[str, Any]) -> Snak:
        """Parse a Wikibase JSON snak into a Snak object."""
        p, st = data.get('property', ''), data.get('snaktype', 'value')
        if (dt := data.get('datatype')) and Snak._PROPERTY_TYPES is not None:
            Snak._PROPERTY_TYPES.setdefault(p, dt)
        h = data.get('hash')
        if st != 'value' or not isinstance(dv := data.get('datavalue'), dict):
            return Snak(p, None, st, h)
        v, t = dv.get('value'), dv.get('type')
        if not isinstance(v, dict): return Snak(p, (str(v),) if v is not None else None, hash=h)
        match t:
            case 'wikibase-entityid': res = (v.get('id', ''),)
            case 'quantity': res = (str(v.get('amount', '')), str(v.get('lowerBound', '')),
                                  str(v.get('upperBound', '')), v.get('unit') or '1')
            case 'time':
                _t = _format_time(v.get('time')) or ''
                _p = str(v.get('precision', ''))
                res = (_zero_subprecision(_t, _p), _p,
                       (v.get('calendarmodel') or '').rsplit('/', 1)[-1])
            case 'monolingualtext': res = (v.get('text', ''), v.get('language', ''))
            case _: res = (str(v),)
        return Snak(p, res, hash=h)

    @staticmethod
    def create(property_id: str, value: Any, st: str = 'value') -> Optional[Snak]:
        """Create a Snak object from a property ID and a value."""
        if st != 'value': return Snak(property_id, None, st)
        if value is None: return Snak(property_id, None)
        if isinstance(value, (tuple, list)): return Snak(property_id, tuple(value))
        k, s = Snak.type_of(property_id), str(value).strip()
        if k == 'quantity': return Snak(property_id, (s, '', '', '1')) if s else None
        if k == 'time':
            if isinstance(value, (date, datetime)):
                return Snak(property_id, (value.strftime('%Y%m%d'), '11', 'Q1985727'))
            if not s: return None
            pats = [(r'^(\d{4})(\d{2})(\d{2})$', 11, (1, 2, 3)),
                    (r'^(\d{4})-(\d{1,2})-(\d{1,2})$', 11, (1, 2, 3)),
                    (r'^(\d{1,2})/(\d{1,2})/(\d{4})$', 11, (3, 1, 2)),
                    (r'^(\d{4})-(\d{1,2})$', 10, (1, 2, None)),
                    (r'^(\d{1,2})/(\d{4})$', 10, (2, 1, None)), (r'^(\d{4})$', 9, (1, None, None))]
            for r, pr, idx in pats:
                if m := re.match(r, s):
                    y, mo, d = [m.group(i) if i else None for i in idx]
                    if pr == 11 and d is not None and not int(d):
                        pr = 10
                    return Snak(property_id, (f"{y.zfill(4)}{mo.zfill(2) if mo else '00'}"
                                             f"{d.zfill(2) if d else '00'}",
                                             str(pr), 'Q1985727'))
            return None
        if property_id == 'P356' and s: s = s.upper()
        if k in ('wikibase-item', 'wikibase-property'):
            return Snak(property_id, (s,)) if re.match(r'^[QP]\d+$', s) else None
        return Snak(property_id, (s,)) if s or k == 'string' else None

    def json(self) -> str:
        """Generate a Wikibase-compatible JSON string for the snak."""
        d: Dict[str, Any] = {'snaktype': self.snaktype, 'property': self.property}
        if dt := Snak.type_of(self.property): d['datatype'] = dt
        if self.snaktype == 'value' and self.value:
            v, res = self.value, None
            match dt:
                case 'wikibase-item':
                    res = {'type': 'wikibase-entityid', 'value': {'entity-type': 'item',
                           'id': v[0]}}
                    if v[0][1:].isdigit(): res['value']['numeric-id'] = int(v[0][1:])
                case 'wikibase-property':
                    res = {'type': 'wikibase-entityid', 'value': {'entity-type':
                           'property', 'id': v[0]}}
                case 'string' | 'external-id' | 'url' | 'commons-media' | 'commonsMedia':
                    res = {'type': 'string', 'value': v[0]}
                case 'monolingual-text' | 'monolingualtext':
                    res = {'type': 'monolingualtext', 'value': {'text': v[0],
                           'language': (v[1] if len(v) > 1 else '') or 'mul'}}
                case 'quantity':
                    if v[0]:
                        res = {'type': 'quantity', 'value': {'amount': v[0],
                               'unit': v[3] if len(v) > 3 else '1'}}
                        if len(v) > 1 and v[1]: res['value']['lowerBound'] = v[1]
                        if len(v) > 2 and v[2]: res['value']['upperBound'] = v[2]
                case 'time':
                    time_str, _, suffix = v[0].partition('/')
                    precision = (int(suffix) if suffix.isdigit() else
                                 int(v[1]) if len(v) > 1 and v[1] and v[1].isdigit() else 11)
                    if len(time_str) == 8:
                        mo = time_str[4:6] if time_str[4:6] != '00' else '01'
                        day = time_str[6:] if time_str[6:] != '00' else '01'
                        time_out = f"+{time_str[:4]}-{mo}-{day}T00:00:00Z"
                    else:
                        time_out = time_str
                    res = {'type': 'time', 'value': {'time': time_out,
                           'timezone': 0, 'before': 0, 'after': 0, 'precision': precision}}
                    if len(v) > 2 and v[2]:
                        res['value']['calendarmodel'] = (f"http://www.wikidata.org/entity"
                        f"/{v[2]}" if not v[2].startswith('http') else v[2])
                case _: res = {'type': 'string', 'value': v[0]}
            if res: d['datavalue'] = res
            else: d.pop('datatype', None)
        return json.dumps(d, sort_keys=True)

    def value_matches(self, other: Snak) -> bool:
        """Return True if this snak's value is compatible with other's.

        For time snaks both dates are truncated to the lower of the two
        precisions before comparing, so a month-precision date matches a
        day-precision date for the same month.  The calendar model must also
        agree.  For all other property types exact tuple equality is used.
        """
        if Snak.type_of(self.property) != 'time':
            return self.value == other.value
        if not self.value or not other.value:
            return self.value == other.value
        if self.value[2:] != other.value[2:]:   # calendar mismatch
            return False
        try:
            p_self  = int(self.value[1])
            p_other = int(other.value[1])
        except (ValueError, IndexError):
            return self.value == other.value
        if p_self != p_other:                    # different precision → insert, don't merge
            return False
        return _truncate_date(self.value[0], p_self) == _truncate_date(other.value[0], p_self)

    @staticmethod
    def resolve_redirect(items: Iterable[Iterable[Snak]], redirects: Dict[str, str]) -> None:
        """Resolve redirects for all P248 snaks in items."""
        for s in (s for it in items for s in it if s.property == 'P248'):
            if s.value and (target := redirects.get(s.value[0])):
                s.value = (target,) + s.value[1:]

    @staticmethod
    def can_merge_references(source: Iterable[Snak],
                             target: Iterable[Snak]) -> bool:
        """Check if two sets of reference snaks can be merged."""
        def group(snaks: Iterable[Snak]):
            d: Dict[str, Set[Optional[Tuple[str, ...]]]] = {}
            for s in snaks:
                d.setdefault(s.property, set()).add(s.value)
            return d
        src, dst = group(source), group(target)
        return all((p in {'P248', 'P12132', 'P813'} or Snak.type_of(p) == 'external-id') and
                   (not (s := src.get(p)) or not (t := dst.get(p)) or s == t)
                   for p in set(src) | set(dst))

    @staticmethod
    def try_to_merge_references(source: List[Snak],
                                target: List[Snak]) -> bool:
        """Make target reference snaks identical to source if compatible."""
        if not Snak.can_merge_references(source, target): return False
        target.clear()
        target.extend(source)
        return True

    @staticmethod
    def get_p248_qids(items: Iterable[Iterable[Snak]]) -> Set[str]:
        """Extract QIDs from P248 (stated in) snaks from multiple references."""
        return {s.value[0] for it in items for s in it
                if s.property == 'P248' and s.value and s.value[0]}

def _truncate_date(date_str: str, precision: int) -> str:
    if precision >= 11:
        return date_str
    if precision == 10:
        return date_str[:6] + '00'
    return date_str[:4] + '0000'


def _normalize_type(uri: str) -> str:
    s = uri.rsplit('#', 1)[-1]
    m = {'CommonsMedia': 'commonsMedia', 'GlobeCoordinate': 'globecoordinate',
         'WikibaseEntitySchema': 'entity-schema'}
    return m.get(s, re.sub(r'([a-z])([A-Z])', r'\1-\2', s).lower())


def _zero_subprecision(time_str: str, precision: str) -> str:
    """Zero out date digits below the given precision level."""
    if len(time_str) != 8 or not precision.isdigit():
        return time_str
    p = int(precision)
    if p <= 9:
        return time_str[:4] + '0000'
    if p == 10:
        return time_str[:6] + '00'
    return time_str


def _format_time(raw: Any) -> Optional[str]:
    try:
        return str(raw).split('T')[0].lstrip('+').replace('-', '') if raw else None
    except:
        return None
