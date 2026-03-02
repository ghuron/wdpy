from http.cookiejar import CookieJar
import io
import json
import logging
from typing import Any, Dict, Iterable, List, Optional, Union
import urllib.parse
from urllib.request import HTTPCookieProcessor, Request, build_opener
import uuid

_action_api_opener = build_opener(HTTPCookieProcessor(CookieJar()))
_csrf_token: str = ''
_login: Optional[str] = None
_psw: Optional[str] = None

def build_request(url: str,
                  params: Optional[Dict[str, Any]] = None,
                  headers: Optional[Dict[str, str]] = None) -> Request:
    actual = {**(headers or {}), 'User-Agent': 'github.com/ghuron/wdpy'}
    data: Optional[bytes] = None
    if params is not None:
        if 'multipart/form-data' in actual.get('Content-Type', '').lower():
            boundary = f'----wdpy{uuid.uuid4().hex}'
            actual['Content-Type'] = f'multipart/form-data; boundary={boundary}'
            data = _encode_multipart(boundary, params)
        else:
            data = urllib.parse.urlencode(params, doseq=True).encode('utf-8')
    return Request(url, headers=actual, data=data)

def _encode_multipart(boundary: str, params: Dict[str, Any]) -> bytes:
    parts: List[str] = []
    for key, value in params.items():
        header = f'Content-Disposition: form-data; name="{key}"'
        body = str(value)
        headers = [header]
        if isinstance(value, list):
            headers.append('Content-Type: text/csv')
            headers[0] += f'; filename="{key}.csv"'
            body = '\r\n'.join(value) + '\r\n' # type: ignore
        parts.append(
            f'--{boundary}\r\n' +
            '\r\n'.join(headers) + 
            '\r\n\r\n' +
            body +
            '\r\n'
        )
    parts.append(f'--{boundary}--\r\n')
    return ''.join(parts).encode('utf-8')

def parse_csv(req: Request) -> Optional[Dict[str, Union[Dict[str, str], List[Dict[str, str]]]]]:
    req.add_header('Accept', 'text/csv')
    try:
        with build_opener().open(req, timeout=120) as r:
            text = r.read().decode('utf-8')
    except Exception as e:
        logging.error('Request failed for %s: %s', req.full_url, e)
        return None

    prefix = 'http://www.wikidata.org/entity/'
    def norm(v: str) -> str:
        s = ' '.join(v.strip().strip('"').split())
        return s[len(prefix):] if s.startswith(prefix) else s
    lines = (s for l in io.StringIO(text) if (s := l.strip('\r\n')))
    try:
        headers = [h.strip() for h in next(lines).split(',')]
    except StopIteration:
        return {}
    is_gid, result = headers[0] == 'gid', {}
    for line in lines:
        vals = [norm(p) for p in line.split(',')]
        row = {h: (vals[i] if i < len(vals) else '') for i, h in enumerate(headers) if i > 0}
        if is_gid:
            result.setdefault(vals[0], []).append(row)
        else:
            result[vals[0]] = row
    return result

def exec(query: str) -> Optional[Dict[str, Union[Dict[str, str], List[Dict[str, str]]]]]:
    return parse_csv(build_request('https://query.wikidata.org/sparql', params={'query': query}))

def fetch_json(action: str, **params: Any) -> Optional[Any]:
    req = build_request('https://www.wikidata.org/w/api.php', params={'action': action,
                                    'format': 'json',
                                    **{
                                        k: json.dumps(v, separators=(',', ':')) if isinstance(v, dict)
                                        else '|'.join(str(item) for item in v)
                                        if isinstance(v, (list, set, tuple)) else str(v)
                                        for k, v in params.items()
                                    },
                                })
    try:
        with _action_api_opener.open(req, timeout=120) as r:
            return json.loads(r.read().decode('utf-8'))
    except Exception as e:
        logging.error('Request failed for %s: %s', 'https://www.wikidata.org/w/api.php', e)
        return None

def logon(login: Optional[str] = None, psw: Optional[str] = None) -> bool:
    def tok(t: str) -> Optional[str]:
        if data := fetch_json('query', meta='tokens', type=t):
            return data.get('query', {}).get('tokens', {}).get(t + 'token') 
    
    global _login, _psw, _csrf_token
    _login, _psw = login or _login, psw or _psw
    if not (_login and _psw):
        logging.error('Missing credentials for logon')
    elif not (lt := tok('login')):
        logging.error('Login token missing in response')
    elif (r := fetch_json('login', lgname=_login, lgpassword=_psw, lgtoken=lt)).get(
            'login', {}).get('result') != 'Success':
        logging.error('Login failed: %s', r)
    elif not (_csrf_token := tok('csrf') or ''):
        logging.error('Edit token missing in response')
    else:
        return True
    return False

def api_write(action: str, **params: Any) -> Optional[Dict[str, Any]]:
    global _csrf_token
    err: Optional[Dict[str, Any]] = None
    for _ in range(2):
        if not _csrf_token and not logon():
            err = {'code': 'notoken'}
            break
        if not (data := fetch_json(action, maxlag = 15, token = _csrf_token, **params)):
            return None
        if not (err := data.get('error')):
            return data
        if 'badtoken' in err.get('code', '') and logon():
            continue
        break
    if err:
        logging.error('Action API error for %s: %s', action, err)
    return None

def search(query: str) -> Optional[List[str]]:
    """CirrusSearch Wikidata query, returns list of item identifiers"""
    if response := fetch_json('query', list='search', srsearch=query):
        return [result.get('title') for result in response.get('query', {}).get('search', [])]
    return None

def haswbstatement(property_id: str, external_id: Any) -> Optional[List[str]]:
    if property_id == 'P1979':
        return search(f'haswbstatement:"P1979={external_id[0]}"[P1810={external_id[1]}]')
    else:
        return search(f'haswbstatement:"{property_id}={external_id}"')

def get_entities(qids: Iterable[str]) -> Optional[Dict[str, Any]]:
    ids = [qid for qid in qids if qid]
    if not ids:
        return None
    if (data := fetch_json('wbgetentities', ids=ids, props='claims|info')) and (
            entities := data.get('entities')):
        return entities
    logging.error('wbgetentities failed for %s', ids)
    return None