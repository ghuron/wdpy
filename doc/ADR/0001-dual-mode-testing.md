# ADR 001: Hybrid Mock/Live Testing Strategy

## Context

Unit tests for external connectors must run in two modes: Mocked for CI speed and Live for manual API verification. The switch between modes is achieved solely by commenting out the `@mock.patch` line, requiring tests to be resilient to the absence of mock objects.

## Decision

Configure mock behavior, such as `return_value`, directly in the `@patch` decorator attributes. If you need interaction assertions like `assert_called_once_with`, use a named mock argument with `None` default and wrap the assertion in an `if` block to ensure it is skipped during live execution. Otherwise Use `*_` as placeholder for optional unused mock arguments.

## Consequences

Tests become hybrid, allowing on-demand integration checks without code changes. Test bodies remain clean as setup logic moves to decorators. However, developers must strictly ensure all mock arguments are optional to avoid runtime errors when running in live mode.

## Code Pattern

```python
@mock.patch('wdpy.core.request', return_value = 'id,e\nQ353673,Q49836\nQ854830,Q49836')
def test_exec_csv(self, mock:Optional[mock.MagicMock] = None):
    query = 'SELECT ?id ?e { ?id ^wdt:P527 wd:Q16661132; wdt:P1344 ?e } ORDER BY ?id'
    self.assertEqual(wdpy.core.exec(query), 
                     {'Q353673': {'e': 'Q49836'}, 'Q854830': {'e': 'Q49836'}})
    if mock: mock.assert_called_once_with(
        'https://query.wikidata.org/sparql', headers={'Accept': 'text/csv'}, params={'query': query}
    )

@mock.patch('wdpy.core.request', return_value = None)
def test_exec_fail(self, *_):
    self.assertIsNone(wdpy.core.exec('SELECT ...'))
