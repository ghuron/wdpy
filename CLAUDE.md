# Project Notes

## Testing

Unit tests use Python's built-in `unittest` framework.

```
python -m unittest discover tests/unit/       # unit tests
python -m unittest discover tests/integration/ # integration tests (hit real APIs)
```

Connector tests (`tests/integration/`) make real network calls and are treated as integration tests.
