**wd.py** is a framework for building bots that periodically synchronize Wikidata with external sources.

Its primary goal is to simplify both the creation of new **connectors** and the maintenance of existing ones. Designed to work alongside the community, the framework respects human contributions and ensures bots play nicely with manual editors.

Beyond standard synchronization, it exposes a flexible API that allows developers to write custom maintenance scripts and implement complex workflows.

# Who it's for

wd.py is aimed at developers who operate or want to build Wikidata bots. You should be comfortable with Python and familiar with Wikidata's data model — items, statements, properties, references, ranks, and qualifiers.

# What problem it solves

Wikidata items often carry identifiers that link them to external databases (arXiv, ORCID, NASA ADS, etc.). Keeping Wikidata in sync with those sources manually doesn't scale. wd.py automates this: given a Wikidata item with an external identifier, it fetches the current data from the source, computes a minimal patch, and applies it — skipping claims that already exist and preserving anything added by human editors.

The framework handles the mechanics of Wikidata's API (authentication, CSRF tokens, edit summaries, claim merging, reference deduplication, rank management) so connector authors only need to write a parser for the external source.

# Built-in connectors

| Connector | External source               | Triggered by      |
| ----------| ------------------------------| ------------------|
| ArXiv     | arXiv.org                     | arXiv ID (P818)   |
| ADS       | NASA Astrophysics Data System | bibcode (P819)    |
| SIMBAD    | SIMBAD Astronomical Database  | SIMBAD ID (P3083) |
| Crossref  | Crossref                      | DOI (P356)        |
| ORCID     | ORCID                         | ORCID iD (P496)   |
| EuropePMC | Europe PubMed Central         | PubMed ID (P698)  |

Connectors are discovered automatically — drop a `.py`/`.json` pair into `src/wdpy/connectors/` and it's picked up.

# Usage

```python
from wdpy import Item, logon

logon('Bot username', 'password')

# Fetch item Q68126267, pull data from every recognized connector, and write
Item('Q68126267').sync().write()
```

`sync()` inspects all external-id claims on the item, calls the matching connector for each, and merges the results. Existing statements are updated only when the source data differs; deprecated identifiers are marked with a deprecation reason rather than deleted.

# Writing a connector

1. Create `src/wdpy/connectors/mysource.json` — declare the Wikidata source item, properties to manage, field-to-property mappings, and any value translation tables.
2. Create `src/wdpy/connectors/mysource.py` — subclass `SourceItem` and implement `parse(text, ident)` to populate claims from the API response.

```python
from wdpy.source_item import SourceItem

class MySourceItem(SourceItem):
    def parse(self, text: str, ident) -> None:
        data = json.loads(text)
        self.add_claim('P1476', data['title'])
        self.add_author(data['author'])
```

Optional hooks: override `make_request()` to customize headers or URL, and `update_ident()` to handle redirects.

# Setup

```bash
pip install -e .
```

The ADS connector requires an API token in `~/.ads`. All other built-in connectors use public APIs.

# Testing

```bash
python -m unittest discover tests/unit/        # unit tests (mocked)
python -m unittest discover tests/integration/ # integration tests (real API calls)
```
