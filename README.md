[Single file](/src/wd.py) lean framework for continuous replication of external data sources with wikidata.
Unlike wikipedia, almost all one-off tasks, that involves mass Wikidata edits, can be performed by the combination
of [WDQS](https://www.wikidata.org/wiki/Wikidata:SPARQL_query_service), spreadsheet
and [QS](https://www.wikidata.org/wiki/Help:QuickStatements).
You only need this framework if you are planning to execute your bot periodically.
It is intended for *experienced* Wikidata users that are able to:

* write data extractor in python 3.9+
* write SPARQL queries (with specific WDQS extensions)
* understand [Wikibase Data Model](https://www.mediawiki.org/wiki/Wikibase/DataModel)

There are source code code of real life bots based on that framework (see [below](#Implementations))

# Installation

In order to run script in any IDE, one has to specify login/password as first/second command line arguments.
Required dependencies can be installed by ```pip install -r requirements.txt```

If you want to run scripts in [toolforge](https://wikitech.wikimedia.org/wiki/Portal:Toolforge), do the following:
1. Open ssh session to your tool on toolforge and run the following 3 commands:
   1. ```git init```
   2. ```git remote add origin https://github.com/ghuron/wdpy.git```
   3. ```git pull origin master```
2. Create [BotPassword](https://www.wikidata.org/wiki/Special:BotPasswords) with a limited set of permissions. Normally besides *basic* I request *editpage*, *createeditmovepage* and *highvolume* (see https://www.wikidata.org/wiki/Special:ListGrants for details)
3. Create one-liner file ```src/toolforge/.credentials``` and type there login and password separated by space. Script will use it to make edits in wikidata. Don't forget ```chmod 440 src/toolforge/.credentials``` to make sure other toolforge users will not see your credentials.
4. Run ```toolforge-jobs run bootstrap-venv --command "cd $PWD && src/bootstrap_venv.sh" --image tf-python39 --wait```
   in order to initialize python virtual environment and install necessary packages.
5. Load jobs schedule via ```toolforge-jobs load src/toolforge/jobs.yaml``` or run them individually (see https://wikitech.wikimedia.org/wiki/Help:Toolforge/Jobs_framework)

# Overview

The framework consists of 4 basic classes:

* ```Wikidata``` class is a thin wrapper around [Wikidata API v1](https://www.wikidata.org/w/api.php)
  and [Wikidata Query Service](https://query.wikidata.org/)
* ```Model``` class is a set of methods that helps to work
  with [values](https://www.mediawiki.org/wiki/Wikibase/DataModel#Values)
  and [snaks](https://www.mediawiki.org/wiki/Wikibase/DataModel#Snaks)
* ```Claim``` class represents [statement](https://www.mediawiki.org/wiki/Wikibase/DataModel#Statements) with focus
  on [list of references](https://www.mediawiki.org/wiki/Wikibase/DataModel#ReferenceRecords)
* ```Element``` represents [items](https://www.mediawiki.org/wiki/Wikibase/DataModel#Items), that can be created from (
  or updated with) a list of snaks

More detailed documentation is included into source code. There are also some tests, that might help with understanding
of expected behaviour.

# Implementations

You can learn how to use the framework by looking into real-life implementations of my bots (from simplest to complex):

1. [simbad_id.py](/src/simbad_id.py) is intended to keep [P3083](https://www.wikidata.org/wiki/Property:P3083) values in
   wikidata up-to-date.
   [SIMBAD](https://simbad.u-strasbg.fr/simbad/) aggressively changes primary identifiers, usually keeping "redirects"
   for some time (but not for long).
   It is important to identify P3083-statements with redirects and "resolve" them. In order to do so, we are
    1. obtaining 10000 statements via WDQS
    2. checking which on them are redirects (running simple ADQL queries using TAP)
    3. updating affected via ```Claim``` class
2. [arxiv.py](/src/arxiv.py) combines two tasks in one file:
    * When run as a standalone bot, it helps to fill missing [P356](https://www.wikidata.org/wiki/Property:P356) values
      when [P818](https://www.wikidata.org/wiki/Property:P818) is known and vise versa.
      It uses slow bulk metadata [OAI-PMH](https://info.arxiv.org/help/oa/index.html), 2 SPARQL queries and ```Claim```
      class
    * Implement bare minimum on top of ```Model``` and ```Element``` classes to make sure
      that ```arxiv.Element.get_by_id()``` is able to create a missing item.
      Information is extracted via [regular ArXiv API](https://info.arxiv.org/help/api/basics.html)
