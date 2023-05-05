![Unit tests status](https://github.com/ghuron/wdpy/actions/workflows/test.yaml/badge.svg)

Minimalistic framework for continuous replication of external sources content to wikidata.
Few scripts demonstrate the use of the framework in practice:
* [simbad_dap.py](/src/simbad_dap.py) replicates stars/galaxies astrometry information from https://simbad.u-strasbg.fr/simbad/ to wikidata. Data is obtained by running ADQL queries via Table Access Protocol (TAP).
* [arxiv.py](/src/arxiv.py) replicates some pieces of preprints metadata from https://arxiv.org. Data is obtained both via regular public API and OAI-PMH. 
* [exoplanet_eu.py](/src/exoplanet_eu.py) replicates exoplanets astrometry information from http://exoplanet.eu/ to wikidata. List of exoplanets is obtained via internal API, actual data is scrapped from html (because no API provide real sources for the measurements). Utilizes scripts above to create/update parent star or preprint when necessary.
* [yad_vashem.py](/src/yad_vashem.py) replacates basic info about [Righteous Among the Nations](https://en.wikipedia.org/wiki/Righteous_Among_the_Nations) from https://righteous.yadvashem.org/ to wikidata. Data is obtained via internal API.

# Installation
You can run them in IDE of your choice, just specify your login and password as a command line argument. Required modules can be installed by ```pip install -r requirements.txt```

If you want to run them in [toolforge](https://wikitech.wikimedia.org/wiki/Portal:Toolforge), do the following:
1. Open ssh session to your tool on toolforge and run the following 3 commands:
   1. ```git init```
   2. ```git remote add origin https://github.com/ghuron/wdpy.git```
   3. ```git pull origin master```
2. Create [BotPassword](https://www.wikidata.org/wiki/Special:BotPasswords) with a limited set of permissions. Normally besides *basic* I request *editpage*, *createeditmovepage* and *highvolume* (see https://www.wikidata.org/wiki/Special:ListGrants for details)
3. Create one-liner file ```src/toolforge/.credentials``` and type there login and password separated by space. Script will use it to make edits in wikidata. Don't forget ```chmod 440 src/toolforge/.credentials``` to make sure other toolforge users will not see your credentials.
4. Run ```toolforge-jobs run bootstrap-venv --command "cd $PWD && src/bootstrap_venv.sh" --image tf-python39 --wait``` in order to initialize python virtual environment and install nessecary packages.
5. Load jobs schedule via ```toolforge-jobs load src/toolforge/jobs.yaml``` or run them individually (see https://wikitech.wikimedia.org/wiki/Help:Toolforge/Jobs_framework)
