Minimalistic framework for continuous replication of external sources content to wikidata.
Few scripts demonstrate the use of the framework in practice:
* [exoplanet_eu.py](/src/exoplanet_eu.py) replicates exoplanets astrometry information (from http://exoplanet.eu/) to wikidata. List of exoplanets is obtained via internal API, actual data is scrapped from html (because no API provide real sources for the measurements)
* [yad_vashem.py](/src/yad_vashem.py) replacates basic info about [Righteous Among the Nations](https://en.wikipedia.org/wiki/Righteous_Among_the_Nations) (from https://https://righteous.yadvashem.org/) to wikidata. Data is obtained via internal API.

# Installation
You can run them in IDE of your choice, just specify your login and password as a command line argument. Required modules can be installed by ```pip install -r src/requirements.txt```
If you want to run them in [toolforge](https://wikitech.wikimedia.org/wiki/Portal:Toolforge), do the following:
1. Get content of this repository
2. Create a ```src/toolforge/.credentials``` file that contains your login and password separated by space. Script will use it to make edits in wikidata. **Highly** recommend creating a [BotPassword](https://www.wikidata.org/wiki/Special:BotPasswords) with a limited set of permissions rather than providing your real credentials. Don't forget ```chmod 440 .credentials``` to restrict who can read it.
3. Run ```toolforge-jobs run bootstrap-venv --command "cd $PWD && src/bootstrap_venv.sh" --image tf-python39 --wait``` in order to initialize python virtual environment and install nessecary packages.
4. Load jobs schedule via ```toolforge-jobs load src/toolforge/jobs.yaml```
