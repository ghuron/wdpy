Minmalistic framework for periodic syncronization wikidata content with external sources together with scripts, that utilizes it

# Setup on Toolforge/K8S
1. Get content of this repository
2. Create ```.catbot``` file that contains your password. Leave read-only permissions for yourself and your group
3. Run ```toolforge-jobs run bootstrap-venv --command "cd $PWD && ./bootstrap_venv.sh" --image tf-python39 --wait``` in order to initialize python virtual environment and install nessecary packages.
4. Load jobs schedule via ```toolforge-jobs load jobs.yaml```
