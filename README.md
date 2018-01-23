# Dataflow pipeline template

This repository is a template for python dataflow pipelines at Global Fishing
Watch. It's intended to be cloned and used as a template for new micropipelines
using apache beam in the google cloud dataflow platform.

It contains all the plumbing needed to validate and parse command line
arguments, as well as reading data from a source bigquery table and dumping the
results in another bigquery table.

## Usage

Clone the repository, delete the `.git` and run `git init` to have a fresh
repository ready to customize.

You'll need to take a look at a couple of files that you need to customize
before starting:

* rename the folder `pipe_template` to `pipe_myproject` (call it whatever your project name is)
  You will need to replace `pipe_template` with `pipe_project` gobally in the import statement 
   in all `.py` files.  (Pycharm refactor will do this for you automagically)

* Customize the name and description of your project. Look for `[TODO]` marks for
instructions on what to change and how in these files
    - `setup.py`
    - `pipe_template.__init__.py`
    - `docker-compose.yaml`
    - `airflow/pipe-template-dag.py`
    - `airflow/post_install.sh`

* Copy this file to a temporary name, delete it when you are done

* Copy `README.template.md` to `README.md` and customize with your project
  documentation. Look for `[TODO]` marks and complete as needed.

* Follow the setup steps in the new README.md and make sure everything works

* Begin wrting code that will do work
    - Set command line options in `pipe_myproject/options/options.py`
    - Create one or more transforms that will do the work modeled after in `pipe_myproject/trasform/addfield.py`
    - Construct a dataflow pipeline in `pipe_myproject/pipeline.py`
    - Modify unit tests in `tests`

# License

Copyright 2017 Global Fishing Watch

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
