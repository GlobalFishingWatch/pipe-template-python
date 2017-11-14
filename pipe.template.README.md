# Dataflow pipeline template

This repository is a template for python dataflow pipelines at Global Fishing
Watch. It's intended to be cloned and used as a template for new micropipelines
using apache beam in the google cloud dataflow platform.

It contains all the plumbing needed to validate and parse command line
arguments, as well as reading data from a source bigquery table and dumping the
results in another sink bigquery table.

## Usage

Clone the repository, delete the `.git` and run `git init` to have a fresh
repository ready to customize.

You'll need to take a look at a couple of files that you need to customize
before starting:

* Customize the name and description of your project at `setup.py` and the
  docker image name at `docker-compose.yaml`. Look for `[TODO]` marks for
instructions on what to change and how.

* Customize the arguments that are needed to run your pipeline. These are
  spread at 3 different files:

    * `pipeline/options/all.py`: Contains options that are required for both
      local and remote runs.

    * `pipeline/options/local.py`: Contains options that are required only on
      local runs.

    * `pipeline/options/remote.py`: Contains options that are required only on
      remote runs.

* Customize the bigquery schemas for inputs and outputs at `pipeline/schemas/input.py`
  and `pipeline/schemas/output.py`.

* Create a sample query to run your pipeline in local test mode. Take a look at
  `examples/local.sql` and customize for what you need here. Take care to only
return a few rows here so that local test runs are quick and cheap.

* Create the transforms you are going to use at `pipeline/transforms/`. See
  `pipeline/transforms/sample.py` for a simple sample transform.

* Define your pipeline at `pipeline/definition.py` by composing the transforms
  you created.

* Copy `README.template.md` to `README.md` and customize with your project
  documentation. Look for `[TODO]` marks and complete as needed.

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
