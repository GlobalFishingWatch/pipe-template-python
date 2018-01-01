# Events pipeline [TODO: Replace with the name of your pipeline]

[TODO: Replace this short introduction]

This repository contains the fishing events pipeline, a dataflow pipeline which
extracts summarized fishing events from a collection of scored AIS messages.

# Running

## Dependencies

You just need [docker](https://www.docker.com/) and
[docker-compose](https://docs.docker.com/compose/) in your machine to run the
pipeline. No other dependency is required.

## Setup

The pipeline reads it's input from BigQuery, so you need to first authenticate
with your google cloud account inside the docker images. To do that, you need
to run this command and follow the instructions:

```
docker-compose run gcloud auth application-default login
```

## CLI

The pipeline includes a CLI that can be used to start both local test runs and
remote full runs. Just run `docker-compose run pipeline --help` and follow the
instructions there.

## Development
To run inside docker, use the following.  docker-compose mounts the current directory, so 
your changes are available immediately wihtout having to re-run docker-compose build.
If you change any pythin dependencies, then you will need to re-run build

```
docker-compose build
docker-compose run py.test tests
```


For a development environment outside of docker
```
virtualenv venv
source venv/bin/actiavte
pip install --process-dependency-links -e .
py.test tests
```

## Airflow

This pipeline is designed to be executed from Airflow using the GFW custom pipeline installation support

To install this pipeline from the locally built docker container, run the following from within the airflow server 
(might need sudo for this)

```
./utils/install_dag.sh gfw/pipe-template
```

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
