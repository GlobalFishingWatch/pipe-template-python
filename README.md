# NNet Pipeline

* `create_features` - create nnet features sharded by time.

* `shard_features` - shard nnet featues by mmsi

* `run_inference` - see classification pipeline for now.

* `annotate_results` - waiting on unique message id.

This pipeline generates features for the nnet.


# Running

## Dependencies

You just need [docker](https://www.docker.com/) and
[docker-compose](https://docs.docker.com/compose/) in your machine to run the
pipeline. No other dependency is required.

## Setup

The pipeline reads it's input from BigQuery, so you need to first authenticate
with your google cloud account inside the docker images. If you have already
authenticated another pipeline that uses this same structure and the docker
volume `gcp` exists, you don't need to do anything. Otherwise you need to run
the following two commands and follow the instructions:

```
docker volume create --name=gcp
docker-compose run gcloud auth application-default login
```

## CLI

The pipeline includes a CLI that can be used to start both local test runs and
remote full runs. Just run `docker-compose run pipeline --help` and follow the
instructions there.

### Examples

    docker-compose run create_features \
                          --start-date 2016-01-01 \
                          --end-date 2016-01-31 \
                          --source-table world-fishing-827:pipeline_classify_p_p516_daily \
                          --sink-table world-fishing-827:machine_learning_dev_ttl_30d.features_test \
                          --temp-gcs-location gs://world-fishing-827-dev-ttl30d/scratch/nnet-features \
                          remote \
                          --project world-fishing-827 \
                          --temp_location gs://machine-learning-dev-ttl-30d/scratch/nnet-features \
                          --job_name pipe-nnet-test \
                          --max_num_workers 200 \
                          --requirements_file ./requirements.txt 


    docker-compose run create_features \
                          --start_date=2016-01-01 \
                          --end_date=2016-12-31 \
                          --source_table=world-fishing-827:pipeline_classify_p_p516_daily \
                          --sink_table=world-fishing-827:machine_learning_dev_ttl_30d.features_test_2016 \
                          --project=world-fishing-827 \
                          --temp_location=gs://machine-learning-dev-ttl-30d/scratch/nnet-features \
                          --job_name=pipe-nnet-test \
                          --max_num_workers 200 \
                          --requirements_file=./requirements.txt \
                          --setup_file=./setup.py \
                          --runner=DataflowRunner


Sharding.

    docker-compose run shard_features \
                          --start-date 2016-01-01 \
                          --end-date 2016-12-31 \
                          --sink-table world-fishing-827:machine_learning_dev_ttl_30d.features_test_1 \
                          --temp-gcs-location gs://world-fishing-827-dev-ttl30d/scratch/nnet-features \
                          --shard-location gs://world-fishing-827-dev-ttl30d/features_2016/test-python-features \
                          remote \
                          --project world-fishing-827 \
                          --temp_location gs://machine-learning-dev-ttl-30d/scratch/nnet-features \
                          --job_name pipe-nnet-shard-test \
                          --max_num_workers 200 \
                          --requirements_file ./requirements.txt


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
