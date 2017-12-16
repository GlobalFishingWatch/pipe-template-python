#!/bin/bash

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"

# import settings
source ${THIS_SCRIPT_DIR}/test-config.sh

JOB_NAME=test_pipe_tmplate_2017_12_12

display_usage() {
	echo "Available Commands"
	echo "  local       run the segmenter locally"
	echo "  remote      run the segmenter in dataflow"
	}


if [[ $# -le 0 ]]
then
    display_usage
    exit 1
fi


case $1 in

  local)
    docker-compose run pipe_template \
      --tag_field tag \
      --tag_value test  \
      --dest ./output/test_pipe_template_
    ;;

  remote)
    docker-compose run pipe_template \
      --tag_field tag \
      --tag_value test  \
      --dest gs://${PIPELINE_BUCKET}/test_pipe_template_ \
      --runner=DataflowRunner \
      --project world-fishing-827 \
      --temp_location gs://${TEMP_BUCKET_NAME}/dataflow-temp/ \
      --staging_location=gs://${TEMP_BUCKET_NAME}/dataflow-staging/ \
      --job_name ${JOB_NAME//_/-} \
      --max_num_workers 4 \
      --disk_size_gb 50 \
      --requirements_file=./requirements.txt \
      --setup_file=./setup.py
    ;;

  *)
    display_usage
    exit 0
    ;;
esac
