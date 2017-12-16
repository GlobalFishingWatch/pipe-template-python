#!/usr/bin/env bash

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"

display_usage() {
	echo "Available Commands"
	echo "  dataflow_test   Run a sample dataflow job"
	}


if [[ $# -le 0 ]]
then
    display_usage
    exit 1
fi


case $1 in

  dataflow_test)

    python -m pipe_template "${@:2}"
    ;;

  *)
    display_usage
    exit 0
    ;;
esac
