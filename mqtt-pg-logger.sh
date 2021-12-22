#!/usr/bin/env bash

export PYTHONUNBUFFERED=1

# change into script dir to use relative pathes
SCRIPT_PATH=$(readlink -f $0)
SCRIPT_DIR=$(dirname "$SCRIPT_PATH")
SCRIPT_NAME=$(basename $0)
cd "$SCRIPT_DIR"

VENV_ACTIVATE="./venv/bin/activate"
if [ ! -f "$VENV_ACTIVATE" ] ; then
	echo -e "$SCRIPT_NAME\nerror: venv environment doesn't exist!"
	exit 1
fi

source "$VENV_ACTIVATE"
RC=$?
if [ $RC -ne 0 ] ; then
	echo -e "$SCRIPT_NAME\nerror: activating environment failed!"
	exit 1
fi


export PYTHONPATH="$SCRIPT_DIR"

python ./src/mqtt_pg_logger.py $@
exit $?
