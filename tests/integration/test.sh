#!/bin/sh

set -e

DIR=$(cd $(dirname $0)/../../ ; pwd)
VIRTUALENV=$DIR/build/virtualenv

if [ -z "$PERSIST_VIRTUALENV" ]
then
    if [ -d $VIRTUALENV ]
    then
        rm -rf $VIRTUALENV
    fi

    virtualenv $VIRTUALENV
fi


(cd $DIR; $VIRTUALENV/bin/python setup.py install)

(cd $DIR; $VIRTUALENV/bin/python $DIR/tests/integration/test_all.py)