#!/bin/sh

set -e

DIR=$(cd $(dirname $0); pwd)

pushd $DIR
echo -e "\nUNIT TESTS\n"
python setup.py test
popd

echo "INTEGRATION TESTS"
$DIR/tests/integration/test.sh
