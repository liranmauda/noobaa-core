#!/bin/bash

count=$1
make tester TESTER_TAG?="liranmauda/noobaa-nightly:test${count}" NOOBAA_TAG="liranmauda/noobaa-nightly:up${count}"
docker push liranmauda/noobaa-nightly:up${count}
docker push liranmauda/noobaa-nightly:test${count}

cd src/test/framework/ 
./run_test_job.sh --name test-${count} --image liranmauda/noobaa-nightly:up${count} --tester_image liranmauda/noobaa-nightly:test${count} --concurrency 10 --delete_on_fail
