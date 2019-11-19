#!/bin/bash
docker pull catenae/kafka
docker pull catenae/link
docker pull catenae/link:test
docker pull catenae/link:develop

# docker pull registry.phd.brunneis.dev/fuc-benchmark
# docker rmi registry.phd.brunneis.dev/fuc-benchmark
docker build -t registry.phd.brunneis.dev/fuc-benchmark .
# docker push registry.phd.brunneis.dev/fuc-benchmark
