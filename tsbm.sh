#!/bin/sh
mvn clean package -Dmaven.test.skip=true
if [ -z "${BENCHMARK_HOME}" ]; then
  export BENCHMARK_HOME="$(cd "`dirname "$0"`"/.; pwd)"
fi
cd bin
echo $(readlink -f ../conf/config.properties)
sh startup.sh -cf $BENCHMARK_HOME/conf/config.properties  -bd $BENCHMARK_HOME/conf/bindings.properties