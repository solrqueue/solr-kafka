#!/bin/bash
set -eo pipefail

SOLR_VER="$1"
KAFKA_VER="$2"
SOLR_DEST="$3"
LIB_DIR="${SOLR_DEST}/server/solr-webapp/webapp/WEB-INF/lib"

mvn -Dsolr.version="${SOLR_VER}" -Dkafka.version="${KAFKA_VER}" clean package

for jar in `find target -name "*jar"`; do
  cp "$jar" "${LIB_DIR}"
  echo cp "$jar" "${LIB_DIR}"
done

DEP_JARS=`mvn -q exec:exec -Dexec.executable=echo -Dexec.args="%classpath" | tr ':' ' '`

DEPS="
kafka-clients-${KAFKA_VER}.jar
lz4-java-1.5.0.jar
snappy-java-1.1.7.2.jar
"

for dep in $DEPS; do
  found_dep='no'
  for jar in $DEP_JARS; do
    jar_base=`basename $jar`
    if [ "$dep" == "$jar_base" ]; then
      cp "$jar" "${LIB_DIR}"
      echo cp "$jar" "${LIB_DIR}"
      found_dep='yes'
    fi
  done
  if [ "$found_dep" == "no" ]; then
    echo could not find $dep in jars. maybe a newer version exists?
    exit 1
  fi
done
