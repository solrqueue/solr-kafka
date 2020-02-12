ROOT_DIR:=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))


###################################
#  BUILD VERSIONS                 #
#  override: make SOLR_VER=7.3.1  #
###################################
SOLR_VER ?= 7.5.0
KAFKA_VER ?= 2.2.1
KAFKA_SCALA_VER ?= 2.12


ZK_VER = 3.5.6

DEPS=$(ROOT_DIR)/.deps

ZK_DEP=$(DEPS)/apache-zookeeper-$(ZK_VER)-bin
SOLR_DEP=$(DEPS)/solr-$(SOLR_VER)
KAFKA_DEP=$(DEPS)/kafka_$(KAFKA_SCALA_VER)-$(KAFKA_VER)

DATA=$(ROOT_DIR)/data
SOLR_NODE1=$(DATA)/solr_node1/solr
SOLR_NODE2=$(DATA)/solr_node2/solr
KAFKA_BROKER1=$(DATA)/kafka_broker1
KAFKA_BROKER2=$(DATA)/kafka_broker2
ZK_NODE1=$(DATA)/zookeeper_node1

SOLR_NODE1_OPTS="-Dsolr.autoSoftCommit.maxTime=1000"
# To add debugging:
#SOLR_NODE1_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5003 -Dsolr.autoSoftCommit.maxTime=1000"
SOLR_NODE2_OPTS="-Dsolr.autoSoftCommit.maxTime=1000"
# To add debugging:
# SOLR_NODE2_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5004 -Dsolr.autoSoftCommit.maxTime=1000"


.PHONY: deps-all nodes-all start stop build base-start clean distclean solr-config solr-trigger solr-restart release-verify

all: $(SOLR_DEP) $(KAFKA_DEP) $(ZK_DEP) $(SOLR_NODE1)


stop: solr-stop
	# this sends a signal, but doesn't block until kafka is actually stopped
	# which can cause issues if running start/stop multiple times
	test ! -d $(KAFKA_DEP) || ($(KAFKA_DEP)/bin/kafka-server-stop.sh && sleep 4) || echo "Kafka stopped"
	test ! -d $(KAFKA_DEP) || ($(KAFKA_DEP)/bin/kafka-server-stop.sh && sleep 4) || echo "Kafka stopped"
	test ! -d $(KAFKA_DEP) || ($(KAFKA_DEP)/bin/kafka-server-stop.sh && sleep 4) || echo "Kafka stopped"
	test ! -d $(KAFKA_DEP) || ($(KAFKA_DEP)/bin/kafka-server-stop.sh && sleep 4) || echo "Kafka stopped"
	test ! -d $(KAFKA_DEP) || ($(KAFKA_DEP)/bin/kafka-server-stop.sh && sleep 4) || echo "Kafka stopped"
	test ! -d $(ZK_DEP) || $(ZK_DEP)/bin/zkServer.sh --config $(ZK_NODE1)/conf stop
	@echo "STOPPED"


# Used for live-verifying the release.  Makes a 2-node cluster, creates a 3-shard replicas=2
# collection adds some docs, and conforms the result
release-verify: distclean start solr-trigger
	curl -sq 'localhost:8983/solr/admin/collections?action=CREATE&collection.configName=solr_kafka&maxShardsPerNode=3&name=sk_test&numShards=3&replicationFactor=2'
	curl -sq -H 'Content-Type: application/json' 'localhost:8983/solr/sk_test/update' -d '{"add": {"doc": {"id": "1", "msg_s": "msg1"}}, "add": {"doc": {"id": "2", "msg_s": "msg2"}}, "add": {"doc": {"id": "3", "msg_s": "msg3"}}}'
	sleep 3
	curl -sq 'localhost:8983/solr/sk_test/query?q=*:*'
	test `curl -sq 'localhost:8983/solr/sk_test/query?q=*:*' | jq .response.numFound` -eq 3



start: base-start solr-restart

build:
	build/build.sh $(SOLR_VER) $(KAFKA_VER) $(SOLR_DEP)

solr-restart: build solr-config solr-stop solr-start

solr-stop:
	test ! -d $(SOLR_DEP) || $(SOLR_DEP)/bin/solr stop -all

solr-trigger: solr-start
	curl -sq "localhost:8983/solr/admin/autoscaling" -HContent-Type:application/json -d@trigger.json

solr-start: base-start
	echo | nc localhost 8983 || SOLR_LOGS_DIR=$(SOLR_NODE1)/logs $(SOLR_DEP)/bin/solr start -h 127.0.0.1 -cloud -p 8983 -s $(SOLR_NODE1) -z localhost:2181 -a $(SOLR_NODE1_OPTS)
	echo | nc localhost 8984 || SOLR_LOGS_DIR=$(SOLR_NODE2)/logs $(SOLR_DEP)/bin/solr start -h 127.0.0.1 -cloud -p 8984 -s $(SOLR_NODE2) -z localhost:2181 -a $(SOLR_NODE2_OPTS)

solr-config: base-start
	rm -rf "$(DATA)/configset"
	cp -r $(SOLR_DEP)/server/solr/configsets/_default $(DATA)/configset
	perl -pi -e 's@^(.*)(<processor class="solr.DistributedUpdateProcessorFactory"/>)@\1<processor class="com.github.solrqueue.solrkafka.KafkaUpdateProcessorFactory">\n\1\1<str name="bootstrap.servers">localhost:9093,localhost:9094</str>\n\1\1<str name="field">_offset_</str>\n\1</processor>\n\1\2@' $(DATA)/configset/conf/solrconfig.xml
	perl -pi -e 's@(<field name="_version_" .*/>)@\1\n<field name="_offset_" type="plong" indexed="false" stored="false"/>@' $(DATA)/configset/conf/managed-schema

	$(SOLR_DEP)/bin/solr zk upconfig -n solr_kafka -d $(DATA)/configset -z localhost:2181

base-start: nodes-all
	echo | nc localhost 2181 || $(ZK_DEP)/bin/zkServer.sh --config $(ZK_NODE1)/conf start
	echo | nc localhost 9093 || LOG_DIR=$(KAFKA_BROKER1)/logs $(KAFKA_DEP)/bin/kafka-server-start.sh -daemon $(KAFKA_BROKER1)/config/server.properties
	echo | nc localhost 9094 || LOG_DIR=$(KAFKA_BROKER2)/logs $(KAFKA_DEP)/bin/kafka-server-start.sh -daemon $(KAFKA_BROKER2)/config/server.properties


nodes-all: deps-all $(SOLR_NODE1) $(SOLR_NODE2) $(KAFKA_BROKER1) $(KAFKA_BROKER2) $(ZK_NODE1)

$(ZK_NODE1): $(ZK_DEP)
	mkdir -p $(ZK_NODE1)/data
	cp -r $(ZK_DEP)/conf $(ZK_NODE1)/
	perl -pi -e "s@dataDir=/tmp/zookeeper@dataDir=$(ZK_NODE1)/data@" $(ZK_NODE1)/conf/zoo.cfg


$(KAFKA_BROKER1): $(KAFKA_DEP)
	mkdir -p $(KAFKA_BROKER1)/config
	mkdir -p $(KAFKA_BROKER1)/data
	cp $(KAFKA_DEP)/config/server.properties $(KAFKA_BROKER1)/config
	perl -pi -e 's@broker.id=0@broker.id=1\nauto.create.topics.enable=false@' $(KAFKA_BROKER1)/config/server.properties
	perl -pi -e 's@#listeners=PLAINTEXT://:9092@listeners=PLAINTEXT://:9093@' $(KAFKA_BROKER1)/config/server.properties
	perl -pi -e "s@log.dirs=/tmp/kafka-logs@log.dirs=$(KAFKA_BROKER1)/data@" $(KAFKA_BROKER1)/config/server.properties
	perl -pi -e "s@zookeeper.connect=localhost:2181@zookeeper.connect=localhost:2181/kafka@" $(KAFKA_BROKER1)/config/server.properties

$(KAFKA_BROKER2): $(KAFKA_DEP)
	mkdir -p $(KAFKA_BROKER2)/config
	mkdir -p $(KAFKA_BROKER2)/data
	cp $(KAFKA_DEP)/config/server.properties $(KAFKA_BROKER2)/config
	perl -pi -e 's@broker.id=0@broker.id=2\nauto.create.topics.enable=false@' $(KAFKA_BROKER2)/config/server.properties
	perl -pi -e 's@#listeners=PLAINTEXT://:9092@listeners=PLAINTEXT://:9094@' $(KAFKA_BROKER2)/config/server.properties
	perl -pi -e "s@log.dirs=/tmp/kafka-logs@log.dirs=$(KAFKA_BROKER2)/data@" $(KAFKA_BROKER2)/config/server.properties
	perl -pi -e "s@zookeeper.connect=localhost:2181@zookeeper.connect=localhost:2181/kafka@" $(KAFKA_BROKER2)/config/server.properties

$(SOLR_NODE2): $(SOLR_DEP)
	mkdir -p $(SOLR_NODE2)
	cp $(SOLR_DEP)/server/solr/solr.xml $(SOLR_DEP)/server/solr/zoo.cfg $(SOLR_NODE2)

$(SOLR_NODE1): $(SOLR_DEP)
	mkdir -p $(SOLR_NODE1)
	cp $(SOLR_DEP)/server/solr/solr.xml $(SOLR_DEP)/server/solr/zoo.cfg $(SOLR_NODE1)

clean: stop
	rm -rf "$(DATA)"

distclean: stop clean
	rm -rf "$(DEPS)"

deps-all: $(ZK_DEP) $(KAFKA_DEP) $(SOLR_DEP)

$(ZK_DEP): $(DEPS)
	curl -Lsq "https://archive.apache.org/dist/zookeeper/zookeeper-$(ZK_VER)/apache-zookeeper-$(ZK_VER)-bin.tar.gz" | tar -C $(DEPS) -xzf -
	cp $(ZK_DEP)/conf/zoo_sample.cfg $(ZK_DEP)/conf/zoo.cfg

$(KAFKA_DEP): $(DEPS)
	curl -Lsq "https://archive.apache.org/dist/kafka/$(KAFKA_VER)/kafka_$(KAFKA_SCALA_VER)-$(KAFKA_VER).tgz" | tar -C $(DEPS) -xzf -

$(SOLR_DEP): $(DEPS)
	curl -Lsq "https://archive.apache.org/dist/lucene/solr/$(SOLR_VER)/solr-$(SOLR_VER).tgz" | tar -C $(DEPS) -xzf -

$(DEPS):
	mkdir -p $(ROOT_DIR)/.deps

