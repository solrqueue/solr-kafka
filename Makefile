ROOT_DIR:=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

SOLR_VER=7.5.0
ZK_VER=3.5.3-beta
KAFKA_VER=2.0.0
KAFKA_SCALA_VER=2.11

DEPS=$(ROOT_DIR)/.deps

ZK_DEP=$(DEPS)/zookeeper-$(ZK_VER)
SOLR_DEP=$(DEPS)/solr-$(SOLR_VER)
KAFKA_DEP=$(DEPS)/kafka_$(KAFKA_SCALA_VER)-$(KAFKA_VER)

DATA=$(ROOT_DIR)/data
SOLR_NODE1=$(DATA)/solr_node1/solr
SOLR_NODE2=$(DATA)/solr_node2/solr
KAFKA_BROKER1=$(DATA)/kafka_broker1
KAFKA_BROKER2=$(DATA)/kafka_broker2
ZK_NODE1=$(DATA)/zookeeper_node1



.PHONY: deps-all nodes-all start stop build base-start clean distclean solr-config solr-trigger solr-restart

all: $(SOLR_DEP) $(KAFKA_DEP) $(ZK_DEP) $(SOLR_NODE1)


stop: solr-stop
	$(KAFKA_DEP)/bin/kafka-server-stop.sh || echo "STOPPED"
	$(ZK_DEP)/bin/zkServer.sh --config $(ZK_NODE1)/conf stop
	@echo "STOPPED"

start: base-start solr-restart

build:
	./build.sh $(KAFKA_VER) $(SOLR_DEP)

solr-restart: build solr-config solr-stop solr-start

solr-stop:
	$(SOLR_DEP)/bin/solr stop -all

solr-trigger: solr-start
	curl -sq "localhost:8983/solr/admin/autoscaling" -HContent-Type:application/json -d@trigger.json

solr-start: base-start
	echo | nc localhost 8983 || SOLR_LOGS_DIR=$(SOLR_NODE1)/logs $(SOLR_DEP)/bin/solr start -cloud -p 8983 -s $(SOLR_NODE1) -z localhost:2181 -a "-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5003"
	echo | nc localhost 8984 || SOLR_LOGS_DIR=$(SOLR_NODE2)/logs $(SOLR_DEP)/bin/solr start -cloud -p 8984 -s $(SOLR_NODE2) -z localhost:2181 -a "-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5004"


solr-config: base-start
	rm -rf "$(DATA)/configset"
	cp -r $(SOLR_DEP)/server/solr/configsets/_default $(DATA)/configset
	perl -pi -e 's@^(.*)(<processor class="solr.DistributedUpdateProcessorFactory"/>)@\1<processor class="com.solrqueue.solrkafka.KafkaUpdateProcessorFactory">\n\1\1<str name="bootstrap.servers">localhost:9093,localhost:9094</str>\n\1\1<str name="field">_offset_</str>\n\1</processor>\n\1\2@' $(DATA)/configset/conf/solrconfig.xml
	perl -pi -e 's@(<field name="_version_" .*/>)@\1\n<field name="_offset_" type="plong" indexed="false" stored="false"/>@' $(DATA)/configset/conf/managed-schema

	$(SOLR_DEP)/bin/solr zk upconfig -n solr_kafka -d $(DATA)/configset -z localhost:2181

base-start: nodes-all
	echo | nc localhost 2181 || $(ZK_DEP)/bin/zkServer.sh --config $(ZK_NODE1)/conf start
	echo | nc localhost 9093 || $(KAFKA_DEP)/bin/kafka-server-start.sh -daemon $(KAFKA_BROKER1)/config/server.properties
	echo | nc localhost 9094 || $(KAFKA_DEP)/bin/kafka-server-start.sh -daemon $(KAFKA_BROKER2)/config/server.properties


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
	curl -L "http://www.apache.org/dist/zookeeper/zookeeper-$(ZK_VER)/zookeeper-$(ZK_VER).tar.gz" | tar -C $(DEPS) -xzf -
	cp $(ZK_DEP)/conf/zoo_sample.cfg $(ZK_DEP)/conf/zoo.cfg

$(KAFKA_DEP): $(DEPS)
	curl -L "http://www.apache.org/dist/kafka/$(KAFKA_VER)/kafka_$(KAFKA_SCALA_VER)-$(KAFKA_VER).tgz" | tar -C $(DEPS) -xzf -

$(SOLR_DEP): $(DEPS)
	curl -L "http://www.apache.org/dist/lucene/solr/$(SOLR_VER)/solr-$(SOLR_VER).tgz" | tar -C $(DEPS) -xzf -

$(DEPS):
	mkdir -p $(ROOT_DIR)/.deps

