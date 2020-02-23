package com.github.solrqueue.solrbenchmark;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;

public class Main {
  private static Options options() {
    Options options = new Options();
    Option updateArg =
        Option.builder("updateArg")
            .argName("key=value")
            .hasArgs()
            .numberOfArgs(2)
            .valueSeparator('=')
            .desc("argument to pass to /update endpoint, e.g. kafka.skip=true")
            .build();
    Option nDocs = Option.builder("nDocs").hasArg().desc("number of documents to upsert").build();
    Option nWorkers =
        Option.builder("nIndexers").hasArg().desc("number of workers to do the indexing").build();
    Option docsGzXml =
        Option.builder("docs")
            .argName("documents.xml.gz")
            .desc(
                "gzipped xml to produce docs from, for example, from https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-abstract.xml.gz")
            .required()
            .hasArg()
            .build();

    Option zkHost =
        Option.builder("zkHost").argName("host:port").desc("zkHost to connect to").hasArg().build();

    Option collection =
        Option.builder("collection").argName("name").desc("collection name").hasArg().build();

    options.addOption(docsGzXml);
    options.addOption(zkHost);
    options.addOption(collection);
    options.addOption(updateArg);
    options.addOption(nDocs);
    options.addOption(nWorkers);
    return options;
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    AtomicBoolean indexingComplete = new AtomicBoolean();

    Options options = options();
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      System.err.println(e.getMessage());
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("solr-benchmark.jar", options);
      System.exit(1);
    }

    int indexers = Integer.parseInt(cmd.getOptionValue("nIndexers", "10"));
    int nDocs = Integer.parseInt(cmd.getOptionValue("nDocs", "-1"));

    Map<String, String> updateArgs = new HashMap<>();
    if (cmd.hasOption("updateArg")) {
      updateArgs.put(cmd.getOptionValues("updateArg")[0], cmd.getOptionValues("updateArg")[1]);
    }

    ExecutorService indexingExecutor = Executors.newFixedThreadPool(indexers);
    ExecutorService queryExecutor = Executors.newFixedThreadPool(5);
    String zkHost = cmd.getOptionValue("zkHost");
    List<String> zkHosts = Collections.singletonList(zkHost);
    CloudSolrClient client =
        new CloudSolrClient.Builder(zkHosts, Optional.empty())
            .withConnectionTimeout(10000)
            .withSocketTimeout(10000)
            .build();
    client.connect();

    String collection = cmd.getOptionValue("collection");

    DocumentIndexer indexer =
        new DocumentIndexer(indexingExecutor, client, 100, collection, updateArgs);

    QueryRunner qRunner = new QueryRunner(queryExecutor, client, collection);

    WikiAbstractDocGenerator generator =
        new WikiAbstractDocGenerator(cmd.getOptionValue("docs"), indexer, nDocs);

    // Stats.startConsoleReport();
    Stats.startOneLineReporter();
    long start = System.currentTimeMillis();
    Thread t =
        new Thread() {
          @Override
          public void run() {
            while (!indexingComplete.get()) {
              qRunner.queryAll();
              try {
                Thread.sleep(10);
              } catch (InterruptedException e) {
                break;
              }
            }
          }
        };
    try {
      t.start();
      generator.parse();
      indexingExecutor.shutdown();
      indexingExecutor.awaitTermination(100, TimeUnit.DAYS);
    } finally {
      indexingComplete.set(true);
    }
    long end = System.currentTimeMillis();
    t.join();
    queryExecutor.shutdown();
    queryExecutor.awaitTermination(100, TimeUnit.DAYS);
    client.close();
    System.out.println();
    Stats.Reporter.report();
    System.out.println("done in " + (end - start) + "ms");
  }
}
