package com.github.solrqueue.solrbenchmark;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class Stats {
  static final MetricRegistry Metrics = new MetricRegistry();
  static final Counter Produced = Metrics.counter("produced");
  static final Counter Indexed = Stats.Metrics.counter("indexed");
  static final AtomicLong AvailableCount = new AtomicLong();
  static final Gauge<Long> Available =
      Stats.Metrics.register(
          "available",
          new Gauge<Long>() {
            @Override
            public Long getValue() {
              return AvailableCount.get();
            }
          });
  static final Timer IndexRequestTimer = Stats.Metrics.timer("updateRequests");
  static final Timer QueryRequestTimer = Stats.Metrics.timer("queryRequests");

  static ConsoleReporter Reporter =
      ConsoleReporter.forRegistry(Metrics)
          .convertRatesTo(TimeUnit.SECONDS)
          .convertDurationsTo(TimeUnit.MILLISECONDS)
          .build();

  static String getOneLine() {
    return String.format(
        "produced: % 10d indexed:% 10d available:% 10d ",
        Produced.getCount(), Indexed.getCount(), Available.getValue());
  }

  static void startOneLineReporter() {
    System.err.println();
    Thread t =
        new Thread() {
          @Override
          public void run() {
            while (true) {
              System.err.print("\r" + getOneLine());
              try {
                Thread.sleep(100);
              } catch (InterruptedException e) {
                break;
              }
            }
          }
        };
    t.setDaemon(true);
    t.start();
  }
}
