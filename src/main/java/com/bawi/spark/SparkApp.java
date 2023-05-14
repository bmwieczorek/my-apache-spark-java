package com.bawi.spark;

import com.bawi.spark.common.Configuration;
import com.bawi.spark.common.ConfigurationProvider;
import com.bawi.spark.common.SparkBase;
//import com.codahale.metrics.Counter;
//import com.codahale.metrics.MetricRegistry;

public class SparkApp implements SparkBase, Read, Write, LoggingInfoListenerRegistrar, CustomSparkMetricsRegistrar, ConfigurationProvider {

//    static class MySource implements org.apache.spark.metrics.source.Source {
//
//        @Override
//        public String sourceName() {
//            return "MySource";
//        }
//
//        @Override
//        public MetricRegistry metricRegistry() {
//            return new MetricRegistry();
//        }
//
//        public static final MetricRegistry METRIC_REGISTRY = new MetricRegistry();
//        public static final Counter COUNTER = METRIC_REGISTRY.counter(MetricRegistry.name("fooCounter"));
//    }

    public static void main(String[] args) {
        new SparkApp().start();
    }

    @Override
    public Configuration getConfiguration() {
        return new Configuration("my.properties");
    }
}
