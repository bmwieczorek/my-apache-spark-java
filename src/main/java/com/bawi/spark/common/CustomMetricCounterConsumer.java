package com.bawi.spark.common;

@FunctionalInterface
public interface CustomMetricCounterConsumer {
    void onMetric(String key, long value);
}