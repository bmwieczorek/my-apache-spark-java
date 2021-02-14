package com.bawi.spark.common;

import org.apache.spark.sql.SparkSession;

import java.util.Map;

public interface SparkMetricsRegistrar {
    default void setupMetrics(SparkSession sparkSession, Map<String, CustomMapAccumulator> customMapAccumulatorMap) { }
}
