package com.bawi.spark.common;

import org.apache.spark.sql.SparkSession;

public interface SparkMetricsRegistrar {
    default void setupMetrics(SparkSession sparkSession) { }
}
