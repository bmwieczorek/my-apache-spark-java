package com.bawi.spark.common;

import org.apache.spark.sql.SparkSession;

public interface SparkBase extends SparkMetricsRegistrar {

    void runSpark(SparkSession sparkSession);

    default void start() {
        SparkSession sparkSession = SparkSession.builder()
                .appName(getClass().getSimpleName())
                .getOrCreate();

        setupMetrics(sparkSession);

        runSpark(sparkSession);

        sparkSession.stop();
    }

}
