package com.bawi.spark.common;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public interface SparkBase {

    default void setupMetrics(SparkSession sparkSession) {}
    void runSpark(SparkSession sparkSession);

    default void start() {
        SparkConf sparkConf = new SparkConf().setAppName(getClass().getSimpleName());
        if (isLocal()) {
            sparkConf.setMaster("local[*]");
        }

        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

        setupMetrics(sparkSession);

        runSpark(sparkSession);

        sparkSession.stop();
    }

    static boolean isLocal() {
        String osName = System.getProperty("os.name").toLowerCase();
        return osName.contains("mac") || osName.contains("windows");
    }

}
