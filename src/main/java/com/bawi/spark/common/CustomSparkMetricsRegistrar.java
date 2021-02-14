package com.bawi.spark.common;

import org.apache.spark.SparkContext;
import org.apache.spark.groupon.metrics.UserMetricsSystem;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public interface CustomSparkMetricsRegistrar extends SparkMetricsRegistrar {
    Logger LOGGER = LoggerFactory.getLogger(CustomSparkMetricsRegistrar.class);

    @Override
    default void setupMetrics(SparkSession sparkSession, Map<String, CustomMapAccumulator> customMapAccumulatorMap) {
        LOGGER.info("Setting up custom spark metrics");
        SparkContext sparkContext = sparkSession.sparkContext();
        UserMetricsSystem.initialize(sparkContext, "custom_metrics");
        sparkContext.addSparkListener(new CustomSparkMetricsListener((key, value) -> {
            LOGGER.info("Metric {} : {}", key, value);
            UserMetricsSystem.counter(key).inc(value);
        }, customMapAccumulatorMap));
    }

}
