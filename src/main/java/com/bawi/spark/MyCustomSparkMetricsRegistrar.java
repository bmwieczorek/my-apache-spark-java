package com.bawi.spark;

import com.bawi.spark.common.CustomSparkMetricsListener;
import com.bawi.spark.common.SparkBase;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkEnv;
import org.apache.spark.groupon.metrics.UserMetricsSystem;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface MyCustomSparkMetricsRegistrar extends SparkBase {
    Logger LOGGER = LoggerFactory.getLogger(MyCustomSparkMetricsRegistrar.class);

    @Override
    default void setupMetrics(SparkSession sparkSession) {
        SparkContext sparkContext = sparkSession.sparkContext();
//        SparkApp.MySource mySource = new SparkApp.MySource();
//        SparkEnv.get().metricsSystem().registerSource(mySource);

        UserMetricsSystem.initialize(sparkContext, "custom_metrics");
        sparkContext.addSparkListener(new CustomSparkMetricsListener((key, value) -> {
            LOGGER.debug("Metric {} : {}", key, value);
            UserMetricsSystem.counter(key).inc(value);
        }));
    }

}
