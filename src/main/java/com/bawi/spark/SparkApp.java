package com.bawi.spark;


import com.bawi.spark.common.Configuration;
import com.bawi.spark.common.ConfigurationProvider;
import com.bawi.spark.common.LoggingInfoListenerRegistrar;
import com.bawi.spark.common.SparkBase;

public class SparkApp extends SparkBase implements LocalParallelCollectionRead, ConsoleOutputWrite,
        LoggingInfoListenerRegistrar, CustomSparkMetricsRegistrar, ConfigurationProvider {

    @Override
    public Configuration getConfiguration() {
        return new Configuration("app.properties");
    }

    public static void main(String[] args) {
        new SparkApp().start();
    }
}
