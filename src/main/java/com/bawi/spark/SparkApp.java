package com.bawi.spark;


import com.bawi.spark.common.*;

public class SparkApp extends SparkReadWriteBase implements LocalParallelCollectionRead, ConsoleOutputWrite,
        LoggingInfoListenerRegistrar, CustomSparkMetricsRegistrar, ConfigurationProvider {

    @Override
    public Configuration getConfiguration() {
        return new Configuration("app.properties");
    }

    public static void main(String[] args) {
        new SparkApp().run();
    }
}
