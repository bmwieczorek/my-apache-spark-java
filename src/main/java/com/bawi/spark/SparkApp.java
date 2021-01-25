package com.bawi.spark;

import com.bawi.spark.common.Configuration;
import com.bawi.spark.common.ConfigurationProvider;
import com.bawi.spark.common.SparkBase;

public class SparkApp implements SparkBase, Read, Write, LoggingInfoListenerRegistrar, CustomSparkMetricsRegistrar, ConfigurationProvider {

    public static void main(String[] args) {
        new SparkApp().start();
    }

    @Override
    public Configuration getConfiguration() {
        return new Configuration("my.properties");
    }
}
