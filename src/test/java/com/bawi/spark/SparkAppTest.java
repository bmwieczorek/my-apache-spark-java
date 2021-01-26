package com.bawi.spark;

import com.bawi.spark.common.Configuration;
import com.bawi.spark.common.ConfigurationProvider;
import com.bawi.spark.common.LoggingInfoListenerRegistrar;
import com.bawi.spark.common.SparkIngestionBase;
import org.apache.spark.sql.SparkSession;

public class SparkAppTest {
    static class SparkApp extends SparkIngestionBase implements LocalParallelCollectionRead, ConsoleOutputWrite,
            LoggingInfoListenerRegistrar, ConfigurationProvider {

        public SparkApp(SparkSession sparkSession) {
            super(sparkSession);
        }

        @Override
        public Configuration getConfiguration() {
            return new Configuration("app.properties");
        }
    }

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName(SparkAppTest.class.getSimpleName())
                .master("local[*]")
                .getOrCreate();
        new SparkApp(sparkSession).start();
        sparkSession.stop();
    }
}
