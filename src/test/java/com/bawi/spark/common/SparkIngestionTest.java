package com.bawi.spark.common;

import com.bawi.spark.ConsoleOutputWrite;
import com.bawi.spark.LocalParallelCollectionRead;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

public class SparkIngestionTest {

    private static class SparkApp extends SparkIngestionBase implements LocalParallelCollectionRead, ConsoleOutputWrite,
            LoggingInfoListenerRegistrar, ConfigurationProvider {

        public SparkApp(SparkSession sparkSession) {
            super(sparkSession);
        }

        @Override
        public Configuration getConfiguration() {
            return new Configuration("app.properties");
        }
    }

    @Test
    public void start() {
        SparkSession sparkSession = SparkSession.builder()
                .appName(SparkIngestionTest.class.getSimpleName())
                .master("local[*]")
                .getOrCreate();
        new SparkApp(sparkSession).start();
        sparkSession.stop();
    }
}