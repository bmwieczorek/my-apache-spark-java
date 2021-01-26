package com.bawi.spark;

import com.bawi.spark.common.*;
import org.apache.spark.sql.SparkSession;

public class SparkApp implements SparkBase, CustomSparkMetricsRegistrar {

    public static void main(String[] args) {
        new SparkApp().start();
    }

    @Override
    public void runSpark(SparkSession sparkSession) {
        class SparkIngestion extends SparkIngestionBase implements LocalParallelCollectionRead, ConsoleOutputWrite,
                LoggingInfoListenerRegistrar, ConfigurationProvider {

            public SparkIngestion(SparkSession sparkSession) {
                super(sparkSession);
            }

            @Override
            public Configuration getConfiguration() {
                return new Configuration("app.properties");
            }
        }
        new SparkIngestion(sparkSession).start();
    }
}
