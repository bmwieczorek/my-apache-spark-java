package com.bawi.spark;

import com.bawi.spark.common.ConfigurationProvider;
import com.bawi.spark.common.SparkIngestionBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface Write extends SparkIngestionBase, ConfigurationProvider {
    Logger LOGGER = LoggerFactory.getLogger(Write.class);

    @Override
    default void write(Dataset<Row> ds) {
        String writePath = getConfiguration().getString("write.path");
        LOGGER.info("Writing dataset to {}", writePath);
        ds.show();
    }
}
