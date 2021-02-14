package com.bawi.spark;

import com.bawi.spark.common.ConfigurationProvider;
import com.bawi.spark.common.DataFrameRead;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface HiveRead extends DataFrameRead, ConfigurationProvider {
    Logger LOGGER = LoggerFactory.getLogger(HiveRead.class);

    @Override
    default Dataset<Row> read(SparkSession sparkSession) {
        String sqlQuery = getConfiguration().getString("sql.query");
        LOGGER.info("executing sql.query: {}", sqlQuery);
        return sparkSession.sql(sqlQuery);
    }
}
