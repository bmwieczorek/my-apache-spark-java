package com.bawi.spark.common;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public interface DataFrameRead {
    Dataset<Row> read(SparkSession sparkSession);
}
