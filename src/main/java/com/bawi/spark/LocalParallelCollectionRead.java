package com.bawi.spark;

import com.bawi.spark.common.ConfigurationProvider;
import com.bawi.spark.common.DataFrameRead;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public interface LocalParallelCollectionRead extends DataFrameRead, ConfigurationProvider {

    default @Override
    Dataset<Row> read(SparkSession sparkSession) {
        List<String> strings = Arrays.asList("bob", "alice");
        Dataset<String> ds = sparkSession.createDataset(strings, Encoders.STRING());
        return ds.toDF("name");
    }
}
