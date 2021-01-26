package com.bawi.spark.common;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface DataFrameWrite {
    void write(Dataset<Row> ds);
}
