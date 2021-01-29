package com.bawi.spark;

import com.bawi.spark.common.ConfigurationProvider;
import com.bawi.spark.common.DataFrameRead;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.Collections;

public interface LocalParallelCollectionRead extends DataFrameRead, ConfigurationProvider {

    default @Override
    Dataset<Row> read(SparkSession sparkSession) {
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
        JavaRDD<String> javaRDDString = javaSparkContext.parallelize(Arrays.asList("bob", "alice"));
//        Dataset<Row> ds = sparkSession.read().json(javaRDD);
// OR
        JavaRDD<Row> javaRDDRow = javaRDDString.map(RowFactory::create);
        StructField structField = DataTypes.createStructField("name", DataTypes.StringType, true);
        StructType structType = DataTypes.createStructType(Collections.singletonList(structField));

        return sparkSession.createDataFrame(javaRDDRow, structType);
    }
}
