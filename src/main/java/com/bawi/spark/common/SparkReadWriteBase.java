package com.bawi.spark.common;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SparkReadWriteBase extends SparkBase implements DataFrameRead, DataFrameWrite {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkReadWriteBase.class);

    public SparkReadWriteBase() {
        super();
    }

    public SparkReadWriteBase(SparkSession sparkSession) {
        super(sparkSession);
    }

    @Override
    public void doInRun(SparkSession sparkSession) {
        CustomMapAccumulator customMapAccumulator = getCustomMapAccumulator();

        Dataset<Row> ds = read(sparkSession);

        // using scala RDD
/*        RDD<Row> rowRDD = ds.rdd().map(new AbstractFunction1<Row, Row>() {
            @Override
            public Row apply(Row row) {
                recordsCountAcc.add(1L);
                return row;
            }
        }, ClassManifestFactory.fromClass(Row.class));
*/

        // using java toJavaRDD
        JavaRDD<Row> rowRDD = ds.toJavaRDD().map(row -> {
            customMapAccumulator.add("recordsCount");
            return row;
        });

        Dataset<Row> dataset = sparkSession.createDataFrame(rowRDD, ds.schema());

        write(dataset);

        LOGGER.info("Spark accumulators: {}", customMapAccumulator.value());
    }

}
