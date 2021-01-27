package com.bawi.spark.common;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SparkIngestionBase implements DataFrameRead, DataFrameWrite, SparkContextListenerRegistrar {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkIngestionBase.class);

    private SparkSession sparkSession;

    public SparkIngestionBase(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public void start() {
        long startTimeMillis = System.currentTimeMillis();
        LongAccumulator recordsCountAcc = sparkSession.sparkContext().longAccumulator("recordsCountAcc");
        try {
            getOnStartListeners().forEach(l -> l.accept(sparkSession));

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
                recordsCountAcc.add(1L);
                return row;
            });

            Dataset<Row> dataset = sparkSession.createDataFrame(rowRDD, ds.schema());

            write(dataset);
            getOnSuccessListeners().forEach(l -> l.accept(sparkSession));

            LOGGER.info("Spark processing {} records succeeded after {} ms",
                    recordsCountAcc.value(), System.currentTimeMillis() - startTimeMillis);
        } catch (Exception e) {
            getOnErrorListeners().forEach(l -> l.accept(sparkSession));
            LOGGER.error("Spark processing " + recordsCountAcc.value() + " records failed after "
                    + (System.currentTimeMillis() - startTimeMillis) + " ms due to ", e);
            throw e;
        }
    }

}
