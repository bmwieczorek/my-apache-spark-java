package com.bawi.spark;

import com.bawi.spark.common.ConfigurationProvider;
import com.bawi.spark.common.DataFrameRead;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.reflect.ClassTag$;

import java.util.Arrays;
import java.util.List;

public interface LocalParallelCollectionRead extends DataFrameRead, ConfigurationProvider {

    default @Override
    Dataset<Row> read(SparkSession sparkSession) {
        List<String> strings = Arrays.asList("bob", "alice");

        // 1. Create RDD of String
        // a) scala way:
        //        val rdd: RDD[Int] = sparkContext.parallelize(Seq(1, 2, 3))

        // b) java way but using scala api (convert java list to scala seq + parallelize with required 3 parameters
        Seq<String> stringSeq = JavaConverters.asScalaIteratorConverter(strings.iterator()).asScala().toSeq();
        RDD<String> rddString = sparkSession.sparkContext().parallelize(stringSeq, 1, ClassTag$.MODULE$.apply(String.class));

        // c) pure java way using javaRDD wrapper to call parallelize
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
        JavaRDD<String> javaRDDString = javaSparkContext.parallelize(strings);
        // RDD<String> rdd = javaRDDString.rdd();

        // 2. Create Dataset/Dataframe of Row
        // a) scala way:
        //      val rdd: RDD[Int] = sparkContext.parallelize(Seq(1, 2, 3))
        //      import sparkSession.implicits._
        //      val df: DataFrame = rdd2.toDF("numbers")

        // b) call scala localSeqToDatasetHolder and toDF/toDS
/*
        implicits$ implicits = sparkSession.implicits();  // compiles in eclipse for scala

        Encoder<String> stringEncoder = implicits.newStringEncoder();
        Dataset<Row> rowDataset1 = implicits.localSeqToDatasetHolder(stringSeq, stringEncoder).toDF();
        rowDataset1.printSchema();
        rowDataset1.show();

        Seq<String> columnNames = JavaConverters.asScalaIteratorConverter(Collections.singletonList("string").iterator()).asScala().toSeq();
        Dataset<Row> rowDataset2 = implicits.localSeqToDatasetHolder(stringSeq, stringEncoder).toDF(columnNames);
        rowDataset2.printSchema();
        rowDataset2.show();

        Dataset<String> stringDataset = implicits.localSeqToDatasetHolder(stringSeq, stringEncoder).toDS();
        stringDataset.printSchema();
        stringDataset.show();
*/
        // c) use java
/*
        // Row is a generic container of data objects of different type
        JavaRDD<Row> javaRDDRow = javaRDDString.map(RowFactory::create);
        //  using struct
        StructField structField = DataTypes.createStructField("name", DataTypes.StringType, true);
        StructType structType = DataTypes.createStructType(Collections.singletonList(structField));
        // DataFrame is a Dataset<Row>
        Dataset<Row> dataset3 = sparkSession.createDataFrame(javaRDDRow, structType);
        dataset3.printSchema();
        dataset3.show();
*/

        // d) use spark api + encoders
        Dataset<String> dataset4 = sparkSession.createDataset(strings, Encoders.STRING());
        dataset4.printSchema();
        dataset4.show();

        Dataset<Row> dataset5 = dataset4.toDF("name");
        dataset5.printSchema();
        dataset5.show();

        return dataset5;
    }
}
