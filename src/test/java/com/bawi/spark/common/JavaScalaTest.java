package com.bawi.spark.common;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.reflect.ClassManifestFactory;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;
import java.util.*;

public class JavaScalaTest {
public static class Person implements Serializable {
    private String name;
    private int age;
    public Person() { } // default construction and setters/getters are required by spark
    public Person(String name, int age) { this.name = name;this.age = age; }
    public void setName(String name) { this.name = name; }
    public void setAge(int age) { this.age = age; }
    public String getName() { return name; }
    public int getAge() { return age; }
    @Override public String toString() { return "Person{name='" + name + "',age='" + age + "'}"; }
}

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().master("local[*]").getOrCreate();

        Iterator<String> iterator = Arrays.asList("a", "b").iterator();
        Seq<String> seq = JavaConverters.asScalaIteratorConverter(iterator).asScala().toSeq();
        RDD<String> rdd = sparkSession.sparkContext().parallelize(seq, 2, ClassTag$.MODULE$.apply(String.class));

        Dataset<String> dataset = sparkSession.createDataset(Arrays.asList("a", "b"), Encoders.STRING());

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


        // compilation error - scala.Function1 is scala specialized and requires implementing additional abstract method apply$mcVJ$sp(long)1
/*
        dataset.map(new Function1<String, Integer>() {
            @Override
            public Integer apply(String string) {
                return string.length();
            }
        }, Encoders.INT());
*/

        // compilation error: incompatible types: scala.Function1 is not a functional interface multiple non-overriding abstract methods found in interface scala.Function1
        // dataset.map((Function1<String, Integer>) String::length, Encoders.INT());


        // org.apache.spark.api.java.function.MapFunction (from java package)
        dataset.map(new MapFunction<String, Integer>() {
            @Override
            public Integer call(String value) throws Exception {
                return value.length();
            }
        }, Encoders.INT());


        // lambda usage as MapFunction is functional interface
        dataset.map((MapFunction<String, Integer>) String::length, Encoders.INT()); // need to cast to MapFunction


        JavaRDD<String> stringJavaRDD = new JavaSparkContext(sparkSession.sparkContext()).parallelize(Arrays.asList("a", "bb", "ccc"));
        stringJavaRDD.foreach(s -> System.out.println(s));

        // java.io.NotSerializableException: java.io.PrintStream
        // stringJavaRDD.foreach(System.out::println);



        JavaRDD<Row> rowJavaRDD = stringJavaRDD.map(new Function<String, Row>() {  // java functional interface org.apache.spark.api.java.function.Function
            @Override
            public Row call(String string) throws Exception {
                return RowFactory.create(string);
            }
        });

        // java functional interface org.apache.spark.api.java.function.Function
        JavaRDD<Row> rowJavaRDD2 = stringJavaRDD.map(RowFactory::create);

        RDD<Row> rowRDD = rowJavaRDD.rdd();
        // requires import of scala.reflect.ClassManifestFactory and scala.runtime.AbstractFunction1
        RDD<Row> rowRDD1 = rowRDD.map(new AbstractFunction1<Row, Row>() {
            @Override
            public Row apply(Row row) {
                return row;
            }
        }, ClassManifestFactory.fromClass(Row.class));


        // using scala RDD
/*        RDD<Row> rowRDD = ds.rdd().map(new AbstractFunction1<Row, Row>() {
            @Override
            public Row apply(Row row) {
                recordsCountAcc.add(1L);
                return row;
            }
        }, ClassManifestFactory.fromClass(Row.class));
*/

        StructField structField = DataTypes.createStructField("my_name", DataTypes.StringType, true);
        StructType structType = DataTypes.createStructType((Collections.singletonList(structField)));
        Dataset<Row> dataFrame = sparkSession.createDataFrame(rowJavaRDD, structType);

        JavaRDD<Person> personJavaRDD = stringJavaRDD.map(name -> new Person(name, new Random().nextInt(100)));



        personJavaRDD.foreach(p -> System.out.println(p));

        Dataset<String> stringDs = sparkSession.createDataset(stringJavaRDD.rdd(), Encoders.STRING());
        Dataset<Person> personDataset = sparkSession.createDataset(Arrays.asList(new Person("Bob", 21), new Person("Alice", 20)), Encoders.bean(Person.class));
        personDataset.printSchema();
        personDataset.show();
        Dataset<Row> personDataframe = sparkSession.createDataFrame(personJavaRDD, Person.class);
        personDataframe.printSchema();
        personDataframe.show();



        Dataset<Person> ds_2 = personDataframe.as(Encoders.bean(Person.class));
        ds_2.show();
        Dataset<Row> df_2 = personDataset.toDF();
        df_2.show();

    }
}

