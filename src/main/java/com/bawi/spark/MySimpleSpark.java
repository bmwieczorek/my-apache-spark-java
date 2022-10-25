package com.bawi.spark;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class MySimpleSpark {
    private static final Logger LOGGER = LoggerFactory.getLogger(MySimpleSpark.class);

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local[*]").getOrCreate();
        Dataset<Integer> ds = spark
        .createDataset(Arrays.asList("1", "2", "3"), Encoders.STRING())
        .map((MapFunction<String, Integer>) s -> {
            LOGGER.warn("processing: {}", s);
            return Integer.parseInt(s);
        }, Encoders.INT());
        ds.cache(); // cache or recalculate input
        ds.filter((FilterFunction<Integer>) i -> i % 2 == 0).write().json("even");
        ds.filter((FilterFunction<Integer>) i -> i % 2 == 1).write().json("odd");
    }
}

//2022-10-03 14:33:05 [Executor task launch worker for task 1id] WARN  com.bawi.spark.MySimpleSpark:21 - processing: 2
//2022-10-03 14:33:05 [Executor task launch worker for task 2id] WARN  com.bawi.spark.MySimpleSpark:21 - processing: 3
//2022-10-03 14:33:05 [Executor task launch worker for task 0id] WARN  com.bawi.spark.MySimpleSpark:21 - processing: 1
//2022-10-03 14:33:06 [Executor task launch worker for task 5id] WARN  com.bawi.spark.MySimpleSpark:21 - processing: 3
//2022-10-03 14:33:06 [Executor task launch worker for task 4id] WARN  com.bawi.spark.MySimpleSpark:21 - processing: 2
//2022-10-03 14:33:06 [Executor task launch worker for task 3id] WARN  com.bawi.spark.MySimpleSpark:21 - processing: 1
