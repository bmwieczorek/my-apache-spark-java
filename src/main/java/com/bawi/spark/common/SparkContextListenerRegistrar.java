package com.bawi.spark.common;

import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public interface SparkContextListenerRegistrar {
    default List<Consumer<SparkSession>> getOnStartListeners() {
        return new ArrayList<>();
    }

    default List<Consumer<SparkSession>> getOnSuccessListeners()  {
        return new ArrayList<>();
    }

    default List<Consumer<SparkSession>> getOnErrorListeners()  {
        return new ArrayList<>();
    }
}
