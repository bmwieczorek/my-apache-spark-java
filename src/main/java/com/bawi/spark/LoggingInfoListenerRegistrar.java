package com.bawi.spark;

import com.bawi.spark.common.SparkIngestionBase;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

public interface LoggingInfoListenerRegistrar extends SparkIngestionBase {
    Logger LOGGER = LoggerFactory.getLogger(LoggingInfoListenerRegistrar.class);

    @Override
    default List<Consumer<SparkSession>> getOnStartListeners() {
        return Collections.singletonList(context -> LOGGER.info("on start listener"));
    }

    @Override
    default List<Consumer<SparkSession>> getOnSuccessListeners() {
        return Collections.singletonList(context -> LOGGER.info("on success listener"));
    }

    @Override
    default List<Consumer<SparkSession>> getOnErrorListeners() {
        return Collections.singletonList(context -> LOGGER.info("on error listener"));
    }

}
