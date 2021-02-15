package com.bawi.spark.common;

import org.apache.spark.*;
import org.apache.spark.groupon.metrics.UserMetricsSystem;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.scheduler.TaskInfo;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.util.QueryExecutionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CustomSparkMetricsListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomSparkMetricsListener.class);
    private static final int KEY_MAX_LENGTH = 30;
    private Pattern EXCEPTION_WITHOUT_PACKAGE_PATTERN = Pattern.compile("[A-Za-z0-9]*Exception.*");


    private long appStartTimeMillis = Instant.now().toEpochMilli();

    private final CustomMetricCounterConsumer metricsConsumer;
    private final Map<String, CustomMapAccumulator> customMapAccumulatorMap;

    public CustomSparkMetricsListener(CustomMetricCounterConsumer metricsConsumer, Map<String, CustomMapAccumulator> customMapAccumulatorMap) {
        this.metricsConsumer = metricsConsumer;
        this.customMapAccumulatorMap = customMapAccumulatorMap;
    }

    public void registerSparkListener(SparkContext sparkContext) {
        sparkContext.addSparkListener(new SparkListener() {
            @Override
            public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
                Long appEndTimeMillis = Optional.ofNullable(applicationEnd).map(SparkListenerApplicationEnd::time).orElse(0L);
                long durationSeconds = (appEndTimeMillis - appStartTimeMillis) / 1000;
                UserMetricsSystem.gauge("applicationElapsedTimeInSecs." + resolveHostName()).set(durationSeconds);
                customMapAccumulatorMap.forEach((name, customAccumulator) -> {
                    HashMap<String, Long> map = customAccumulator.value();
                    map.forEach((metricsName, metricsValue) -> {
                        String metric = metricsName.contains(".") ? metricsName : metricsName + "." + resolveHostName();
                        metricsConsumer.onMetric(metric, metricsValue);
                    });
                });
                LOGGER.info("{} applicationElapsedTimeInSecs: {}", resolveHostName(), durationSeconds);
            }

            @Override
            public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
                if (taskEnd != null && taskEnd.reason() != null) {
                    TaskEndReason reason = taskEnd.reason();
                    if (reason instanceof Success$) {
                        metricsConsumer.onMetric("TaskSuccess." + appendTags(taskEnd), 1);
                    }
                    if (reason instanceof TaskFailedReason) {
                        TaskFailedReason taskFailedReason = (TaskFailedReason) reason;
                        if (taskFailedReason instanceof ExceptionFailure) {
                            ExceptionFailure exceptionFailure = (ExceptionFailure) taskFailedReason;
                            Option<Throwable> exception = exceptionFailure.exception();

                            // cannot use java 8 lambdas since Function1 are specialized traits (additional methods added by scalac());
/*                    String errorMessage = exception.map(new AbstractFunction1<Throwable, String>() {
                        @Override
                        public String apply(Throwable t) {
                            return t.getCause() == null ? t.getMessage() : t.getCause().getMessage();
                        }
                    }).getOrElse(new AbstractFunction0<String>() {

                        @Override
                        public String apply() {
                            return "UNKNOWN";
                        }
                    });
*/
                            String errorMessage = exception.isEmpty()
                                    ?
                                    "UNKNOWN"
                                    :
                                    exception.get().getCause() == null ? exception.get().getMessage() : exception.get().getCause().toString();
                            metricsConsumer.onMetric("TaskEx." + appendTags(taskEnd), 1);
                            metricsConsumer.onMetric("TaskExMsg." + substr(removeExceptionPackage(errorMessage)), 1);

                        } else {
                            metricsConsumer.onMetric("TaskFailed." + appendTags(taskEnd), 1);
                            metricsConsumer.onMetric("TaskFailedMsg." + substr(removeExceptionPackage(taskFailedReason.toErrorString())), 1);
                        }
                    }

                    metricsConsumer.onMetric("Task_executorDeserialTime." + appendTags(taskEnd), taskEnd.taskMetrics().executorDeserializeTime());
                    metricsConsumer.onMetric("Task_executorDeserialCpuTime." + appendTags(taskEnd), taskEnd.taskMetrics().executorDeserializeCpuTime());
                    metricsConsumer.onMetric("Task_executorRunTime." + appendTags(taskEnd), taskEnd.taskMetrics().executorRunTime());
                    metricsConsumer.onMetric("Task_executorCpuTime." + appendTags(taskEnd), taskEnd.taskMetrics().executorCpuTime());
                    metricsConsumer.onMetric("Task_resultSize." + appendTags(taskEnd), taskEnd.taskMetrics().resultSize());
                    metricsConsumer.onMetric("Task_jvmGCTime." + appendTags(taskEnd), taskEnd.taskMetrics().jvmGCTime());
                    metricsConsumer.onMetric("Task_resultSerializationTime." + appendTags(taskEnd), taskEnd.taskMetrics().resultSerializationTime());
                    metricsConsumer.onMetric("Task_memoryBytesSpilled." + appendTags(taskEnd), taskEnd.taskMetrics().memoryBytesSpilled());
                    metricsConsumer.onMetric("Task_diskBytesSpilled." + appendTags(taskEnd), taskEnd.taskMetrics().diskBytesSpilled());
                    metricsConsumer.onMetric("Task_peakExecutionMemory." + appendTags(taskEnd), taskEnd.taskMetrics().peakExecutionMemory());

                    metricsConsumer.onMetric("Task_shufW_bytesWritten." + appendTags(taskEnd), taskEnd.taskMetrics().shuffleWriteMetrics().bytesWritten());
                    metricsConsumer.onMetric("Task_shufW_recordsWritten." + appendTags(taskEnd), taskEnd.taskMetrics().shuffleWriteMetrics().recordsWritten());
                    metricsConsumer.onMetric("Task_shufW_writeTime." + appendTags(taskEnd), taskEnd.taskMetrics().shuffleWriteMetrics().writeTime());

                    metricsConsumer.onMetric("Task_shufR_remoteBlocksFetched." + appendTags(taskEnd), taskEnd.taskMetrics().shuffleReadMetrics().remoteBlocksFetched());
                    metricsConsumer.onMetric("Task_shufR_localBlocksFetched." + appendTags(taskEnd), taskEnd.taskMetrics().shuffleReadMetrics().localBlocksFetched());
                    metricsConsumer.onMetric("Task_shufR_remoteBytesRead." + appendTags(taskEnd), taskEnd.taskMetrics().shuffleReadMetrics().remoteBytesRead());
                    metricsConsumer.onMetric("Task_shufR_localBytesRead." + appendTags(taskEnd), taskEnd.taskMetrics().shuffleReadMetrics().localBytesRead());
                    metricsConsumer.onMetric("Task_shufR_fetchWaitTime." + appendTags(taskEnd), taskEnd.taskMetrics().shuffleReadMetrics().fetchWaitTime());
                    metricsConsumer.onMetric("Task_shufR_recordsRead." + appendTags(taskEnd), taskEnd.taskMetrics().shuffleReadMetrics().recordsRead());
                    metricsConsumer.onMetric("Task_shufR_totalBytesRead." + appendTags(taskEnd), taskEnd.taskMetrics().shuffleReadMetrics().totalBytesRead());
                    metricsConsumer.onMetric("Task_shufR_totalBlocksFetched." + appendTags(taskEnd), taskEnd.taskMetrics().shuffleReadMetrics().totalBlocksFetched());

                    metricsConsumer.onMetric("Task_output_bytesWritten." + appendTags(taskEnd), taskEnd.taskMetrics().outputMetrics().bytesWritten());
                    metricsConsumer.onMetric("Task_output_recordsWritten." + appendTags(taskEnd), taskEnd.taskMetrics().outputMetrics().recordsWritten());

                    metricsConsumer.onMetric("Task_input_recordsRead." + appendTags(taskEnd), taskEnd.taskMetrics().inputMetrics().recordsRead());
                    metricsConsumer.onMetric("Task_input_bytesRead." + appendTags(taskEnd), taskEnd.taskMetrics().inputMetrics().bytesRead());

                }
            }
        });
    }

    public void registerQueryExecutionListener(SparkSession sparkSession ) {
        sparkSession.listenerManager().register(new QueryExecutionListener() {

            @Override
            public void onFailure(String funcName, QueryExecution qe, Exception exception) {
                metricsConsumer.onMetric("QueryFail_" + funcName + "." + resolveHostName(), 1);
                metricsConsumer.onMetric("QueryFailMsg_" + funcName + "." + substr(removeExceptionPackage(exception.getCause().toString())), 1);
                LOGGER.info("QueryFailMsg_" + funcName + "." + resolveHostName() + "." + exception.getCause().toString());
            }

            @Override
            public void onSuccess(String funcName, QueryExecution qe, long durationInNanos) {
                metricsConsumer.onMetric("QuerySuccessDuration_" + funcName + "." + resolveHostName(), durationInNanos/1000000000L);
                LOGGER.info("QuerySuccess_Duration_" + funcName + "." + resolveHostName(), durationInNanos / 1000000000.0);
            }
        });
    }

    private String removeExceptionPackage(String exception) {
        Matcher matcher = EXCEPTION_WITHOUT_PACKAGE_PATTERN.matcher(exception);
        if (matcher.find()) {
            return matcher.group();
        }
        return exception;
    }

    private String resolveHostName() {
        try {
            return InetAddress.getLocalHost().getHostName().split("\\.")[0];
        } catch (UnknownHostException e) {
            return "UNKNOWN";
        }
    }

    private String appendTags(SparkListenerTaskEnd taskEnd) {
        TaskInfo info = taskEnd.taskInfo();
        return substr(info.host().split("\\.")[0]);
    }

    private String substr(String string) {
        if (string == null) {
            return "";
        } else {
            return string.substring(0, Math.min(string.length(), KEY_MAX_LENGTH)).replaceAll("\\.", "_");
        }
    }
}
