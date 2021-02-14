package com.bawi.spark.common;

import org.apache.spark.ExceptionFailure;
import org.apache.spark.Success$;
import org.apache.spark.TaskEndReason;
import org.apache.spark.TaskFailedReason;
import org.apache.spark.groupon.metrics.UserMetricsSystem;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.scheduler.TaskInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class CustomSparkMetricsListener extends SparkListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomSparkMetricsListener.class);
    private static final int KEY_MAX_LENGTH = 30;

    private long appStartTimeMillis = Instant.now().toEpochMilli();

    private final CustomMetricCounterConsumer metricsConsumer;
    private final Map<String, CustomMapAccumulator> customMapAccumulatorMap;

    public CustomSparkMetricsListener(CustomMetricCounterConsumer metricsConsumer, Map<String, CustomMapAccumulator> customMapAccumulatorMap) {
        this.metricsConsumer = metricsConsumer;
        this.customMapAccumulatorMap = customMapAccumulatorMap;
    }

    @Override
    public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
        Long appEndTimeMillis = Optional.ofNullable(applicationEnd).map(SparkListenerApplicationEnd::time).orElse(0L);
        long durationMillis = appEndTimeMillis - appStartTimeMillis;
        UserMetricsSystem.gauge("applicationElapsedTimeInSecs." + resolveHostName()).set(durationMillis);
        customMapAccumulatorMap.forEach((name, customAccumulator) -> {
            HashMap<String, Long> map = customAccumulator.value();
            map.forEach((metricsName, metricsValue) -> {
                metricsConsumer.onMetric(metricsName + "." + resolveHostName(), metricsValue);
            });
        });
        LOGGER.info("{} applicationElapsedTimeInSecs: {}", resolveHostName(), durationMillis);
    }

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
        if (taskEnd != null && taskEnd.reason() != null) {
            TaskEndReason reason = taskEnd.reason();
            if (reason instanceof Success$) {
                metricsConsumer.onMetric("TaskStatus_Success." + appendTags(taskEnd), 1);
            }
            if (reason instanceof TaskFailedReason) {
                TaskFailedReason taskFailedReason = (TaskFailedReason) reason;
                if (taskFailedReason instanceof ExceptionFailure) {
                    ExceptionFailure exceptionFailure = (ExceptionFailure) taskFailedReason;
                    Option<Throwable> exception = exceptionFailure.exception();

                    // cannot use java 8 lambdas since Function1 are specialized traits (additional methods added by scalac)
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
                            exception.get().getCause() == null ? exception.get().getMessage() : exception.get().getCause().getMessage();
                    metricsConsumer.onMetric("TaskStatus_ExceptionFailure." + appendTags(taskEnd), 1);
                    metricsConsumer.onMetric("TaskStatus_ExceptionFailureMsg." + substr(errorMessage), 1);

                } else {
                    metricsConsumer.onMetric("TaskStatus_TaskFailedReason." + appendTags(taskEnd), 1);
                    metricsConsumer.onMetric("TaskStatus_TaskFailedReasonMsg." + substr(taskFailedReason.toErrorString()), 1);
                }
            }
            metricsConsumer.onMetric("Task_output_bytesWritten." + appendTags(taskEnd), taskEnd.taskMetrics().outputMetrics().bytesWritten());
            metricsConsumer.onMetric("Task_output_recordsWritten." + appendTags(taskEnd), taskEnd.taskMetrics().outputMetrics().recordsWritten());

            metricsConsumer.onMetric("Task_input_recordsRead." + appendTags(taskEnd), taskEnd.taskMetrics().inputMetrics().recordsRead());
            metricsConsumer.onMetric("Task_input_bytesRead." + appendTags(taskEnd), taskEnd.taskMetrics().inputMetrics().bytesRead());
        }
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
